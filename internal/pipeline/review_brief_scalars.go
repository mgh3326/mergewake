package pipeline

import (
	"bytes"
	"encoding/base64"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// privateKeyBegin matches the opening marker of a private key whose label ends
// in PRIVATE KEY (RSA, EC, OPENSSH, ENCRYPTED and plain) or is the PGP private
// key block, with the label as group 1. A certificate or a public key is not
// matched, and neither is a line that merely mentions the words.
var privateKeyBegin = regexp.MustCompile(`^-----BEGIN ((?:[A-Z0-9]+ )*PRIVATE KEY(?: BLOCK)?)-----`)

// startsPrivateKey reports whether text, after leading white space, opens with a
// private-key marker.
func startsPrivateKey(text string) bool {
	return privateKeyBegin.MatchString(strings.TrimLeft(text, " \t\r\n"))
}

// privateKeyLine finds a private-key marker that opens a line of a value.
var privateKeyLine = regexp.MustCompile(`(?m)^[ \t]*-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY(?: BLOCK)?-----`)

// scalarBase64HoldsPrivateKey reports whether a scalar value is base64 that
// decodes to text holding a private key, wherever in that text (a bundle may
// carry a certificate first). The value is the scalar after the YAML or JSON
// rules of quoting, folding and escaping, so the way the file wraps it does not
// matter. A certificate, a public key or a hash decodes to no such marker.
func scalarBase64HoldsPrivateKey(value string) bool {
	run := strings.Join(strings.Fields(value), "")
	n := 0
	for n < len(run) && n < scalarDecodeCap && (run[n] >= 'A' && run[n] <= 'Z' || run[n] >= 'a' && run[n] <= 'z' || run[n] >= '0' && run[n] <= '9' || run[n] == '+' || run[n] == '/') {
		n++
	}
	if n < 24 {
		return false
	}
	dec, err := base64.StdEncoding.DecodeString(run[:n/4*4])
	return err == nil && privateKeyLine.Match(dec)
}

// scalarDecodeCap bounds the base64 characters decoded for one scalar.
const scalarDecodeCap = 256 << 10

// markScalarPrivateKeys marks the source lines of every YAML or JSON scalar in
// body that holds a private key. The scalar's own lines are found from the
// parse, not from what they look like, so a wrapped value ends where the scalar
// ends: neither earlier (a deeper indent, a quote, a comment) nor later (a hash
// on the next line). It reports whether the whole file parsed; a file that does
// not parse marks only the scalars before the error.
func markScalarPrivateKeys(body []byte, lines []string, secret []bool) (parsed bool) {
	var scalars []*yaml.Node
	nodeLines := map[int]bool{}
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		nodeLines[n.Line] = true
		if n.Kind == yaml.ScalarNode {
			scalars = append(scalars, n)
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	dec := yaml.NewDecoder(bytes.NewReader(body))
	for {
		var doc yaml.Node
		if err := dec.Decode(&doc); err != nil {
			parsed = err == io.EOF
			break
		}
		walk(&doc)
	}
	for _, s := range scalars {
		if !scalarHoldsKey(s, lines) {
			continue
		}
		first, last := scalarLines(s, lines, nodeLines)
		for l := first; l <= last && l <= len(secret); l++ {
			if l >= 1 {
				secret[l-1] = true
			}
		}
	}
	return parsed
}

// scalarLines is the 1-based line range of a scalar's source text.
func scalarLines(s *yaml.Node, lines []string, nodeLines map[int]bool) (first, last int) {
	first = s.Line
	if s.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
		first++ // the line with the | or > indicator holds no value
	}
	switch {
	case s.Style&yaml.DoubleQuotedStyle != 0:
		return first, quotedEnd(s, lines, '"')
	case s.Style&yaml.SingleQuotedStyle != 0:
		return first, quotedEnd(s, lines, '\'')
	}
	// Block, plain: up to the line before the next node of the file.
	last = len(lines)
	for l := s.Line + 1; l <= len(lines); l++ {
		if nodeLines[l] {
			last = l - 1
			break
		}
	}
	// Blank lines, comments and document markers after the value are not it.
	for last > first {
		t := strings.TrimSpace(lines[last-1])
		if t != "" && !strings.HasPrefix(t, "#") && t != "---" && t != "..." {
			break
		}
		last--
	}
	return first, last
}

// quotedEnd is the line of the closing quote of the quoted scalar s.
func quotedEnd(s *yaml.Node, lines []string, quote rune) int {
	l := s.Line
	if l < 1 || l > len(lines) {
		return s.Line
	}
	row := []rune(lines[l-1])
	// Column is the scalar's first character, which is its anchor or tag when it
	// has one: skip those properties to reach the opening quote.
	i := s.Column - 1
	for i >= 0 && i < len(row) && (row[i] == '&' || row[i] == '!') {
		for i < len(row) && row[i] != ' ' && row[i] != '\t' {
			i++
		}
		for i < len(row) && (row[i] == ' ' || row[i] == '\t') {
			i++
		}
	}
	if i < 0 || i >= len(row) || row[i] != quote {
		return l
	}
	i++ // first rune after the opening quote
	for {
		for ; i < len(row); i++ {
			switch {
			case quote == '"' && row[i] == '\\':
				i++ // an escaped character, or an escaped line break at row end
			case row[i] == quote && quote == '\'' && i+1 < len(row) && row[i+1] == '\'':
				i++ // '' is a quote inside a single-quoted scalar
			case row[i] == quote:
				return l
			}
		}
		if l >= len(lines) {
			return l
		}
		l++
		row, i = []rune(lines[l-1]), 0
	}
}

// scalarHoldsKey reports whether the source lines of scalar s are to be marked.
// Block scalars and plain scalars that carry PEM text are left to the line
// scan (markPEMBlock), which ends a block at its own END marker and keeps the
// text around it visible; the scalar rule takes what that scan cannot see.
func scalarHoldsKey(s *yaml.Node, lines []string) bool {
	if scalarBase64HoldsPrivateKey(s.Value) {
		return true
	}
	switch {
	case s.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle) != 0:
		return privateKeyLine.MatchString(s.Value)
	case s.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0:
		return false
	}
	// A plain scalar that opens with the marker, after `key: `, not at a line
	// start: markPEMBlock cannot see that marker.
	return startsPrivateKey(s.Value) && !privateKeyBegin.MatchString(strings.TrimSpace(lines[min(max(s.Line, 1), len(lines))-1]))
}
