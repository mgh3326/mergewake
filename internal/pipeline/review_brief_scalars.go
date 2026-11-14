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

// scalarHoldsPrivateKey reports whether a parsed scalar value is a private key:
// PEM text, or base64 that decodes to it. The value is the scalar after the YAML
// or JSON rules of quoting, folding and escaping, so the way the file wraps it
// does not matter. Only its first bytes are decoded: truncated keys are keys
// too, and no other base64 (a hash, a certificate) opens with the marker.
func scalarHoldsPrivateKey(value string) bool {
	if startsPrivateKey(value) {
		return true
	}
	run := strings.Join(strings.Fields(value), "")
	n := 0
	for n < len(run) && n < 256 && (run[n] >= 'A' && run[n] <= 'Z' || run[n] >= 'a' && run[n] <= 'z' || run[n] >= '0' && run[n] <= '9' || run[n] == '+' || run[n] == '/') {
		n++
	}
	if n < 24 {
		return false
	}
	dec, err := base64.StdEncoding.DecodeString(run[:n/4*4])
	return err == nil && startsPrivateKey(string(dec))
}

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
		if !scalarHoldsPrivateKey(s.Value) {
			continue
		}
		plain := s.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) == 0
		if plain && startsPrivateKey(s.Value) {
			// A PEM file that happens to parse as one plain scalar runs on into
			// whatever follows its END marker; markPEMBlock ends it there.
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
	col := s.Column // 1-based; the opening quote
	if col < 1 || col > len(row) {
		return l
	}
	i := col // first rune after the opening quote
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
