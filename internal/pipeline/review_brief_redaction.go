package pipeline

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/archive"
)

// redactRemovedDiffLinesOldSide masks the secret-like text of a delta's removed
// lines, and withholds the removed lines whose hunk the old side does not cover:
// a removed line is shown only when the scanned old side of its file matches its
// hunk. Its old-side line number may fall in a private-key block of the file at
// old, or it may itself be a base64 private key. A nil old has no old side at
// all, so every removed line is withheld. It returns the masked diff, the number
// of masked strings, and the paths, pattern names and deleted paths behind them.
// Withheld lines are not counted there; they are recorded in old.
func redactRemovedDiffLinesOldSide(diff string, old *oldSideLoader) (string, int, []string, []string, []string) {
	if old == nil {
		old = &oldSideLoader{}
	}
	lines := strings.SplitAfter(diff, "\n")
	paths := map[string]bool{}
	deletedPaths := map[string]bool{}
	patterns := map[string]bool{}
	count := 0
	withheld := 0
	path := ""
	pemBegin := strings.Join([]string{"-----", "BEGIN "}, "")
	pemEnd := strings.Join([]string{"-----", "END "}, "")
	privateKeyType := "PRIVATE KEY"
	inPrivateKey := false
	var out strings.Builder
	for i := 0; i < len(lines); {
		line := lines[i]
		plain := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(plain, "diff --git ") {
			path = ""
			inPrivateKey = false
		}
		if strings.HasPrefix(plain, "--- ") && !strings.HasPrefix(plain, "--- /dev/null") {
			path = strings.TrimSpace(strings.TrimPrefix(plain, "--- "))
			if before, _, ok := strings.Cut(path, "\t"); ok {
				path = before
			}
			if unquoted, err := strconv.Unquote(path); err == nil {
				path = unquoted
			}
			path = strings.TrimPrefix(path, "a/")
		}
		if strings.HasPrefix(plain, "+++ /dev/null") && path != "" {
			deletedPaths[path] = true
		}
		if !strings.HasPrefix(plain, "@@ ") {
			out.WriteString(line)
			i++
			continue
		}
		end := i + 1
		for end < len(lines) {
			next := strings.TrimRight(lines[end], "\r\n")
			if strings.HasPrefix(next, "@@ ") || strings.HasPrefix(next, "diff --git ") {
				break
			}
			end++
		}
		out.WriteString(line)
		hunk := lines[i+1 : end]
		oldLine := hunkOldStart(plain)
		var hunkOld *oldSide
		reason := ""
		if hunkHasRemoved(hunk) {
			o := old.lookup(path)
			switch {
			case o == nil:
				reason = old.unscannedReason(path)
			case oldLine < 1 || !o.aligned(oldLine, hunk):
				reason = oldSideMisaligned
			default:
				hunkOld = o
			}
		}
		withholding := hunkOld == nil
		// pending counts the withheld removed lines of the run being read; the
		// run is written as one marker line, when the next line is not removed.
		pending, hunkWithheld := 0, 0
		flush := func() {
			if pending > 0 {
				out.WriteString(withheldMarker(pending, reason))
				pending = 0
			}
		}
		for _, hunkLine := range hunk {
			prefix := byte(0)
			if len(hunkLine) > 0 {
				prefix = hunkLine[0]
			}
			if prefix != '-' && prefix != '+' && prefix != ' ' {
				flush()
				out.WriteString(hunkLine)
				continue
			}
			content := hunkLine[1:]
			oldSecret := false
			if prefix != '+' {
				oldSecret = prefix == '-' && (hunkOld.secretAt(oldLine) || base64PEMEnd(strings.TrimSpace(content)) > 0)
				oldLine++
			}
			startsPrivateKey := strings.Contains(content, pemBegin) && strings.Contains(content, privateKeyType)
			endsPrivateKey := strings.Contains(content, pemEnd) && strings.Contains(content, privateKeyType)
			if startsPrivateKey {
				inPrivateKey = true
			}
			switch {
			case prefix == '-' && withholding:
				pending++
				hunkWithheld++
			case prefix != '-':
				flush()
				out.WriteString(hunkLine)
			case inPrivateKey || oldSecret:
				flush()
				out.WriteString("-[redacted]")
				if strings.HasSuffix(hunkLine, "\r\n") {
					out.WriteString("\r\n")
				} else if strings.HasSuffix(hunkLine, "\n") {
					out.WriteByte('\n')
				}
				count++
				if path != "" {
					paths[path] = true
				}
				patterns["private_key"] = true
			default:
				flush()
				redacted, n, names := archive.RedactSecretLike(content)
				if n == 0 {
					out.WriteString(hunkLine)
				} else {
					out.WriteByte('-')
					out.WriteString(redacted)
					count += n
					if path != "" {
						paths[path] = true
					}
					for _, name := range names {
						patterns[name] = true
					}
				}
			}
			if endsPrivateKey {
				inPrivateKey = false
			}
		}
		flush()
		if hunkHasRemoved(hunk) {
			old.note(path, !withholding, reason, hunkWithheld)
		}
		withheld += hunkWithheld
		i = end
	}
	if count == 0 {
		if withheld == 0 {
			return diff, 0, nil, nil, nil
		}
		return out.String(), 0, nil, nil, nil
	}
	pathList := make([]string, 0, len(paths))
	for p := range paths {
		pathList = append(pathList, p)
	}
	sort.Strings(pathList)
	patternList := make([]string, 0, len(patterns))
	for name := range patterns {
		patternList = append(patternList, name)
	}
	sort.Strings(patternList)
	deletedPathList := make([]string, 0, len(deletedPaths))
	for p := range deletedPaths {
		if paths[p] {
			deletedPathList = append(deletedPathList, p)
		}
	}
	sort.Strings(deletedPathList)
	return out.String(), count, pathList, patternList, deletedPathList
}

var hunkOldStartRE = regexp.MustCompile(`^@@ -(\d+)(?:,\d+)? `)

// hunkOldStart is the old-side line a hunk header starts at, or 0.
func hunkOldStart(header string) int {
	m := hunkOldStartRE.FindStringSubmatch(header)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}

func hunkHasRemoved(hunk []string) bool {
	for _, l := range hunk {
		if strings.HasPrefix(l, "-") {
			return true
		}
	}
	return false
}
