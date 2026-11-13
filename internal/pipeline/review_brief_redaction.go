package pipeline

import (
	"sort"
	"strconv"
	"strings"

	"github.com/mgh3326/mergewake/internal/archive"
)

// redactRemovedDiffLines changes only secret-pattern matches in removed lines.
// Its input is the review delta; callers retain that original for every
// non-brief use.
func redactRemovedDiffLines(diff string) (string, int, []string, []string, []string) {
	lines := strings.SplitAfter(diff, "\n")
	paths := map[string]bool{}
	deletedPaths := map[string]bool{}
	patterns := map[string]bool{}
	count := 0
	path := ""
	pemBegin := strings.Join([]string{"-----", "BEGIN"}, "")
	pemEnd := strings.Join([]string{"-----", "END"}, "")
	privateKeySuffix := " PRIVATE KEY" + strings.Join([]string{"-----"}, "")
	var out strings.Builder
	for i := 0; i < len(lines); {
		line := lines[i]
		plain := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(plain, "diff --git ") {
			path = ""
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
		inPrivateKey := false
		for _, hunkLine := range lines[i+1 : end] {
			if len(hunkLine) == 0 {
				out.WriteString(hunkLine)
				continue
			}
			prefix := hunkLine[0]
			if prefix != '-' && prefix != '+' && prefix != ' ' {
				out.WriteString(hunkLine)
				continue
			}
			content := hunkLine[1:]
			startsPrivateKey := strings.Contains(content, pemBegin) && strings.Contains(content, privateKeySuffix)
			endsPrivateKey := strings.Contains(content, pemEnd) && strings.Contains(content, privateKeySuffix)
			if startsPrivateKey {
				inPrivateKey = true
			}
			if prefix != '-' {
				out.WriteString(hunkLine)
			} else if inPrivateKey {
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
			} else {
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
		i = end
	}
	if count == 0 {
		return diff, 0, nil, nil, nil
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
