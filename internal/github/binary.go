package github

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/mgh3326/mergewake/internal/manifest"
)

func binaryName() string { return "mergewake-" + runtime.GOOS + "-" + runtime.GOARCH }
func (c *Client) BinaryHead(ctx context.Context, repo string) (string, error) {
	releases, err := list[Release](ctx, c, base(repo)+"/releases")
	if err != nil {
		return "", err
	}
	for _, r := range releases {
		if r.Draft || r.Prerelease || !strings.HasPrefix(r.Tag, "v") {
			continue
		}
		binary, sums := false, false
		for _, a := range r.Assets {
			binary = binary || a.Name == binaryName()
			sums = sums || a.Name == "SHA256SUMS"
		}
		if binary && sums {
			commit, err := c.Commit(ctx, repo, r.Tag)
			return commit.SHA, err
		}
	}
	return "", errors.New("no compatible binary release")
}

// BinaryAtCommit uses exactly the same binaries and SHA256SUMS as install.sh.
// A release tag must resolve to the requested full commit, including rollback.
func (c *Client) BinaryAtCommit(ctx context.Context, repo, sha string) ([]byte, error) {
	if !manifest.SHA.MatchString(sha) {
		return nil, errors.New("binary install requires a full commit SHA")
	}
	releases, err := list[Release](ctx, c, base(repo)+"/releases")
	if err != nil {
		return nil, err
	}
	for _, r := range releases {
		if r.Draft || r.Prerelease || !strings.HasPrefix(r.Tag, "v") {
			continue
		}
		commit, err := c.Commit(ctx, repo, r.Tag)
		if err != nil {
			return nil, err
		}
		if commit.SHA != sha {
			continue
		}
		var binary, checksums Asset
		for _, a := range r.Assets {
			if a.Name == binaryName() {
				binary = a
			}
			if a.Name == "SHA256SUMS" {
				checksums = a
			}
		}
		if binary.ID == 0 || checksums.ID == 0 || binary.Size < 1 || binary.Size > manifest.MaxFileSize {
			return nil, errors.New("missing or oversized binary release assets")
		}
		sums, err := c.asset(ctx, repo, checksums)
		if err != nil {
			return nil, err
		}
		expected := ""
		for _, line := range strings.Split(string(sums), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == binary.Name {
				if expected != "" {
					return nil, errors.New("duplicate checksum")
				}
				expected = fields[0]
			}
		}
		if len(expected) != 64 {
			return nil, errors.New("binary checksum missing")
		}
		data, err := c.request(ctx, "GET", fmt.Sprintf("%s/releases/assets/%d", base(repo), binary.ID), "application/octet-stream", nil)
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != binary.Size || manifest.Sum(data) != expected {
			return nil, errors.New("binary checksum or size mismatch")
		}
		check, err := c.Commit(ctx, repo, r.Tag)
		if err != nil {
			return nil, err
		}
		if check.SHA != sha {
			return nil, errors.New("binary release tag moved")
		}
		return data, nil
	}
	return nil, errors.New("no binary release at requested commit")
}
