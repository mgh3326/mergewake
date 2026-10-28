// Package reviewer owns CLI-specific restrictions; profiles only select models.
package reviewer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/mgh3326/mergewake/internal/config"
	"github.com/mgh3326/mergewake/internal/integrations"
)

type Invocation struct {
	Command string
	Args    []string
	Kind    string
	Env     []string
	Model   string
}

func Prepare(p config.Reviewer, control string) (Invocation, error) {
	v := Invocation{Command: config.Expand(p.Command), Kind: filepath.Base(p.Command)}
	if !config.ReviewerCommand(p.Command) {
		return v, errors.New("unsupported reviewer CLI")
	}
	if err := config.ValidateReviewer(p); err != nil {
		return v, err
	}
	for i := 0; i < len(p.Args); i += 2 {
		k, value := p.Args[i], p.Args[i+1]
		if v.Kind == "codex" && k == "--effort" {
			v.Args = append(v.Args, "-c", "model_reasoning_effort="+value)
		} else {
			v.Args = append(v.Args, k, value)
		}
	}
	write := func(name, body string) (string, error) {
		path := filepath.Join(control, name)
		return path, os.WriteFile(path, []byte(body), 0600)
	}
	switch v.Kind {
	case "pi":
		v.Args = append(v.Args, "--print", "--no-session", "--no-tools", "--no-mcp", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files")
	case "cmd":
		v.Args = append(v.Args, "--print", "--no-session", "--safe-mode", "--no-skills", "--skip-onboarding", "--no-auto-update", "--max-turns", "1", "--permission-mode", "dont-ask", "--config", `permissions.deny=["*"]`)
	case "claude":
		v.Args = append(v.Args, "-p", "--output-format", "text", "--no-session-persistence", "--safe-mode", "--restricted", "--tools", "", "--disable-slash-commands", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--setting-sources", "", "--permission-mode", "dontAsk", "--permission-prompts", "none")
	case "codex":
		v.Args = append([]string{"exec"}, v.Args...)
		v.Args = append(v.Args, "--ignore-user-config", "--ignore-rules", "--skip-git-repo-check", "--ephemeral", "--sandbox", "read-only", "--color", "never")
		for _, c := range []string{`approval_policy="never"`, `mcp_servers={}`, `web_search="disabled"`, `project_doc_max_bytes=0`, `apps._default.enabled=false`, `agents.enabled=false`, `skills.include_instructions=false`, `features.shell_tool=false`, `features.code_mode=false`, `features.multi_agent=false`, `features.multi_agent_v2=false`, `features.plugins=false`, `features.hooks=false`, `features.codex_hooks=false`, `features.memory_tool=false`, `features.search_tool=false`, `features.skip_host_skill_discovery=true`} {
			v.Args = append(v.Args, "-c", c)
		}
		v.Args = append(v.Args, "-")
	case "devin":
		file, err := write("devin.json", `{"permissions":{"deny":["read","edit","write","grep","glob","exec","fetch","mcp__*","Read(/**)","Write(/**)"]},"subagents_enabled":false,"auto_update":false,"notify":"never","read_config_from":{"agents_standard":false,"cursor":false,"windsurf":false,"claude":false,"copilot":false,"opencode":false,"zed":false}}`)
		if err != nil {
			return v, err
		}
		v.Args = append(v.Args, "--print", "--prompt-file", "/dev/stdin", "--config", file, "--respect-workspace-trust", "false", "--permission-mode", "auto", "--verbosity", "0")
	case "grok":
		v.Args = append(v.Args, "--prompt-file", "/dev/stdin", "--output-format", "plain", "--tools", "", "--deny", "MCPTool(*)", "--disable-web-search", "--no-subagents", "--max-turns", "1", "--permission-mode", "dontAsk")
	case "kimi":
		home, err := prepareKimi(control, p.GateEffort)
		if err != nil {
			return v, err
		}
		for i := 0; i < len(p.Args); i += 2 {
			if p.Args[i] != "--model" {
				return v, errors.New("Kimi only accepts model selection; effort belongs in its configured model")
			}
			v.Model = p.Args[i+1]
		}
		v.Args = []string{"acp"}
		v.Env = []string{"KIMI_CODE_HOME=" + home}

	}
	return v, nil
}

func Run(ctx context.Context, runner integrations.Runner, v Invocation, input, dir, tokenEnv string) ([]byte, error) {
	args := []string{"-u", "GITHUB_TOKEN", "-u", "GH_TOKEN"}
	if tokenEnv != "" && tokenEnv != "GITHUB_TOKEN" && tokenEnv != "GH_TOKEN" {
		args = append(args, "-u", tokenEnv)
	}
	args = append(args, "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION=false", "GROK_MANAGED_MCPS_ENABLED=false", "GROK_MANAGED_MCP_GATEWAY_TOOLS_ENABLED=false", "GROK_CLAUDE_MCPS_ENABLED=false", "GROK_CURSOR_MCPS_ENABLED=false")
	args = append(args, v.Env...)
	args = append(args, v.Command)
	args = append(args, v.Args...)
	if v.Kind == "kimi" {
		duplex, ok := runner.(interface {
			RunDuplex(context.Context, string, []string, string, func(io.Reader, io.Writer) ([]byte, error)) ([]byte, error)
		})
		if !ok {
			return nil, errors.New("Kimi adapter needs a duplex runner")
		}
		return duplex.RunDuplex(ctx, "env", args, dir, func(r io.Reader, w io.Writer) ([]byte, error) { return kimiProtocol(r, w, dir, v.Model, input) })
	}
	if isolated, ok := runner.(interface {
		RunReview(context.Context, string, []string, string, string) ([]byte, error)
	}); ok {
		return isolated.RunReview(ctx, "env", args, input, dir)
	}
	return runner.Run(ctx, "env", args, input, dir)
}
