package reviewer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// ACP is used because Kimi's -p accepts only an argv prompt. The isolated home
// keeps provider/model authentication but excludes MCP, hooks, plugins, skills,
// history and user instructions. Never copy this home into an audit or report.
func prepareKimi(control, effort string) (string, error) {
	source := os.Getenv("KIMI_CODE_HOME")
	if source == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		source = filepath.Join(home, ".kimi-code")
	}
	home := filepath.Join(control, "kimi")
	if err := os.Mkdir(home, 0700); err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(source, "config.toml"))
	if err != nil {
		return "", errors.New("Kimi model configuration unavailable")
	}
	var original map[string]any
	if err = toml.Unmarshal(data, &original); err != nil {
		return "", errors.New("invalid Kimi model configuration")
	}
	safe := map[string]any{}
	for _, key := range []string{"providers", "models", "default_model", "thinking"} {
		if value, ok := original[key]; ok {
			safe[key] = value
		}
	}
	// A non-empty allowlist that matches no registered tool disables every tool.
	// [] would mean unrestricted in global config (unlike an agent-file tools: []).
	if effort != "" {
		safe["thinking"] = map[string]any{"effort": effort}
	}
	safe["tools"] = map[string]any{"enabled": []string{"*"}}
	data, err = toml.Marshal(safe)
	if err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(home, "config.toml"), data, 0600); err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(home, "SYSTEM.md"), []byte("Review only the user-supplied diff. Return strict findings JSON. Do not use tools, context files, or delegation."), 0600); err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(home, "mcp.json"), []byte(`{"mcpServers":{}}`), 0600); err != nil {
		return "", err
	}
	// OAuth credentials are private transient copies, permitting a refresh without
	// changing the operator's config. API-key providers use the selected config.
	entries, err := os.ReadDir(filepath.Join(source, "credentials"))
	if err != nil && !os.IsNotExist(err) {
		return "", errors.New("Kimi credentials unavailable")
	}
	if len(entries) > 0 {
		target := filepath.Join(home, "credentials")
		if err = os.Mkdir(target, 0700); err != nil {
			return "", err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			data, e := os.ReadFile(filepath.Join(source, "credentials", entry.Name()))
			if e != nil {
				return "", errors.New("Kimi credentials unavailable")
			}
			if e = os.WriteFile(filepath.Join(target, entry.Name()), data, 0600); e != nil {
				return "", e
			}
		}
	}
	return home, nil
}

func kimiProtocol(r io.Reader, w io.Writer, dir, model, input string) ([]byte, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	encoder := json.NewEncoder(w)
	send := func(id int, method string, params any) error {
		return encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	}
	var text strings.Builder
	receive := func(id int) (json.RawMessage, error) {
		for scanner.Scan() {
			var m struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
				Result json.RawMessage `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
				return nil, errors.New("invalid Kimi ACP JSON")
			}
			if m.Method != "" {
				// No client filesystem, terminal, permission or MCP capability is granted.
				if len(m.ID) > 0 {
					_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32601, "message": "Diff-only review: client tools disabled"}})
					return nil, errors.New("Kimi requested a client tool")
				}
				if m.Method == "session/update" {
					var p struct {
						Update struct {
							Kind    string `json:"sessionUpdate"`
							Content struct {
								Type string `json:"type"`
								Text string `json:"text"`
							} `json:"content"`
						} `json:"update"`
					}
					if err := json.Unmarshal(m.Params, &p); err != nil {
						return nil, err
					}
					switch p.Update.Kind {
					case "agent_message_chunk":
						if p.Update.Content.Type != "text" {
							return nil, errors.New("non-text Kimi message")
						}
						if text.Len()+len(p.Update.Content.Text) > 4<<20 {
							return nil, errors.New("Kimi output exceeds limit")
						}
						text.WriteString(p.Update.Content.Text)
					case "tool_call", "tool_call_update":
						return nil, errors.New("Kimi attempted a tool call")
					}
				}
				continue
			}
			if string(m.ID) != fmt.Sprint(id) {
				return nil, errors.New("unexpected Kimi response ID")
			}
			if len(m.Error) > 0 {
				return nil, errors.New("Kimi ACP request failed")
			}
			return m.Result, nil
		}
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		return nil, io.ErrUnexpectedEOF
	}
	if err := send(1, "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{"fs": map[string]bool{"readTextFile": false, "writeTextFile": false}, "terminal": false}, "clientInfo": map[string]string{"name": "mergewake", "version": "0.3"}}); err != nil {
		return nil, err
	}
	if _, err := receive(1); err != nil {
		return nil, err
	}
	if err := send(2, "session/new", map[string]any{"cwd": dir, "mcpServers": []any{}}); err != nil {
		return nil, err
	}
	b, err := receive(2)
	if err != nil {
		return nil, err
	}
	var session struct {
		ID string `json:"sessionId"`
	}
	if err = json.Unmarshal(b, &session); err != nil || session.ID == "" {
		return nil, errors.New("Kimi did not create a session")
	}
	if model != "" {
		if err = send(3, "session/set_model", map[string]any{"sessionId": session.ID, "modelId": model}); err != nil {
			return nil, err
		}
		if _, err = receive(3); err != nil {
			return nil, err
		}
	}
	if err = send(4, "session/prompt", map[string]any{"sessionId": session.ID, "prompt": []any{map[string]string{"type": "text", "text": input}}}); err != nil {
		return nil, err
	}
	b, err = receive(4)
	if err != nil {
		return nil, err
	}
	var result struct {
		Stop string `json:"stopReason"`
	}
	if err = json.Unmarshal(b, &result); err != nil || result.Stop != "end_turn" {
		return nil, errors.New("Kimi review did not complete one turn")
	}
	if text.Len() == 0 {
		return nil, errors.New("empty Kimi review")
	}
	return []byte(text.String()), nil
}
