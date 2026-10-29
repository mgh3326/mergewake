package reviewer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
)

// agy's print mode takes its prompt only as an argv value, so the diff goes in
// as one stream-json user message on stdin instead. Headless agy denies tool
// permissions it would otherwise prompt for; the session-only settings deny
// the shell, file, URL and MCP tools outright, and any tool step still fails
// the pass.
const agySettings = `{"permissions":{"deny":["command(*)","read_file(*)","write_file(*)","read_url(*)","mcp(*)"]},"allowNonWorkspaceAccess":false}`

var agyArgs = []string{"--print=", "--input-format", "stream-json", "--output-format", "stream-json", "--disable-slash-commands", "--sandbox", "--settings", agySettings}

func agyInput(prompt string) (string, error) {
	b, err := json.Marshal(map[string]any{"event": "user", "message": map[string]string{"content": prompt}})
	return string(b) + "\n", err
}

func agyOutput(b []byte) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	var response *string
	for scanner.Scan() {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var e struct {
			Event string `json:"event"`
			Step  struct {
				Type string `json:"step_type"`
			} `json:"step_update"`
			Result struct {
				Status   string `json:"status"`
				Response string `json:"response"`
				Turns    int    `json:"num_turns"`
			} `json:"result"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return nil, errors.New("invalid agy stream JSON")
		}
		switch e.Event {
		case "step_update":
			if e.Step.Type != "user_input" && e.Step.Type != "agent_response" {
				return nil, errors.New("agy attempted a tool call")
			}
		case "result":
			if e.Result.Status != "SUCCESS" || e.Result.Turns != 1 {
				return nil, errors.New("agy review did not complete one turn")
			}
			response = &e.Result.Response
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if response == nil || len(bytes.TrimSpace([]byte(*response))) == 0 {
		return nil, errors.New("empty agy review")
	}
	return []byte(*response), nil
}
