package classify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mrodavis/dispute-triage-stream/internal/dispute"
)

const toolName = "record_classification"

// Anthropic calls the Messages API directly over HTTP with a forced tool call, so the structured
// output contract is explicit in this file rather than hidden in an SDK.
type Anthropic struct {
	APIKey  string
	Model_  string
	BaseURL string
	HTTP    *http.Client
}

func NewAnthropic(apiKey, model string) *Anthropic {
	if model == "" {
		model = "claude-sonnet-5-5"
	}
	return &Anthropic{APIKey: apiKey, Model_: model, BaseURL: "https://api.anthropic.com", HTTP: &http.Client{Timeout: 30 * time.Second}}
}

func (a *Anthropic) Model() string      { return a.Model_ }
func (a *Anthropic) PromptHash() string { return PromptHash() }

type message struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type contentBlock struct {
	Type  string          `json:"type"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	Text  string          `json:"text,omitempty"`
}

type messagesResponse struct {
	Content []contentBlock `json:"content"`
}

// Classify forces the tool call, validates the result, and gives the model one corrective retry.
func (a *Anthropic) Classify(ctx context.Context, e dispute.Event) (dispute.Classification, error) {
	messages := []message{{Role: "user", Content: UserContent(e)}}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		res, err := a.call(ctx, messages)
		if err != nil {
			return dispute.Classification{}, err
		}
		var tool *contentBlock
		for i := range res.Content {
			if res.Content[i].Type == "tool_use" && res.Content[i].Name == toolName {
				tool = &res.Content[i]
			}
		}
		if tool == nil {
			return dispute.Classification{}, fmt.Errorf("model did not call %s", toolName)
		}
		c, err := Parse(tool.Input)
		if err == nil {
			return c, nil
		}
		lastErr = err
		messages = append(messages,
			message{Role: "assistant", Content: res.Content},
			message{Role: "user", Content: []map[string]any{{
				"type": "tool_result", "tool_use_id": tool.ID, "is_error": true,
				"content": fmt.Sprintf("Invalid classification: %v. Call %s again with corrected values.", err, toolName),
			}}},
		)
	}
	return dispute.Classification{}, fmt.Errorf("invalid classification twice: %w", lastErr)
}

func (a *Anthropic) call(ctx context.Context, messages []message) (*messagesResponse, error) {
	body, _ := json.Marshal(map[string]any{
		"model":       a.Model_,
		"max_tokens":  512,
		"system":      SystemPrompt,
		"tools":       []map[string]any{{"name": toolName, "description": "Record the dispute classification.", "input_schema": Schema}},
		"tool_choice": map[string]any{"type": "tool", "name": toolName},
		"messages":    messages,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", a.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := a.HTTP.Do(req)
	if err != nil {
		return nil, TransientError{err}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		var out messagesResponse
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		return &out, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		// 429, 500, 529 overloaded: retry later with backoff.
		return nil, TransientError{fmt.Errorf("anthropic status %d", resp.StatusCode)}
	default:
		return nil, fmt.Errorf("anthropic status %d: %s", resp.StatusCode, truncate(string(data), 200))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
