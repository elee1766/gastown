package acpworker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  any             `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type rpcPeer struct {
	writer  io.Writer
	writeMu sync.Mutex
	mu      sync.Mutex
	next    uint64
	pending map[string]chan rpcMessage
	done    chan struct{}
	err     error
	update  func(json.RawMessage)
}

func newPeer(reader io.Reader, writer io.Writer, update func(json.RawMessage)) *rpcPeer {
	p := &rpcPeer{writer: writer, pending: make(map[string]chan rpcMessage), done: make(chan struct{}), update: update}
	go p.read(reader)
	return p
}
func (p *rpcPeer) write(m any) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return json.NewEncoder(p.writer).Encode(m)
}
func (p *rpcPeer) read(reader io.Reader) {
	var readErr error
	defer func() { p.mu.Lock(); p.err = readErr; close(p.done); p.mu.Unlock() }()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 65536), 16<<20)
	for scanner.Scan() {
		var m rpcMessage
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			readErr = fmt.Errorf("invalid ACP JSON: %w", err)
			return
		}
		if m.JSONRPC != "2.0" {
			readErr = fmt.Errorf("invalid ACP JSON-RPC version")
			return
		}
		if m.Method != "" {
			raw, _ := json.Marshal(m.Params)
			if len(m.ID) > 0 {
				// No implicit permission approval or client filesystem/terminal access.
				if m.Method == "session/request_permission" {
					_ = p.write(map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{"outcome": map[string]string{"outcome": "cancelled"}}})
				} else {
					_ = p.write(map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32601, "message": "unsupported client method"}})
				}
			} else if m.Method == "session/update" && p.update != nil {
				p.update(raw)
			}
			continue
		}
		p.mu.Lock()
		ch := p.pending[string(m.ID)]
		p.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
	}
	readErr = scanner.Err()
	if readErr == nil {
		readErr = io.EOF
	}
}
func (p *rpcPeer) call(ctx context.Context, method string, params, out any) error {
	return p.callWithSent(ctx, method, params, out, nil)
}

func (p *rpcPeer) callWithSent(ctx context.Context, method string, params, out any, sent func()) error {
	p.mu.Lock()
	p.next++
	id := fmt.Sprintf("%d", p.next)
	ch := make(chan rpcMessage, 1)
	p.pending[id] = ch
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	err := ctx.Err()
	if err == nil {
		err = p.write(rpcMessage{JSONRPC: "2.0", ID: json.RawMessage(id), Method: method, Params: params})
	}
	if sent != nil {
		sent()
	}
	if err != nil {
		return err
	}
	select {
	case m := <-ch:
		if len(m.Error) > 0 && string(m.Error) != "null" {
			return fmt.Errorf("ACP %s: %s", method, m.Error)
		}
		if out != nil {
			return json.Unmarshal(m.Result, out)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		p.mu.Lock()
		err := p.err
		p.mu.Unlock()
		return fmt.Errorf("ACP disconnected: %w", err)
	}
}
func (p *rpcPeer) notify(method string, params any) error {
	return p.write(rpcMessage{JSONRPC: "2.0", Method: method, Params: params})
}
