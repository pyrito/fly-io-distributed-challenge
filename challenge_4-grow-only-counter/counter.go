package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	count_key = "count_key"
)

type Service struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func NewService(node *maelstrom.Node, kv *maelstrom.KV) *Service {
	service := &Service{
		node: node,
		kv:   kv,
	}

	return service
}

func (s *Service) AddHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	ctx_read, cancel_read := context.WithTimeout(context.Background(), time.Second)
	defer cancel_read()

	old_val, err := s.kv.ReadInt(ctx_read, count_key)
	if err != nil {
		rpcError, ok := err.(*maelstrom.RPCError)
		// If the key doesn't exist, let's initialize it
		if ok && rpcError.Code == maelstrom.KeyDoesNotExist {
			old_val = 0
		} else {
			return err
		}
	}

	// First read the latest value, then add it with the delta and write it out
	ctx_write, cancel_write := context.WithTimeout(context.Background(), time.Second)
	defer cancel_write()

	if err := s.kv.CompareAndSwap(ctx_write, count_key, old_val, old_val+delta, true); err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}

func (s *Service) ReadHandler(msg maelstrom.Message) error {
	ctx_read, cancel_read := context.WithTimeout(context.Background(), time.Second)
	defer cancel_read()

	value, err := s.kv.ReadInt(ctx_read, count_key)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": value,
	})
}

// What does sequential consistency mean - it means that we should never view
// a state from a previous time. If W(x, 1) happens before W(x, 2), then if we
// read W(x, 2)
func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	nodeService := NewService(n, kv)

	n.Handle("add", nodeService.AddHandler)
	n.Handle("read", nodeService.ReadHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
