package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Service struct {
	node *maelstrom.Node
	kv   *maelstrom.KV

	kvLock *sync.RWMutex
}

func NewService(node *maelstrom.Node, kv *maelstrom.KV) *Service {
	service := &Service{
		node:   node,
		kv:     kv,
		kvLock: &sync.RWMutex{},
	}

	return service
}

func (s *Service) InitHandler(msg maelstrom.Message) error {
	// By this point the Node should have initialized some stuff
	ctx_write, cancel_write := context.WithTimeout(context.Background(), time.Second)
	defer cancel_write()

	if err := s.kv.Write(ctx_write, s.node.ID(), 0); err != nil {
		log.Println(err)
	}

	return nil
}

func (s *Service) AddHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.kvLock.Lock()
	delta := int(body["delta"].(float64))
	ctx_read, cancel_read := context.WithTimeout(context.Background(), time.Second)
	defer cancel_read()

	old_val, err := s.kv.ReadInt(ctx_read, s.node.ID())
	if err != nil {
		return err
	}

	// First read the latest value, then add it with the delta and write it out
	ctx_write, cancel_write := context.WithTimeout(context.Background(), time.Second)
	defer cancel_write()
	if err := s.kv.Write(ctx_write, s.node.ID(), old_val+delta); err != nil {
		return err
	}
	s.kvLock.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}

func (s *Service) ReadHandler(msg maelstrom.Message) error {
	ctx_read, cancel_read := context.WithTimeout(context.Background(), time.Second)
	defer cancel_read()

	value, err := s.kv.ReadInt(ctx_read, s.node.ID())
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": value,
	})
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	nodeService := NewService(n, kv)

	// I learned the hard way that the init handler needs to be set
	n.Handle("init", nodeService.InitHandler)
	n.Handle("add", nodeService.AddHandler)
	n.Handle("read", nodeService.ReadHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
