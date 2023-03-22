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
	node     *maelstrom.Node
	messages map[int]struct{}
	*sync.RWMutex
}

func NewService(node *maelstrom.Node) *Service {
	service := &Service{
		node:     node,
		messages: make(map[int]struct{}),
		RWMutex:  &sync.RWMutex{},
	}

	return service
}

func (s *Service) addMessage(msg int) bool {
	s.Lock()
	defer s.Unlock()
	if _, exists := s.messages[msg]; !exists {
		s.messages[msg] = struct{}{}
		return true
	}
	return false
}

func (s *Service) getMessages() []int {
	s.RLock()
	defer s.RUnlock()
	msgs := make([]int, 0, len(s.messages))
	for k := range s.messages {
		msgs = append(msgs, k)
	}
	return msgs
}

func (s *Service) sendRPC(dst string, body map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.node.SyncRPC(ctx, dst, body)
	return err
}

func (s *Service) sendBroadcastMessages(dst_id string, msg_body map[string]any) error {
	// Let's try sending the message 1000 times and wait for a timeout!
	err := error(nil)
	for i := 0; i < 1000; i++ {
		if err := s.sendRPC(dst_id, msg_body); err != nil {
			time.Sleep(time.Duration(3) * time.Second)
			continue
		}
		// Message succeeds
		return nil
	}
	return err
}

func (s *Service) BroadcastHandler(msg maelstrom.Message) error {
	// Unmarshal the message body as a loosely-typed map
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message := int(body["message"].(float64))

	// Returns false if we already have seen this message
	if !s.addMessage(message) {
		return nil
	}

	// Broadcast the message to everyone else
	for _, node := range s.node.NodeIDs() {
		if s.node.ID() == node || msg.Src == node {
			continue
		}
		// Nasty golang bug, requires you to copy iterator var
		node_copy := node
		go s.sendBroadcastMessages(node_copy, body)
	}

	// Reply with the new message
	return s.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *Service) ReadHandler(msg maelstrom.Message) error {
	// Just return the messages list
	msgs := s.getMessages()
	return s.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": msgs,
	})
}

func (s *Service) TopologyHandler(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func main() {
	n := maelstrom.NewNode()
	nodeService := NewService(n)

	n.Handle("broadcast", nodeService.BroadcastHandler)
	n.Handle("read", nodeService.ReadHandler)
	n.Handle("topology", nodeService.TopologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
