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
	node        *maelstrom.Node
	messages    map[int]struct{}
	msgBuffer   []int
	receiverIDs []string
	*sync.RWMutex
}

func NewService(node *maelstrom.Node) *Service {
	service := &Service{
		node:        node,
		messages:    make(map[int]struct{}),
		msgBuffer:   make([]int, 0),
		receiverIDs: make([]string, 0),
		RWMutex:     &sync.RWMutex{},
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

func (s *Service) queueMsg(msg int) {
	s.Lock()
	defer s.Unlock()
	s.msgBuffer = append(s.msgBuffer, msg)
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

	// Let's hijack the broadcast message and put in an extra
	// part of the message that has the batch
	if _, exists := body["messages"]; exists {
		message := body["messages"].([]int)
		// If we have "messages", we iterate over them and mark
		// that we've seen this msg and queue it up to broadcast
		// next.
		for _, msg := range message {
			// If we've already seen it, don't need to send it
			// again to whoever
			if !s.addMessage(msg) {
				continue
			}
			s.queueMsg(msg)
		}
	} else if _, exists := body["message"]; exists {
		message := int(body["message"].(float64))
		// Return if we've already seen this message
		if !s.addMessage(message) {
			return nil
		}
		s.queueMsg(message)
	}

	// Reply with the new message
	return s.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *Service) broadcast(body map[string]any) {
	for _, node := range s.receiverIDs {
		if s.node.ID() == node {
			continue
		}
		// Nasty golang bug, requires you to copy iterator var
		node_copy := node
		go s.sendBroadcastMessages(node_copy, body)
	}
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
	// Now the topology actually matters. What we can do is create
	// a topology where we broadcast all our messages to a single
	// node 0, which then relays the message to everyone else.
	// Maybe in the future we can do something smarter!
	// If we are node ID 0, we send to everyone, else we just
	// send to node ID 0 only. This way we reduce the traffic
	// by a fair amount!
	// We can now try to implement batching our message sends
	if s.node.ID() == "n0" {
		s.receiverIDs = append(s.receiverIDs, s.node.NodeIDs()...)
	} else {
		s.receiverIDs = append(s.receiverIDs, "n0")
	}

	return s.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func (s *Service) sendBatchMessages() error {
	var wg sync.WaitGroup
	for _, msg := range s.msgBuffer {
		// We're going to hijack the message with a new attribute
		// called messages (vs. message) which will contain a list
		// of messages.
		msg_send := map[string]any{
			"type":     "broadcast",
			"messages": msg,
		}
		go func() {
			defer wg.Done()
			s.broadcast(msg_send)
		}()
		wg.Add(1)
	}
	wg.Wait()

	return nil
}

func main() {
	n := maelstrom.NewNode()
	nodeService := NewService(n)

	go func() {
		// We are going to wait 250 milliseconds before we
		// send a batch of messages
		time.Sleep(time.Duration(250) * time.Millisecond)
		nodeService.sendBatchMessages()
	}()

	n.Handle("broadcast", nodeService.BroadcastHandler)
	n.Handle("read", nodeService.ReadHandler)
	n.Handle("topology", nodeService.TopologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
