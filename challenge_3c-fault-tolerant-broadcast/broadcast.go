package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Service struct {
	node         *maelstrom.Node
	messages     map[int]struct{}
	recvAckChans map[string](chan any)
	*sync.RWMutex
}

func NewService(node *maelstrom.Node) *Service {
	service := &Service{
		node:         node,
		messages:     make(map[int]struct{}),
		recvAckChans: make(map[string](chan any)),
		RWMutex:      &sync.RWMutex{},
	}

	// Initialize the service channels
	for _, n := range node.NodeIDs() {
		if n == node.ID() {
			continue
		}
		service.recvAckChans[n] = make(chan any, 100)
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

func (s *Service) sendBroadcastMessages(dst_id string, message int) error {
	ch := s.recvAckChans[dst_id]
	body := map[string]any{
		"message": message,
		"type":    "sendAck",
	}
	for {
		if err := s.node.Send(dst_id, body); err != nil {
			panic(err)
		}
		select {
		case <-ch:
			// Seems like communication with this node is fine, so let's
			// just break here.
			log.Printf("RECEIVED ack")
			break
		case <-time.After(1 * time.Second):
			// If we haven't gotten anything in a second, then we retry
			log.Printf("retrying message to %s", dst_id)
			log.Println(body)
			continue
		}
	}
	return nil
}

func (s *Service) SendAckHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message := int(body["message"].(float64))

	// Add the message that we got from the broadcast, let's now
	// send back an ack
	if !s.addMessage(message) {
		return nil
	}

	return s.node.Reply(msg, map[string]any{
		"type": "ack",
	})
}

func (s *Service) RecvAckHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// message := int(body["message"].(float64))

	log.Println("we are receiving the ack")
	log.Println(body)
	ch := s.recvAckChans[msg.Src]
	ch <- 123

	return nil
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
		// Maintain a map of channels for each node ID
		// n1 -> [...], n2 -> [...], n3 -> [...]
		// When we broadcast a value to a specific node (n.Send), we will set
		// the message type to "sendAck", which should end up calling a handler
		// called: "sendAck". This will package up an "ack" message from the dest
		// node and the original node will handle this by adding it to a channel.

		// The original node will be listening to the respective channel, if nothing
		// is sent on the channel, we can assume that we need to retry the message
		// send. We assume packets don't drop
		go s.sendBroadcastMessages(node_copy, message)
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
	// We're just not going to do anything special for now
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
	n.Handle("sendAck", nodeService.SendAckHandler)
	n.Handle("ack", nodeService.RecvAckHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
