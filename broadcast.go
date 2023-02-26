package main

import (
	"encoding/json"
	"log"
	"sync"

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
		service.recvAckChans[n] = make(chan any)
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
		go func() {
			// Maintain a map of channels for each node ID
			// n1 -> [...], n2 -> [...], n3 -> [...]
			// When we broadcast a value to a specific node (n.Send), we will set
			// the message type to "sendAck", which should end up calling a handler
			// called: "sendAck". This will package up an "ack" message from the dest
			// node and the original node will handle this by adding it to a channel.

			// The original node will be listening to the respective channel, if nothing
			// is sent on the channel, we can assume that we need to retry the message
			// send. We assume packets don't drop
			if err := s.node.Send(node_copy, body); err != nil {
				panic(err)
			}
		}()
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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
