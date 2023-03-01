package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Metadata struct {
	messages map[int]struct{}
	*sync.RWMutex
}

func NewMetadata() *Metadata {
	return &Metadata{
		messages: make(map[int]struct{}),
		RWMutex:  &sync.RWMutex{},
	}
}

func (m *Metadata) AddMessage(msg int) bool {
	m.Lock()
	defer m.Unlock()
	if _, exists := m.messages[msg]; !exists {
		m.messages[msg] = struct{}{}
		return true
	}
	return false
}

func (m *Metadata) GetMessages() []int {
	m.RLock()
	defer m.RUnlock()
	msgs := make([]int, 0, len(m.messages))
	for k := range m.messages {
		msgs = append(msgs, k)
	}
	return msgs
}

func main() {
	n := maelstrom.NewNode()
	nodeMetadata := NewMetadata()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := int(body["message"].(float64))

		// Returns false if we already have seen this message
		if !nodeMetadata.AddMessage(message) {
			return nil
		}

		// Broadcast the message to everyone else
		for _, node := range n.NodeIDs() {
			if n.ID() == node || msg.Src == node {
				continue
			}
			// Nasty golang bug, requires you to copy iterator var
			node_copy := node
			go func() {
				if err := n.Send(node_copy, body); err != nil {
					panic(err)
				}
			}()
		}

		// Reply with the new message
		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Just return the messages list
		msgs := nodeMetadata.GetMessages()
		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": msgs,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// We're just not going to do anything special for now
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
