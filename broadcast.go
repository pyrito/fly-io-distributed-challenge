package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var messages []any
	var topology map[string]any

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Append the messages to a local list
		messages = append(messages, body["message"])

		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "broadcast_ok"

		// Reply with the new message
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"
		body["messages"] = messages

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = make(map[string]any)
		for k, v := range body["topology"].(map[string]any) {
			topology[k] = v
		}

		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "topology_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
