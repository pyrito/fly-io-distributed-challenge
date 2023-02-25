package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func contains(s []any, e any) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

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
		// Check that the message doesn't exist in what we've seen
		// already!
		message := body["message"]
		if !contains(messages, message) {
			messages = append(messages, body["message"])
		}

		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "broadcast_ok"

		// We are going to send the message to everyone we can talk to
		dest_nodes := topology[n.ID()]
		for _, node := range dest_nodes.([]interface{}) {
			if err := n.Send(node.(string), body); err != nil {
				log.Fatal(err)
			}
		}
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
