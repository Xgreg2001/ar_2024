// client/main.go
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

// command is the same as defined on the server.
type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func executeCommand(cmd command) {
	data, err := json.Marshal(cmd)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	resp, err := http.Post("http://localhost:8080/command", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if !strings.HasSuffix(string(body), "\n") {
		fmt.Print(string(body) + "\n")
	} else {
		fmt.Print(string(body))
	}
}

func main() {
	fmt.Println("Welcome to the Key-Value Store Client")
	fmt.Println("Available commands:")
	fmt.Println("  get <key>")
	fmt.Println("  set <key> <value>")
	fmt.Println("  leader")
	fmt.Println("  stop <node_id>")
	fmt.Println("  start <node_id>") // Add this line
	fmt.Println("  quit or exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		input = strings.TrimSpace(input)

		if input == "quit" || input == "exit" {
			break
		}

		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		var cmd command
		cmd.Op = strings.ToLower(parts[0])

		switch cmd.Op {
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			cmd.Key = parts[1]
		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			cmd.Key = parts[1]
			cmd.Value = strings.Join(parts[2:], " ")
		case "leader":
			checkLeader()
			continue
		case "stop":
			if len(parts) != 2 {
				fmt.Println("Usage: stop <node_id>")
				continue
			}
			stopNode(parts[1])
			continue
		case "start":
			if len(parts) != 2 {
				fmt.Println("Usage: start <node_id>")
				continue
			}
			startNode(parts[1])
			continue
		default:
			fmt.Println("Unknown command. Use 'get', 'set', 'leader', 'stop', 'start', or 'quit'/'exit'")
			continue
		}

		executeCommand(cmd)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

// Add this new function
func checkLeader() {
	resp, err := http.Get("http://localhost:8080/leader")
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Current leader: %s (State: %s)\n", result["leader"], result["state"])
}

// Add this new function
func stopNode(nodeID string) {
	data := map[string]string{
		"node_id": nodeID,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	resp, err := http.Post("http://localhost:8080/stop", "application/json", bytes.NewReader(jsonData))
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v\n", err)
		return
	}

	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		// If not JSON, print the raw message
		fmt.Printf("Server response: %s\n", string(body))
		return
	}

	if result["error"] != "" {
		fmt.Printf("Error: %s\n", result["error"])
	} else {
		fmt.Printf("Node %s stopped successfully\n", nodeID)
	}
}

// Add this new function at the end of the file
func startNode(nodeID string) {
	data := map[string]string{
		"node_id": nodeID,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	resp, err := http.Post("http://localhost:8080/start", "application/json", bytes.NewReader(jsonData))
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response: %v\n", err)
		return
	}

	// Try to decode as JSON first
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		// If not JSON, print the raw message
		fmt.Printf("Server response: %s\n", string(body))
		return
	}

	if result["error"] != "" {
		fmt.Printf("Error: %s\n", result["error"])
	} else {
		fmt.Printf("Node %s started successfully\n", nodeID)
	}
}
