// raftcli is a tiny REPL for talking to a raftd cluster
// Caches the leader after the first successful op
// Falls back to peer-walking on 503
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type cli struct {
	peers    []string
	client   *http.Client
	leader   string
	leaderID int
	term     int
}

func main() {
	peersFlag := flag.String("peers", "", "comma-separated raftd HTTP endpoints")
	flag.Parse()

	if *peersFlag == "" {
		fmt.Fprintln(os.Stderr, "--peers is required")
		os.Exit(2)
	}

	c := &cli{
		peers:    strings.Split(*peersFlag, ","),
		client:   &http.Client{Timeout: 10 * time.Second},
		leaderID: -1,
	}

	fmt.Println("commands: put <k> <v> | get <k> | keys [pattern] | status | quit")
	c.refreshLeader()

	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(c.prompt())
		if !sc.Scan() {
			return
		}

		f := strings.Fields(sc.Text())
		if len(f) == 0 {
			continue
		}

		switch f[0] {
		case "quit", "exit":
			return

		case "status":
			c.runStatus()

		case "put":
			if len(f) < 3 {
				fmt.Println("usage: put <k> <v>")
				continue
			}
			if _, ok := c.runOp("/put?" + url.Values{"key": {f[1]}, "value": {f[2]}}.Encode()); ok {
				fmt.Println("OK")
			}

		case "get":
			if len(f) < 2 {
				fmt.Println("usage: get <k>")
				continue
			}
			if v, ok := c.runOp("/get?" + url.Values{"key": {f[1]}}.Encode()); ok {
				fmt.Println(v)
			}

		case "keys":
			pattern := "*"
			if len(f) >= 2 {
				pattern = f[1]
			}
			c.runKeys(pattern)

		default:
			fmt.Println("unknown:", f[0])
		}
	}
}

func (c *cli) prompt() string {
	if c.leaderID < 0 {
		return "[Cluster leader: ?] > "
	}
	return fmt.Sprintf("[Cluster leader: peer %d (term %d)] > ", c.leaderID, c.term)
}

// runOp issues a put/get and updates the leader cache from the response
// Returns the value field on 200; logs and returns ok=false otherwise
func (c *cli) runOp(suffix string) (string, bool) {
	body, peer, ok := c.fetch(suffix)
	if !ok {
		return "", false
	}

	var r struct {
		Value    string `json:"value"`
		LeaderID int    `json:"leader_id"`
		Term     int    `json:"term"`
	}
	_ = json.Unmarshal(body, &r)
	c.leader, c.leaderID, c.term = peer, r.LeaderID, r.Term

	return r.Value, true
}

// runKeys issues /keys?pattern=... and prints matching keys one per line
func (c *cli) runKeys(pattern string) {
	body, peer, ok := c.fetch("/keys?" + url.Values{"pattern": {pattern}}.Encode())
	if !ok {
		return
	}

	var r struct {
		Keys     []string `json:"keys"`
		LeaderID int      `json:"leader_id"`
		Term     int      `json:"term"`
	}
	_ = json.Unmarshal(body, &r)
	c.leader, c.leaderID, c.term = peer, r.LeaderID, r.Term

	if len(r.Keys) == 0 {
		fmt.Println("(no keys)")
		return
	}
	for _, k := range r.Keys {
		fmt.Println(k)
	}
}

// runStatus polls every peer and prints each response
// Updates the leader cache from whichever peer reports is_leader=true
func (c *cli) runStatus() {
	for _, p := range c.peers {
		body, status, err := c.get(p + "/status")
		if err != nil {
			fmt.Printf("%s: unreachable (%v)\n", p, err)
			continue
		}
		fmt.Printf("%s: %s\n", p, strings.TrimSpace(string(body)))

		if status != http.StatusOK {
			continue
		}

		var s struct {
			ID       int  `json:"id"`
			Term     int  `json:"term"`
			IsLeader bool `json:"is_leader"`
		}
		if err := json.Unmarshal(body, &s); err == nil && s.IsLeader {
			c.leader, c.leaderID, c.term = p, s.ID, s.Term
		}
	}
}

// refreshLeader polls /status across peers at startup so the prompt
// reflects the leader before the first op
func (c *cli) refreshLeader() {
	for _, p := range c.peers {
		body, status, err := c.get(p + "/status")
		if err != nil || status != http.StatusOK {
			continue
		}

		var s struct {
			ID       int  `json:"id"`
			Term     int  `json:"term"`
			IsLeader bool `json:"is_leader"`
		}
		if err := json.Unmarshal(body, &s); err != nil {
			continue
		}
		if s.IsLeader {
			c.leader, c.leaderID, c.term = p, s.ID, s.Term
			return
		}
	}
}

// fetch tries the cached leader first then every peer
// Returns the body and the responding peer URL on a 200
// Skips 503; surfaces other non-200 statuses
func (c *cli) fetch(suffix string) ([]byte, string, bool) {
	candidates := c.peers
	if c.leader != "" {
		candidates = append([]string{c.leader}, c.peers...)
	}

	for _, p := range candidates {
		body, status, err := c.get(p + suffix)
		if err != nil {
			continue
		}

		switch status {
		case http.StatusOK:
			return body, p, true
		case http.StatusServiceUnavailable:
			continue
		default:
			fmt.Printf("status %d: %s\n", status, strings.TrimSpace(string(body)))
			return nil, "", false
		}
	}

	fmt.Println("no peer accepted")
	return nil, "", false
}

func (c *cli) get(url string) ([]byte, int, error) {
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, nil
}
