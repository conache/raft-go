// dslogs renders Dlog output in a readable, optionally column-per-peer format.
//
// Usage:
//
//	dslogs [flags] [file]
//
// Flags:
//
//	-c, --columns N     render N columns, one per peer (0 = single column)
//	-i, --ignore LIST   skip these comma-separated topics
//	-j, --just LIST     keep only these comma-separated topics
//	--no-color          disable ANSI colorization
//
// Reads stdin when no file is given.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// topics maps topic codes to hex RGB colors, mirroring tools/dslogs.py.
var topics = map[string]string{
	"TIMR":   "#9a9a99",
	"VOTE":   "#67a0b2",
	"LEAD":   "#d0b343",
	"SUCC":   "#70c43f",
	"LOG1":   "#4878bc",
	"LOG2":   "#398280",
	"CMIT":   "#98719f",
	"PERS":   "#d08341",
	"SNAP":   "#FD971F",
	"DROP":   "#ff615c",
	"CLNT":   "#00813c",
	"HBIT":   "#fe2c79",
	"INFO":   "#ffffff",
	"WARN":   "#d08341",
	"ERRO":   "#fe2626",
	"TRCE":   "#fe2626",
	"CTRLER": "#7c38a1",
	"SGCL":   "#e6a800",
	"SGSV":   "#2eb8b8",
}

type config struct {
	noColor bool
	cols    int
	just    string
	ignore  string
	file    string
}

func main() {
	cfg := parseFlags()

	in, closeFn, err := openInput(cfg.file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dslogs:", err)
		os.Exit(1)
	}
	defer closeFn()

	allowed, err := buildAllowed(cfg.just, cfg.ignore)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dslogs:", err)
		os.Exit(2)
	}

	width := terminalWidth()

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1<<16), 1<<20)

	panicSeen := false
	for scanner.Scan() {
		processLine(scanner.Text(), cfg, allowed, width, &panicSeen)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "dslogs: read:", err)
		os.Exit(3)
	}
}

func parseFlags() config {
	var cfg config
	flag.BoolVar(&cfg.noColor, "no-color", false, "disable ANSI colorization")
	flag.IntVar(&cfg.cols, "columns", 0, "render N columns, one per peer (0 = single column)")
	flag.IntVar(&cfg.cols, "c", 0, "short for --columns")
	flag.StringVar(&cfg.just, "just", "", "keep only these comma-separated topics")
	flag.StringVar(&cfg.just, "j", "", "short for --just")
	flag.StringVar(&cfg.ignore, "ignore", "", "skip these comma-separated topics")
	flag.StringVar(&cfg.ignore, "i", "", "short for --ignore")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: dslogs [flags] [file]")
		fmt.Fprintln(os.Stderr, "  -c, --columns N     render N columns, one per peer (0 = single)")
		fmt.Fprintln(os.Stderr, "  -i, --ignore LIST   skip these comma-separated topics")
		fmt.Fprintln(os.Stderr, "  -j, --just LIST     keep only these comma-separated topics")
		fmt.Fprintln(os.Stderr, "      --no-color      disable ANSI colorization")
	}

	flag.Parse()
	if flag.NArg() > 0 {
		cfg.file = flag.Arg(0)
	}
	return cfg
}

func openInput(path string) (*os.File, func(), error) {
	if path == "" {
		return os.Stdin, func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

// buildAllowed returns the set of topics that should pass through the filter.
// --just wins over --ignore when both are set.
func buildAllowed(just, ignore string) (map[string]bool, error) {
	valid := func(t string) bool {
		_, ok := topics[t]
		return ok || t == "TEST"
	}

	if just != "" {
		allowed := make(map[string]bool)
		for _, t := range strings.Split(just, ",") {
			t = strings.TrimSpace(t)
			if !valid(t) {
				return nil, fmt.Errorf("unknown topic: %s", t)
			}
			allowed[t] = true
		}
		return allowed, nil
	}

	allowed := make(map[string]bool, len(topics)+1)
	for t := range topics {
		allowed[t] = true
	}
	allowed["TEST"] = true
	if ignore != "" {
		for _, t := range strings.Split(ignore, ",") {
			t = strings.TrimSpace(t)
			if !valid(t) {
				return nil, fmt.Errorf("unknown topic: %s", t)
			}
			delete(allowed, t)
		}
	}
	return allowed, nil
}

// terminalWidth returns the terminal's column count. It prefers a direct
// TIOCGWINSZ query on stdout, falls back to COLUMNS env var, and finally to
// a hardcoded 120.
func terminalWidth() int {
	if w := ttyColumns(); w > 0 {
		return w
	}
	if s := os.Getenv("COLUMNS"); s != "" {
		if w, err := strconv.Atoi(s); err == nil && w > 0 {
			return w
		}
	}
	return 120
}

// processLine parses one line and emits it in the configured form.
func processLine(line string, cfg config, allowed map[string]bool, width int, panicSeen *bool) {
	trimmed := strings.TrimSpace(line)
	fields := strings.SplitN(trimmed, " ", 3)

	if len(fields) < 3 {
		handleUnparseable(line, cfg, width, panicSeen)
		return
	}
	time, topic, msg := fields[0], fields[1], fields[2]

	if !allowed[topic] {
		return
	}

	if !cfg.noColor {
		if hex, ok := topics[topic]; ok {
			msg = colorize(msg, hex)
		}
	}

	if cfg.cols == 0 || topic == "TEST" {
		fmt.Println(time, msg)
		return
	}

	peerID := extractPeerID(fields[2])
	if peerID < 0 || peerID >= cfg.cols {
		fmt.Println(time, msg)
		return
	}

	colWidth := width / cfg.cols
	cols := make([]string, cfg.cols)
	cols[peerID] = msg
	fmt.Println(joinColumns(cols, colWidth))
}

// handleUnparseable matches the Python script's behavior for lines that
// don't parse as "<time> <topic> <msg>": panic traces pass through, other
// noise gets a banner unless --just filters it out.
func handleUnparseable(line string, cfg config, width int, panicSeen *bool) {
	if strings.HasPrefix(line, "panic") {
		*panicSeen = true
	}
	if cfg.just != "" && !*panicSeen {
		return
	}
	if !*panicSeen {
		fmt.Println(strings.Repeat("#", width))
	}
	fmt.Println(line)
}

// extractPeerID reads the peer id from the first two chars of msg, which
// Dlog format guarantees to be "S<digit>;...".
func extractPeerID(msg string) int {
	if len(msg) < 2 || msg[0] != 'S' {
		return -1
	}
	if !unicode.IsDigit(rune(msg[1])) {
		return -1
	}
	id, err := strconv.Atoi(string(msg[1]))
	if err != nil {
		return -1
	}
	return id
}

// colorize wraps msg in ANSI 24-bit color escape codes.
func colorize(msg, hex string) string {
	r, g, b := parseHex(hex)
	return fmt.Sprintf("\x1b[38;2;%d;%d;%dm%s\x1b[0m", r, g, b, msg)
}

func parseHex(hex string) (int, int, int) {
	s := strings.TrimPrefix(hex, "#")
	n, err := strconv.ParseUint(s, 16, 24)
	if err != nil {
		return 255, 255, 255
	}
	return int((n >> 16) & 0xFF), int((n >> 8) & 0xFF), int(n & 0xFF)
}

// joinColumns renders cols as a single line, padding each column to
// colWidth-1 visual characters (ANSI codes don't count toward width).
func joinColumns(cols []string, colWidth int) string {
	var sb strings.Builder
	for i, c := range cols {
		sb.WriteString(c)
		if pad := colWidth - 1 - visualLen(c); pad > 0 {
			sb.WriteString(strings.Repeat(" ", pad))
		}
		if i < len(cols)-1 {
			sb.WriteByte(' ')
		}
	}
	return sb.String()
}

// visualLen counts printable characters in s, ignoring ANSI SGR escapes.
func visualLen(s string) int {
	n, inEscape := 0, false
	for _, r := range s {
		if inEscape {
			if r == 'm' {
				inEscape = false
			}
			continue
		}
		if r == '\x1b' {
			inEscape = true
			continue
		}
		n++
	}
	return n
}
