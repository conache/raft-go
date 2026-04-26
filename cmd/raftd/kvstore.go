package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"path"
	"sort"
	"sync"
)

type Op struct {
	// Kind is "PUT", "GET", or "LIST" (Key holds the glob pattern for LIST)
	Kind  string
	Key   string
	Value string
}

func init() { gob.Register(Op{}) }

type kvStore struct {
	mu sync.Mutex
	m  map[string]string
}

func newKV() *kvStore { return &kvStore{m: map[string]string{}} }

func (kv *kvStore) DoOp(req any) any {
	op := req.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.Kind {
	case "PUT":
		kv.m[op.Key] = op.Value
		return ""
	case "GET":
		return kv.m[op.Key]
	case "LIST":
		var keys []string
		for k := range kv.m {
			if matched, _ := path.Match(op.Key, k); matched {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		return keys
	}

	return ""
}

func (kv *kvStore) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv.m); err != nil {
		log.Fatalf("snapshot encode: %v", err)
	}

	return buf.Bytes()
}

func (kv *kvStore) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var m map[string]string
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		log.Fatalf("snapshot decode: %v", err)
	}

	kv.m = m
}
