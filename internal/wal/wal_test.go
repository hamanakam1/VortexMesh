package wal

import (
	"context"
	"path/filepath"
	"testing"
	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func TestAppendPendingAndAcknowledge(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "messages.db")); if err != nil { t.Fatal(err) }; defer s.Close()
	msg, _ := envelope.New("m-1", envelope.StateSync, "sync", "node-a", map[string]int{"version": 2})
	seq, err := s.Append(context.Background(), msg); if err != nil { t.Fatal(err) }
	entries, err := s.Pending(context.Background(), 10); if err != nil { t.Fatal(err) }; if len(entries) != 1 || entries[0].Sequence != seq { t.Fatalf("unexpected entries: %+v", entries) }
	if err := s.MarkDelivered(context.Background(), seq); err != nil { t.Fatal(err) }
	entries, err = s.Pending(context.Background(), 10); if err != nil { t.Fatal(err) }; if len(entries) != 0 { t.Fatalf("expected empty pending set: %+v", entries) }
}

func TestAppendIsIdempotentByMessageID(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "messages.db")); if err != nil { t.Fatal(err) }; defer s.Close()
	msg, _ := envelope.New("m-1", envelope.TaskRequest, "tasks", "node-a", map[string]string{"action": "sync"})
	one, err := s.Append(context.Background(), msg); if err != nil { t.Fatal(err) }; two, err := s.Append(context.Background(), msg); if err != nil { t.Fatal(err) }; if one != two { t.Fatalf("duplicate IDs created sequences %d and %d", one, two) }
}
