package wal

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestAppendPendingAndAcknowledge(t *testing.T) {
	s := openStore(t)

	msg, err := envelope.New("m-1", envelope.StateSync, "sync", "node-a", map[string]int{"version": 2})
	if err != nil {
		t.Fatal(err)
	}

	seq, err := s.Append(context.Background(), msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, err := s.Pending(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(entries) != 1 || entries[0].Sequence != seq {
		t.Fatalf("unexpected entries: %+v", entries)
	}
	if entries[0].Message.ID != "m-1" {
		t.Fatalf("decoded id = %q, want m-1", entries[0].Message.ID)
	}

	if err := s.MarkDelivered(context.Background(), seq); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	entries, err = s.Pending(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty pending set: %+v", entries)
	}
}

func TestAppendIsIdempotentByMessageID(t *testing.T) {
	s := openStore(t)

	msg, err := envelope.New("m-1", envelope.TaskRequest, "tasks", "node-a", map[string]string{"action": "sync"})
	if err != nil {
		t.Fatal(err)
	}

	first, err := s.Append(context.Background(), msg)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	second, err := s.Append(context.Background(), msg)
	if err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if first != second {
		t.Fatalf("duplicate IDs created sequences %d and %d", first, second)
	}

	entries, err := s.Pending(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one stored entry, got %d", len(entries))
	}
}

func TestPendingIsOrderedAndLimited(t *testing.T) {
	s := openStore(t)

	for _, id := range []string{"a", "b", "c"} {
		msg, err := envelope.New(id, envelope.Telemetry, "telemetry", "node-a", map[string]string{"id": id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Append(context.Background(), msg); err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
	}

	entries, err := s.Pending(context.Background(), 2)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len = %d, want 2", len(entries))
	}
	if entries[0].Message.ID != "a" || entries[1].Message.ID != "b" {
		t.Fatalf("unexpected order: %q, %q", entries[0].Message.ID, entries[1].Message.ID)
	}
	if entries[0].Sequence >= entries[1].Sequence {
		t.Fatal("expected strictly increasing sequence numbers")
	}
}

func TestAppendRejectsInvalidEnvelope(t *testing.T) {
	s := openStore(t)

	if _, err := s.Append(context.Background(), envelope.Envelope{}); err == nil {
		t.Fatal("expected invalid envelope to be rejected")
	}
}
