// Package wal persists envelopes while a node is offline so they can be
// replayed, in order, once connectivity returns.
package wal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
	_ "modernc.org/sqlite" // pure-Go SQLite driver: no cgo, cross-compiles cleanly.
)

// Entry is one pending record with its monotonic sequence number.
type Entry struct {
	Sequence uint64
	Message  envelope.Envelope
}

// Store is a SQLite-backed write-ahead log.
type Store struct {
	db *sql.DB
}

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS messages (
    sequence   INTEGER PRIMARY KEY AUTOINCREMENT,
    id         TEXT    NOT NULL UNIQUE,
    envelope   BLOB    NOT NULL,
    delivered  INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_pending ON messages (delivered, sequence);
`

// Open creates or opens a WAL at path and applies the schema. A single
// connection is used deliberately: SQLite serialises writes anyway, and one
// connection keeps sequence ordering predictable.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize WAL: %w", err)
	}
	return &Store{db: db}, nil
}

// Append records msg and returns its sequence number. Duplicate message IDs
// are idempotent: the existing sequence is returned and nothing is written,
// so a retry after a failed acknowledgment cannot create a second copy.
func (s *Store) Append(ctx context.Context, msg envelope.Envelope) (uint64, error) {
	if err := msg.Validate(); err != nil {
		return 0, err
	}
	payload, err := msg.MarshalJSON()
	if err != nil {
		return 0, err
	}

	result, err := s.db.ExecContext(ctx,
		`INSERT INTO messages (id, envelope, created_at) VALUES (?, ?, ?)
		 ON CONFLICT (id) DO NOTHING`,
		msg.ID, payload, msg.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, fmt.Errorf("append WAL entry: %w", err)
	}

	if n, err := result.RowsAffected(); err == nil && n == 0 {
		var seq uint64
		if err := s.db.QueryRowContext(ctx, `SELECT sequence FROM messages WHERE id = ?`, msg.ID).Scan(&seq); err != nil {
			return 0, fmt.Errorf("look up existing WAL entry: %w", err)
		}
		return seq, nil
	}

	seq, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read WAL sequence: %w", err)
	}
	return uint64(seq), nil
}

// Pending returns up to limit undelivered entries in sequence order.
func (s *Store) Pending(ctx context.Context, limit int) ([]Entry, error) {
	if limit < 1 {
		limit = 1
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT sequence, envelope FROM messages WHERE delivered = 0 ORDER BY sequence LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending WAL entries: %w", err)
	}
	defer func() { _ = rows.Close() }()

	entries := make([]Entry, 0, limit)
	for rows.Next() {
		var (
			seq     uint64
			payload []byte
		)
		if err := rows.Scan(&seq, &payload); err != nil {
			return nil, fmt.Errorf("scan WAL entry: %w", err)
		}
		msg, err := envelope.Decode(payload)
		if err != nil {
			return nil, fmt.Errorf("decode WAL entry %d: %w", seq, err)
		}
		entries = append(entries, Entry{Sequence: seq, Message: msg})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate WAL entries: %w", err)
	}
	return entries, nil
}

// MarkDelivered acknowledges a sequence so it is no longer replayed.
func (s *Store) MarkDelivered(ctx context.Context, sequence uint64) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE messages SET delivered = 1 WHERE sequence = ?`, sequence); err != nil {
		return fmt.Errorf("mark WAL entry %d delivered: %w", sequence, err)
	}
	return nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }
