package wal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
	_ "modernc.org/sqlite"
)

type Entry struct {
	Sequence uint64
	Message  envelope.Envelope
}

type Store struct { db *sql.DB }

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil { return nil, fmt.Errorf("open WAL: %w", err) }
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS messages (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, envelope BLOB NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL); CREATE INDEX IF NOT EXISTS messages_pending ON messages(delivered, sequence);`); err != nil { _ = db.Close(); return nil, fmt.Errorf("initialize WAL: %w", err) }
	return &Store{db: db}, nil
}

func (s *Store) Append(ctx context.Context, msg envelope.Envelope) (uint64, error) {
	if err := msg.Validate(); err != nil { return 0, err }
	payload, err := json.Marshal(msg)
	if err != nil { return 0, err }
	result, err := s.db.ExecContext(ctx, `INSERT INTO messages(id, envelope, created_at) VALUES(?, ?, ?) ON CONFLICT(id) DO NOTHING`, msg.ID, payload, msg.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
	if err != nil { return 0, fmt.Errorf("append WAL entry: %w", err) }
	if n, _ := result.RowsAffected(); n == 0 { var seq uint64; err = s.db.QueryRowContext(ctx, `SELECT sequence FROM messages WHERE id=?`, msg.ID).Scan(&seq); return seq, err }
	seq, err := result.LastInsertId()
	return uint64(seq), err
}

func (s *Store) Pending(ctx context.Context, limit int) ([]Entry, error) {
	if limit < 1 { limit = 1 }
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, envelope FROM messages WHERE delivered=0 ORDER BY sequence LIMIT ?`, limit)
	if err != nil { return nil, err }; defer rows.Close()
	var entries []Entry
	for rows.Next() { var seq uint64; var payload []byte; if err := rows.Scan(&seq, &payload); err != nil { return nil, err }; var msg envelope.Envelope; if err := json.Unmarshal(payload, &msg); err != nil { return nil, err }; entries = append(entries, Entry{Sequence: seq, Message: msg}) }
	return entries, rows.Err()
}

func (s *Store) MarkDelivered(ctx context.Context, sequence uint64) error { _, err := s.db.ExecContext(ctx, `UPDATE messages SET delivered=1 WHERE sequence=?`, sequence); return err }

func (s *Store) Close() error { return s.db.Close() }
