// Package transport provides the authenticated QUIC boundary of the mesh.
// Every stream carries exactly one length-prefixed, validated envelope, which
// keeps framing simple and makes partial writes impossible to misinterpret.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
	"github.com/quic-go/quic-go"
)

const (
	// ProtocolName is the ALPN identifier for this protocol version.
	ProtocolName = "vortexmesh/1"
	// MaxFrameSize bounds a single envelope frame at 4 MiB.
	MaxFrameSize = 4 << 20
	// idleTimeout closes connections whose peer has gone silent, which is the
	// expected failure mode on an intermittent edge link.
	idleTimeout = 30 * time.Second
)

// Handler receives one validated envelope per QUIC stream.
type Handler func(context.Context, envelope.Envelope) error

// Server is a QUIC listener that hands validated envelopes to a Handler.
type Server struct {
	listener *quic.Listener
	handler  Handler
	onError  func(error)
}

// ServerTLSConfig builds a TLS 1.3 configuration that requires and verifies a
// client certificate signed by clientRoots.
func ServerTLSConfig(cert tls.Certificate, clientRoots *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    clientRoots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{ProtocolName},
	}
}

// ClientTLSConfig builds the matching client configuration. serverName must be
// a name present in the server certificate's SANs.
func ClientTLSConfig(cert tls.Certificate, serverRoots *x509.CertPool, serverName string) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      serverRoots,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{ProtocolName},
	}
}

// Listen starts a QUIC listener on addr. A nil handler is a programming error.
func Listen(addr string, tlsConfig *tls.Config, handler Handler) (*Server, error) {
	if tlsConfig == nil {
		return nil, errors.New("transport: TLS config is required")
	}
	if handler == nil {
		return nil, errors.New("transport: handler is required")
	}
	l, err := quic.ListenAddr(addr, tlsConfig, &quic.Config{
		MaxIdleTimeout:  idleTimeout,
		KeepAlivePeriod: idleTimeout / 3,
	})
	if err != nil {
		return nil, fmt.Errorf("listen QUIC on %s: %w", addr, err)
	}
	return &Server{listener: l, handler: handler}, nil
}

// OnError registers an optional callback for protocol-level errors. Handler
// errors are reported here instead of being silently discarded.
func (s *Server) OnError(fn func(error)) { s.onError = fn }

func (s *Server) report(err error) {
	if err != nil && s.onError != nil {
		s.onError(err)
	}
}

// Addr returns the bound UDP address.
func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Serve accepts connections until ctx is cancelled or the listener closes.
// A clean shutdown returns ctx.Err().
func (s *Server) Serve(ctx context.Context) error {
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("accept QUIC connection: %w", err)
		}
		go s.serveConnection(ctx, conn)
	}
}

// Close stops the listener. In-flight streams finish or fail with the
// listener's error.
func (s *Server) Close() error { return s.listener.Close() }

func (s *Server) serveConnection(ctx context.Context, conn quic.Connection) {
	defer func() { _ = conn.CloseWithError(0, "") }()
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.serveStream(ctx, stream)
	}
}

func (s *Server) serveStream(ctx context.Context, stream quic.Stream) {
	defer func() { _ = stream.Close() }()

	msg, err := ReadEnvelope(stream)
	if err != nil {
		s.report(fmt.Errorf("read envelope: %w", err))
		return
	}
	if err := s.handler(ctx, msg); err != nil {
		s.report(fmt.Errorf("handle envelope %s: %w", msg.ID, err))
	}
}

// Dial opens a QUIC connection to addr using the given client TLS config.
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config) (quic.Connection, error) {
	return quic.DialAddr(ctx, addr, tlsConfig, &quic.Config{
		MaxIdleTimeout:  idleTimeout,
		KeepAlivePeriod: idleTimeout / 3,
	})
}

// WriteEnvelope writes one length-prefixed envelope to w.
func WriteEnvelope(w io.Writer, msg envelope.Envelope) error {
	payload, err := msg.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	if len(payload) > MaxFrameSize {
		return fmt.Errorf("envelope exceeds %d byte frame limit", MaxFrameSize)
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write frame payload: %w", err)
	}
	return nil
}

// ReadEnvelope reads and validates one length-prefixed envelope from r.
// It rejects empty and oversized frames before allocating.
func ReadEnvelope(r io.Reader) (envelope.Envelope, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return envelope.Envelope{}, fmt.Errorf("read frame header: %w", err)
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 {
		return envelope.Envelope{}, errors.New("empty frame")
	}
	if n > MaxFrameSize {
		return envelope.Envelope{}, fmt.Errorf("frame of %d bytes exceeds %d byte limit", n, MaxFrameSize)
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return envelope.Envelope{}, fmt.Errorf("read frame payload: %w", err)
	}
	return envelope.Decode(payload)
}
