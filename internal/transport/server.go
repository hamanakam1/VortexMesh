package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
	"github.com/quic-go/quic-go"
)

const maxFrameSize = 4 << 20

// Handler receives one validated envelope per QUIC stream.
type Handler func(context.Context, envelope.Envelope) error

type Server struct {
	listener *quic.Listener
	handler  Handler
}

// TLSConfig creates a server configuration that requires and verifies client certificates.
func TLSConfig(cert tls.Certificate, clientRoots *x509.CertPool, serverName string) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs: clientRoots,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
		NextProtos: []string{"vortexmesh/1"},
	}
}

func Listen(addr string, tlsConfig *tls.Config, handler Handler) (*Server, error) {
	if tlsConfig == nil || handler == nil { return nil, fmt.Errorf("TLS config and handler are required") }
	l, err := quic.ListenAddr(addr, tlsConfig, &quic.Config{MaxIdleTimeout: 0})
	if err != nil { return nil, fmt.Errorf("listen QUIC on %s: %w", addr, err) }
	return &Server{listener: l, handler: handler}, nil
}

func (s *Server) Addr() net.Addr { return s.listener.Addr() }

func (s *Server) Serve(ctx context.Context) error {
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil { return err }
		go s.serveConnection(ctx, conn)
	}
}

func (s *Server) Close() error { return s.listener.Close() }

func (s *Server) serveConnection(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil { return }
		go s.serveStream(ctx, stream)
	}
}

func (s *Server) serveStream(ctx context.Context, stream *quic.Stream) {
	defer stream.Close()
	var length [4]byte
	if _, err := io.ReadFull(stream, length[:]); err != nil { return }
	n := binary.BigEndian.Uint32(length[:])
	if n == 0 || n > maxFrameSize { return }
	payload := make([]byte, n)
	if _, err := io.ReadFull(stream, payload); err != nil { return }
	var msg envelope.Envelope
	if err := json.Unmarshal(payload, &msg); err != nil || msg.Validate() != nil { return }
	_ = s.handler(ctx, msg)
}

// WriteEnvelope writes one length-prefixed envelope to a QUIC stream.
func WriteEnvelope(w io.Writer, msg envelope.Envelope) error {
	payload, err := json.Marshal(msg)
	if err != nil { return err }
	if len(payload) > maxFrameSize { return fmt.Errorf("envelope exceeds %d byte frame limit", maxFrameSize) }
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	if _, err := w.Write(length[:]); err != nil { return err }
	_, err = w.Write(payload)
	return err
}
