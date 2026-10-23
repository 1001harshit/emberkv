package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/keyspace"
	"github.com/1001harshit/emberkv/lsm"
)

func TestAuth(t *testing.T) {
	s, addr := startServer(t, lsm.Options{})
	s.RequirePassword("s3cret")
	c := dial(t, addr)
	c.expect(resp.Error("NOAUTH Authentication required."), "GET", "k")
	c.expect(resp.Error("NOAUTH Authentication required."), "PING")
	c.expectErr("ERR unknown command", "NOSUCH") // reported before auth, as in Redis
	c.expect(resp.Error("WRONGPASS invalid username-password pair or user is disabled."), "AUTH", "wrong")
	c.expect(resp.Error("WRONGPASS invalid username-password pair or user is disabled."), "AUTH", "admin", "s3cret")
	c.expect("OK", "AUTH", "s3cret")
	c.expect("OK", "SET", "k", "v")
	c.expect("v", "GET", "k")

	other := dial(t, addr) // authentication is per connection
	other.expect(resp.Error("NOAUTH Authentication required."), "GET", "k")
	other.expect("OK", "AUTH", "default", "s3cret")
	other.expect("v", "GET", "k")

	// A failed AUTH logs the connection out again.
	c.expectErr("WRONGPASS", "AUTH", "nope")
	c.expect(resp.Error("NOAUTH Authentication required."), "GET", "k")
}

func TestAuthWithoutPassword(t *testing.T) {
	c := newClient(t)
	c.expectErr("ERR AUTH <password> called without any password configured", "AUTH", "x")
	c.expect("PONG", "PING")
}

func TestTLS(t *testing.T) {
	cert, pool := selfSigned(t)
	st, err := keyspace.Open(t.TempDir(), lsm.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(st, nil)
	go srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}}))
	t.Cleanup(func() { srv.Close(); st.Close() })

	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &client{t: t, conn: conn, r: resp.NewReader(conn), w: resp.NewWriter(conn)}
	c.expect("OK", "SET", "secure", "yes")
	c.expect("yes", "GET", "secure")

	// A plaintext client gets no service from a TLS listener.
	plain, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	plain.Write([]byte("PING\r\n"))
	plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	if n, _ := plain.Read(buf); n > 0 && string(buf[:n]) == "+PONG\r\n" {
		t.Fatal("plaintext PING was answered by a TLS listener")
	}
}

func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}
