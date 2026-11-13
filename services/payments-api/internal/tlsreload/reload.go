// Package tlsreload serves a TLS certificate, key and client CA from files that can be
// replaced at runtime.
//
// The alternative -- loading the certificate once at startup -- makes a certificate
// rotation a full restart, which in this platform means a window with no payment
// ingestion. More importantly, it couples a routine security operation to the availability
// of the money path.
//
// The reload policy is fail-safe, not fail-closed: a half-written or invalid file leaves
// the previous, known-good certificate in place and reports the problem. A rotation that is
// applied incorrectly must not take the service down; it must leave it serving the old,
// still-valid certificate until the operator fixes it.
package tlsreload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// DefaultInterval is how often Start checks the files for a replacement. It is short
// enough that a rotation completes within seconds and long enough that the check is
// negligible next to request handling.
const DefaultInterval = 5 * time.Second

// Options configures a Reloader.
type Options struct {
	CertFile    string
	KeyFile     string
	CAFile      string
	RequireMTLS bool
	Logger      *slog.Logger
	Interval    time.Duration
}

// Reloader holds the current TLS material behind an RWMutex. The handshake path takes a
// read lock; a reload takes the write lock only long enough to swap the pointers.
type Reloader struct {
	opts Options

	mu        sync.RWMutex
	cert      tls.Certificate
	clientCAs *x509.CertPool
	stamp     stampSet
}

type fileStamp struct {
	mod  time.Time
	size int64
}

type stampSet struct {
	cert fileStamp
	key  fileStamp
	ca   fileStamp
}

// New loads the configured material once. A failure here is fatal on purpose: starting
// without a usable certificate would either serve plaintext when TLS was requested or fail
// every handshake later, and both are worse than refusing to start.
func New(opts Options) (*Reloader, error) {
	if opts.CertFile == "" || opts.KeyFile == "" {
		return nil, errors.New("tlsreload: cert and key files are required")
	}
	if opts.RequireMTLS && opts.CAFile == "" {
		return nil, errors.New("tlsreload: mTLS requires a client CA file")
	}
	if opts.Interval <= 0 {
		opts.Interval = DefaultInterval
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	r := &Reloader{opts: opts}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// TLSConfig returns a config whose parameters are resolved for every handshake, so a
// replaced certificate or client CA is used by the next connection without a restart.
func (r *Reloader) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return r.current(), nil
		},
	}
}

func (r *Reloader) current() *tls.Config {
	r.mu.RLock()
	defer r.mu.RUnlock()

	conf := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{r.cert},
	}
	if r.opts.RequireMTLS {
		conf.ClientAuth = tls.RequireAndVerifyClientCert
		conf.ClientCAs = r.clientCAs
	}
	return conf
}

// Reload forces a re-read of the files. It is intended for a SIGHUP handler and for tests.
func (r *Reloader) Reload() error {
	if err := r.load(); err != nil {
		r.opts.Logger.Error("tls reload failed; keeping the previous certificate", "error", err)
		return err
	}
	r.opts.Logger.Info("tls material reloaded",
		"cert", r.opts.CertFile,
		"require_mtls", r.opts.RequireMTLS,
	)
	return nil
}

// Start polls the files until ctx is cancelled. Callers should run it in its own goroutine.
func (r *Reloader) Start(ctx context.Context) {
	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.changed() {
				continue
			}
			_ = r.Reload()
		}
	}
}

func (r *Reloader) changed() bool {
	cert, key, ca := statSet(r.opts)
	r.mu.RLock()
	current := r.stamp
	r.mu.RUnlock()
	return cert != current.cert || key != current.key || ca != current.ca
}

func (r *Reloader) load() error {
	cert, err := tls.LoadX509KeyPair(r.opts.CertFile, r.opts.KeyFile)
	if err != nil {
		return fmt.Errorf("load TLS certificate/key: %w", err)
	}

	var pool *x509.CertPool
	if r.opts.RequireMTLS {
		caPEM, readErr := os.ReadFile(r.opts.CAFile)
		if readErr != nil {
			return fmt.Errorf("read TLS CA: %w", readErr)
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return errors.New("TLS CA file contains no usable certificates")
		}
	}

	certStamp, keyStamp, caStamp := statSet(r.opts)

	r.mu.Lock()
	r.cert = cert
	r.clientCAs = pool
	r.stamp = stampSet{cert: certStamp, key: keyStamp, ca: caStamp}
	r.mu.Unlock()
	return nil
}

// statSet records the current file stamps. A stat error is encoded as a zero stamp, which
// differs from the previous one and therefore triggers a reload attempt that reports the
// real error.
func statSet(opts Options) (cert, key, ca fileStamp) {
	return stat(opts.CertFile), stat(opts.KeyFile), stat(opts.CAFile)
}

func stat(path string) fileStamp {
	if path == "" {
		return fileStamp{}
	}
	info, err := os.Stat(path)
	if err != nil {
		return fileStamp{}
	}
	return fileStamp{mod: info.ModTime(), size: info.Size()}
}
