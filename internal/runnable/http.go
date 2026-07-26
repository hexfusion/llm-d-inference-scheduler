/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runnable

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ReadHeaderTimeout alone does not cover the body, and with ReadTimeout and
// IdleTimeout both zero net/http sets no body deadline and never reaps idle
// keep-alives, so a trickled body behind a large Content-Length would pin memory
// indefinitely.
const (
	defaultReadHeaderTimeout = 10 * time.Second
	defaultReadTimeout       = 60 * time.Second
	defaultWriteTimeout      = 60 * time.Second
	defaultIdleTimeout       = 90 * time.Second
	defaultMaxHeaderBytes    = 1 << 20 // net/http's DefaultMaxHeaderBytes
	httpShutdownTimeout      = 10 * time.Second
)

// newHTTPServer applies the shared timeout policy so the two entry points below
// cannot drift apart.
func newHTTPServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    defaultMaxHeaderBytes,
	}
}

// HTTPServer converts the given handler into a runnable serving on port.
// The server name is just being used for logging.
func HTTPServer(name string, h http.Handler, port int) manager.Runnable {
	return manager.RunnableFunc(func(ctx context.Context) error {
		// Use "name" key as that is what manager.Server does as well.
		log := ctrl.Log.WithValues("name", name)
		log.Info("HTTP server starting")

		srv := newHTTPServer(h)
		srv.Addr = fmt.Sprintf(":%d", port)
		log.Info("HTTP server listening", "port", port)
		return serveHTTP(ctx, log, func() error { return srv.ListenAndServe() }, srv)
	})
}

// HTTPServerOnListener converts the given handler into a runnable serving on an
// already-bound listener. Mirrors GRPCServerOnListener: reserving the port in
// advance of the runnable starting removes the window in which another process
// can take a port that was selected but not yet bound. Takes ownership of the
// listener: http.Server.Serve closes it on return.
func HTTPServerOnListener(name string, h http.Handler, lis net.Listener) manager.Runnable {
	return manager.RunnableFunc(func(ctx context.Context) error {
		log := ctrl.Log.WithValues("name", name)
		log.Info("HTTP server starting")

		srv := newHTTPServer(h)
		log.Info("HTTP server listening", "address", lis.Addr().String())
		return serveHTTP(ctx, log, func() error { return srv.Serve(lis) }, srv)
	})
}

// serveHTTP runs serve until the context closes, then shuts the server down
// gracefully. serve is ListenAndServe or Serve depending on the entry point.
func serveHTTP(ctx context.Context, log logr.Logger, serve func() error, srv *http.Server) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-ctx.Done():
			log.Info("HTTP server shutting down")
			sctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
			defer cancel()
			_ = srv.Shutdown(sctx)
		case <-doneCh:
		}
	}()

	if err := serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server failed - %w", err)
	}
	log.Info("HTTP server terminated")
	return nil
}
