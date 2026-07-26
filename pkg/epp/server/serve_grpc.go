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

package server

import (
	"context"
	"crypto/tls"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"net"
	"time"

	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Register gzip compressor.
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"github.com/llm-d/llm-d-router/internal/runnable"
	tlsutil "github.com/llm-d/llm-d-router/internal/tls"
	"github.com/llm-d/llm-d-router/pkg/common"
	datalayerlogger "github.com/llm-d/llm-d-router/pkg/epp/datalayer/logger"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	"github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

// grpcFrontend serves ext_proc over a grpc.Server.
type grpcFrontend struct {
	Datastore                        datastore.Datastore
	RefreshPrometheusMetricsInterval time.Duration
	MetricsStalenessThreshold        time.Duration

	SecureServing    bool
	CertPath         string
	EnableCertReload bool

	GRPCMaxRecvMsgSize      int
	GRPCMaxSendMsgSize      int
	EnableGRPCStreamMetrics bool
	HealthChecking          bool

	GrpcPort int
	// GrpcListener is an optional pre-bound listener. When set, GrpcPort is ignored.
	GrpcListener net.Listener
}

func (d grpcFrontend) Serve(ctx context.Context, ss *handlers.StreamingServer) error {
	logger := log.FromContext(ctx)
	datalayerlogger.StartMetricsLogger(ctx, d.Datastore, d.RefreshPrometheusMetricsInterval, d.MetricsStalenessThreshold)

	var srv *grpc.Server
	var creds credentials.TransportCredentials
	if d.SecureServing {
		var cert tls.Certificate
		var err error
		if d.CertPath != "" {
			cert, err = tls.LoadX509KeyPair(d.CertPath+"/tls.crt", d.CertPath+"/tls.key")
		} else {
			// Create tls based credential.
			cert, err = tlsutil.CreateSelfSignedTLSCertificate(logger)
		}
		if err != nil {
			return fmt.Errorf("failed to create self signed certificate - %w", err)
		}

		if d.CertPath != "" && d.EnableCertReload {
			reloader, err := common.NewCertReloader(ctx, d.CertPath, &cert)
			if err != nil {
				return fmt.Errorf("failed to create cert reloader: %w", err)
			}
			creds = credentials.NewTLS(&tls.Config{
				GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
					return reloader.Get(), nil
				},
				NextProtos: []string{"h2"},
			})
		} else {
			creds = credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{"h2"},
			})
		}
	}

	var grpcOpts []grpc.ServerOption
	if creds != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}
	if d.GRPCMaxRecvMsgSize > 0 {
		grpcOpts = append(grpcOpts, grpc.MaxRecvMsgSize(d.GRPCMaxRecvMsgSize))
	}
	if d.GRPCMaxSendMsgSize > 0 {
		grpcOpts = append(grpcOpts, grpc.MaxSendMsgSize(d.GRPCMaxSendMsgSize))
	}
	if d.EnableGRPCStreamMetrics {
		metrics.RegisterGRPCStreamMetrics()
		grpcOpts = append(grpcOpts, grpc.ChainStreamInterceptor(streamMetricsInterceptor))
	}

	srv = grpc.NewServer(grpcOpts...)

	extProcPb.RegisterExternalProcessorServer(srv, ss)

	if d.HealthChecking {
		healthcheck := health.NewServer()
		healthgrpc.RegisterHealthServer(srv,
			healthcheck,
		)
		svcName := extProcPb.ExternalProcessor_ServiceDesc.ServiceName
		logger.Info("Setting ExternalProcessor service status to SERVING", "serviceName", svcName)
		healthcheck.SetServingStatus(svcName, healthgrpc.HealthCheckResponse_SERVING)
	}

	// Forward to the gRPC runnable.
	if d.GrpcListener != nil {
		return runnable.GRPCServerOnListener("ext-proc", srv, d.GrpcListener).Start(ctx)
	}
	return runnable.GRPCServer("ext-proc", srv, d.GrpcPort).Start(ctx)
}
