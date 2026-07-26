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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/llm-d/llm-d-router/internal/runnable"
	"github.com/llm-d/llm-d-router/pkg/common"
	"github.com/llm-d/llm-d-router/pkg/epp/controller"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts"
	fwkfc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	"github.com/llm-d/llm-d-router/pkg/epp/requestcontrol"
)

// A Frontend serves routing decisions over a client protocol.
type Frontend interface {
	// Serve blocks until ctx ends or serving fails.
	Serve(ctx context.Context, ss *handlers.StreamingServer) error
}

// ExtProcServerRunner provides methods to manage an external process server.
type ExtProcServerRunner struct {
	// streaming is the phase handler, built once and shared by every frontend.
	streamingOnce sync.Once
	streaming     *handlers.StreamingServer

	GrpcPort int
	// GrpcListener is an optional pre-bound listener for the ext_proc server.
	// When set, GrpcPort is ignored. Reserving the port in advance of this
	// runnable starting closes the window in which another process can take a
	// port that was selected but not yet bound.
	GrpcListener                     net.Listener
	GKNN                             common.GKNN
	ControllerCfg                    ControllerConfig
	Datastore                        datastore.Datastore
	SecureServing                    bool
	HealthChecking                   bool
	CertPath                         string
	EnableCertReload                 bool
	RefreshPrometheusMetricsInterval time.Duration
	MetricsStalenessThreshold        time.Duration
	Director                         *requestcontrol.Director
	ParserRegistry                   *handlers.ParserRegistry
	SaturationDetector               fwkfc.SaturationDetector
	PriorityBandControlPlane         contracts.PriorityBandControlPlane
	GRPCMaxRecvMsgSize               int
	GRPCMaxSendMsgSize               int
	EnableGRPCStreamMetrics          bool
}

// NewDefaultExtProcServerRunner creates a runner with default values.
// Note: Dependencies like Datastore, Scheduler, SD need to be set separately.
func NewDefaultExtProcServerRunner() *ExtProcServerRunner {
	opts := NewOptions()
	if opts.PoolNamespace == "" {
		opts.PoolNamespace = DefaultPoolNamespace
	}

	gknn := common.GKNN{
		NamespacedName: types.NamespacedName{Name: opts.PoolName, Namespace: opts.PoolNamespace},
		GroupKind: schema.GroupKind{
			Group: opts.PoolGroup,
			Kind:  "InferencePool",
		},
	}
	return &ExtProcServerRunner{
		GrpcPort:           opts.GRPCPort,
		GRPCMaxRecvMsgSize: opts.GRPCMaxRecvMsgSize,
		GRPCMaxSendMsgSize: opts.GRPCMaxSendMsgSize,
		GKNN:               gknn,
		ControllerCfg: ControllerConfig{
			startCrdReconcilers:       true,
			hasInferenceObjective:     true,
			hasInferenceModelRewrites: true,
			InferenceObjectiveGV:      inferenceAPIGV,
			InferenceModelRewriteGV:   inferenceAPIGV,
		},
		SecureServing:                    opts.SecureServing,
		HealthChecking:                   opts.HealthChecking,
		RefreshPrometheusMetricsInterval: opts.RefreshPrometheusMetricsInterval,
		MetricsStalenessThreshold:        opts.MetricsStalenessThreshold,
		// Dependencies can be assigned later.
	}
}

// StreamingServer builds the shared decider once. Every frontend receives the
// same instance, so one buffer pool serves both and they cannot drift apart.
func (r *ExtProcServerRunner) StreamingServer() *handlers.StreamingServer {
	r.streamingOnce.Do(func() {
		poolCap := r.GRPCMaxRecvMsgSize
		if poolCap == 0 {
			poolCap = defaultMaxRoutableBodyBytes
		}
		r.streaming = handlers.NewStreamingServer(r.Datastore, r.Director, r.ParserRegistry, poolCap)
	})
	return r.streaming
}

// GRPCFrontend builds the gRPC frontend from this runner's settings.
func (r *ExtProcServerRunner) GRPCFrontend() Frontend {
	return grpcFrontend{
		Datastore:                        r.Datastore,
		RefreshPrometheusMetricsInterval: r.RefreshPrometheusMetricsInterval,
		MetricsStalenessThreshold:        r.MetricsStalenessThreshold,
		SecureServing:                    r.SecureServing,
		CertPath:                         r.CertPath,
		EnableCertReload:                 r.EnableCertReload,
		GRPCMaxRecvMsgSize:               r.GRPCMaxRecvMsgSize,
		GRPCMaxSendMsgSize:               r.GRPCMaxSendMsgSize,
		EnableGRPCStreamMetrics:          r.EnableGRPCStreamMetrics,
		HealthChecking:                   r.HealthChecking,
		GrpcPort:                         r.GrpcPort,
		GrpcListener:                     r.GrpcListener,
	}
}

// AsRunnable returns the gRPC frontend as a runnable, for existing callers.
// cmd/epp/runner composes the frontend list.
// The runnable implements LeaderElectionRunnable with leader election disabled.
func (r *ExtProcServerRunner) AsRunnable(logger logr.Logger) manager.Runnable {
	return runnable.NoLeaderElection(manager.RunnableFunc(func(ctx context.Context) error {
		return r.GRPCFrontend().Serve(log.IntoContext(ctx, logger), r.StreamingServer())
	}))
}

// SetupWithManager sets up the runner with the given manager.
func (r *ExtProcServerRunner) SetupWithManager(mgr ctrl.Manager) error {
	// Create the controllers and register them with the manager
	// When PopulateNonLeaderDatastore is set the reconcilers run on every
	// replica (not just the leader) so non-leaders keep a populated datastore.
	runOnNonLeaders := r.ControllerCfg.PopulateNonLeaderDatastore
	if r.ControllerCfg.startCrdReconcilers {
		if err := (&controller.InferencePoolReconciler{
			Datastore:       r.Datastore,
			Reader:          mgr.GetClient(),
			RunOnNonLeaders: runOnNonLeaders,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("failed setting up InferencePoolReconciler - %w", err)
		}

		if r.ControllerCfg.hasInferenceObjective {
			if err := (&controller.InferenceObjectiveReconciler{
				Datastore:                r.Datastore,
				Reader:                   mgr.GetClient(),
				PoolGKNN:                 r.GKNN,
				PriorityBandControlPlane: r.PriorityBandControlPlane,
				RunOnNonLeaders:          runOnNonLeaders,
			}).SetupWithManager(mgr); err != nil {
				return fmt.Errorf("failed setting up InferenceObjectiveReconciler - %w", err)
			}
		}
		if r.ControllerCfg.hasInferenceModelRewrites {
			if err := (&controller.InferenceModelRewriteReconciler{
				Datastore:       r.Datastore,
				Reader:          mgr.GetClient(),
				PoolGKNN:        r.GKNN,
				RunOnNonLeaders: runOnNonLeaders,
			}).SetupWithManager(mgr); err != nil {
				return fmt.Errorf("failed setting up InferenceModelRewriteReconciler - %w", err)
			}
		}
	}

	if err := (&controller.PodReconciler{
		Datastore:       r.Datastore,
		Reader:          mgr.GetClient(),
		RunOnNonLeaders: runOnNonLeaders,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed setting up PodReconciler - %w", err)
	}
	return nil
}
