/*
Copyright 2021 NDD.

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

package registrator

import (
	"context"
	"time"

	pkgmetav1 "github.com/yndd/ndd-core/apis/pkg/meta/v1"
	"github.com/yndd/ndd-runtime/pkg/logging"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultWaitTime                  = 1 * time.Second
	defaultRegistrationCheckInterval = 10 * time.Second
	defaultMaxServiceFail            = 3
)

// Option can be used to manipulate Register config.
type Option func(Registrator)

type WatchOptions struct {
	// RetriveServices defines if service details are required
	// as part of ServiceResponse(s)
	RetriveServices bool
}

// TargetController defines the interfaces for the target controller
type Registrator interface {
	//options
	// add a logger to the Registrator
	WithLogger(log logging.Logger)
	// Register
	Register(ctx context.Context, s *Service)
	// DeRegister
	DeRegister(ctx context.Context, id string)
	// Query
	Query(ctx context.Context, serviceName string, tags []string) ([]*Service, error)
	// GetEndpointAddress returns the address/port of the serviceEndpoint
	GetEndpointAddress(ctx context.Context, serviceName string, tags []string) (string, error)
	// Watch
	// 1 channel per service to watch
	Watch(ctx context.Context, serviceName string, tags []string, opts WatchOptions) chan *ServiceResponse
	// all services through 1 channel
	WatchCh(ctx context.Context, serviceName string, tags []string, opts WatchOptions, ch chan *ServiceResponse)
	//
	StopWatch(serviceName string)
}

// WithLogger adds a logger to the Registrator
func WithLogger(l logging.Logger) Option {
	return func(o Registrator) {
		o.WithLogger(l)
	}
}

type Service struct {
	Name         string       // service name e.g. provider or worker
	ID           string       // service instance
	Port         int          // service port
	Address      string       // service address
	Tags         []string     // service tags
	HealthChecks []HealthKind // what type of health check kinds are needed to test the service
}

type ServiceResponse struct {
	ServiceName      string
	ServiceInstances []*Service
	Err              error
}

type HealthKind string

const (
	HealthKindTTL  HealthKind = "ttl"
	HealthKindGRPC HealthKind = "grpc"
)

type Options struct {
	Logger                    logging.Logger
	Scheme                    *runtime.Scheme
	ServiceDiscoveryDcName    string
	ServiceDiscovery          pkgmetav1.ServiceDiscoveryType
	ServiceDiscoveryNamespace string
	Address                   string
}

func New(ctx context.Context, config *rest.Config, o *Options) (Registrator, error) {
	switch o.ServiceDiscovery {
	case pkgmetav1.ServiceDiscoveryTypeConsul:
		c, err := getClient(config, o)
		if err != nil {
			return nil, err
		}
		return newConsulRegistrator(ctx, c, o.ServiceDiscoveryNamespace, o.ServiceDiscoveryDcName,
			WithLogger(o.Logger))
	case pkgmetav1.ServiceDiscoveryTypeK8s:
		c, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, err
		}
		return newK8sRegistrator(ctx, c, o.ServiceDiscoveryNamespace, WithLogger(o.Logger))
	default:
		return newNopRegistrator(o), nil
	}
}

func getClient(config *rest.Config, o *Options) (client.Client, error) {
	// get client
	return client.New(config, client.Options{
		Scheme: o.Scheme,
	})
}
