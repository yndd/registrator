package registrator

import (
	"context"

	"github.com/yndd/ndd-runtime/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newk8sRegistrator(ctx context.Context, namespace string, opts ...Option) (Registrator, error) {
	if namespace == "" {
		namespace = "ndd-system"
	}
	r := new(k8sRegistrator)
	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// k8sRegistrator implements the Registrator interface
type k8sRegistrator struct {
	client client.Client
	log    logging.Logger
}

func (r *k8sRegistrator) WithLogger(log logging.Logger) {
	r.log = log
}

func (r *k8sRegistrator) WithClient(c client.Client) {
	r.client = c
}

func (r *k8sRegistrator) Register(ctx context.Context, s *Service) {}

func (r *k8sRegistrator) DeRegister(ctx context.Context, id string) {}

func (r *k8sRegistrator) Query(ctx context.Context, serviceName string, tags []string) ([]*Service, error) {
	return nil, nil
}

func (r *k8sRegistrator) GetEndpointAddress(ctx context.Context, serviceName string, tags []string) (string, error)

func (r *k8sRegistrator) Watch(ctx context.Context, serviceName string, tags []string) chan *ServiceResponse {
	return nil
}

func (r *k8sRegistrator) WatchCh(ctx context.Context, serviceName string, tags []string, ch chan *ServiceResponse) {
}

func (r *k8sRegistrator) StopWatch(serviceName string) {}
