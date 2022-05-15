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
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/yndd/ndd-runtime/pkg/logging"
	"github.com/yndd/ndd-target-runtime/pkg/resource"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultDCName       = "kind-dc1"
	defaultConsulPort   = "8500"
	defaultWatchTimeout = 1 * time.Minute
)

type consulConfig struct {
	Namespace  string // namespace in which consul is deployed
	Address    string // address of the consul client
	Datacenter string // default kind-dc1
	Username   string
	Password   string
	Token      string
}

// consul implements the Registrator interface
type consul struct {
	//serviceConfig *serviceConfig
	consulConfig *consulConfig
	// kubernetes
	client resource.ClientApplicator
	// consul
	consulClient *api.Client
	// services
	m        sync.Mutex
	services map[string]chan struct{}      // used to stop the registration
	cfn      map[string]context.CancelFunc // used for canceling the watch
	// logging
	log logging.Logger
}

func NewConsulRegistrator(ctx context.Context, namespace, dcName string, opts ...Option) (Registrator, error) {
	// if the namespace is not provided we initialize to consul namespace
	if namespace == "" {
		namespace = "consul"
	}

	r := &consul{
		//serviceConfig: &serviceConfig{},
		consulConfig: &consulConfig{
			Namespace:  namespace,
			Datacenter: dcName,
		},
		services: map[string]chan struct{}{},
		cfn:      map[string]context.CancelFunc{},
	}

	for _, opt := range opts {
		opt(r)
	}

	r.init(ctx)


	if err := r.createClient(); err != nil {
		return nil, err
	}
	
	return r, nil
}

func (r *consul) createClient() error {
	log := r.log.WithValues("Consul", *r.consulConfig)
	log.Debug("consul create client...")

	clientConfig := &api.Config{
		Address:    r.consulConfig.Address,
		Scheme:     "http",
		Datacenter: r.consulConfig.Datacenter,
		Token:      r.consulConfig.Token,
	}
	if r.consulConfig.Username != "" && r.consulConfig.Password != "" {
		clientConfig.HttpAuth = &api.HttpBasicAuth{
			Username: r.consulConfig.Username,
			Password: r.consulConfig.Password,
		}
	}

	var err error
	if r.consulClient, err = api.NewClient(clientConfig); err != nil {
		log.Debug("failed to connect to consul", "error", err)
		return err
	}
	self, err := r.consulClient.Agent().Self()
	if err != nil {
		log.Debug("failed to connect to consul", "error", err)
		time.Sleep(1 * time.Second)
		return err
	}
	if cfg, ok := self["Config"]; ok {
		b, _ := json.Marshal(cfg)
		log.Debug("consul agent config:", "agent config", string(b))
	}
	return nil
}

func (r *consul) WithLogger(l logging.Logger) {
	r.log = l
}

func (r *consul) WithClient(rc resource.ClientApplicator) {
	r.client = rc
}

func (r *consul) init(ctx context.Context) {
	log := r.log.WithValues("Consul", *r.consulConfig)
	log.Debug("consul init, trying to find daemonset...")

CONSULDAEMONSETPOD:
	// get all the pods in the consul namespace
	opts := []client.ListOption{
		client.InNamespace(r.consulConfig.Namespace),
	}
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods, opts...); err != nil {
		log.Debug("cannot list pods on k8s api", "err", err)
		time.Sleep(2 * time.Second)
		goto CONSULDAEMONSETPOD
	}

	found := false
	for _, pod := range pods.Items {
		log.Debug("consul pod",
			"consul pod kind", pod.OwnerReferences[0].Kind,
			"consul pod phase", pod.Status.Phase,
			"consul pod node name", pod.Spec.NodeName,
			"consul pod node ip", pod.Status.HostIP,
			"consul pod ip", pod.Status.PodIP,
			"pod node naame", os.Getenv("NODE_NAME"),
			"pod node ip", os.Getenv("Node_IP"),
		)
		if len(pod.OwnerReferences) == 0 {
			// pod has no owner
			continue
		}
		switch pod.OwnerReferences[0].Kind {
		case "DaemonSet":
			if pod.Status.Phase == "Running" &&
				pod.Status.PodIP != "" &&
				pod.Spec.NodeName == os.Getenv("NODE_NAME") {
				//pod.Status.HostIP == os.Getenv("Node_IP") {
				found = true
				r.consulConfig.Address = strings.Join([]string{pod.Status.PodIP, defaultConsulPort}, ":") // TODO
				r.consulConfig.Datacenter = defaultDCName
			}
		default:
			// could be ReplicaSet, StatefulSet, etc, but not releant here
			continue
		}
	}
	if !found {
		// daemonset not found
		log.Debug("consul daemonset not found")
		time.Sleep(defaultTimout)
		goto CONSULDAEMONSETPOD
	}
	log.Debug("consul daemonset found", "address", r.consulConfig.Address, "datacenter", r.consulConfig.Datacenter)
}

func (r *consul) Register(ctx context.Context, s *Service) {
	r.m.Lock()
	defer r.m.Unlock()
	r.services[s.ID] = make(chan struct{})
	go r.registerService(ctx, s, r.services[s.ID])
}

func (r *consul) DeRegister(ctx context.Context, id string) {
	log := r.log.WithValues("Consul", r.consulConfig)
	log.Debug("Deregister...")

	r.m.Lock()
	defer r.m.Unlock()
	if stopCh, ok := r.services[id]; ok {
		close(stopCh)
		delete(r.services, id)
	}

}

func (r *consul) registerService(ctx context.Context, s *Service, stopCh chan struct{}) {
	log := r.log.WithValues("Consul", r.consulConfig)
	log.Debug("Register...")

	/*
	clientConfig := &api.Config{
		Address:    r.consulConfig.Address,
		Scheme:     "http",
		Datacenter: r.consulConfig.Datacenter,
		Token:      r.consulConfig.Token,
	}
	if r.consulConfig.Username != "" && r.consulConfig.Password != "" {
		clientConfig.HttpAuth = &api.HttpBasicAuth{
			Username: r.consulConfig.Username,
			Password: r.consulConfig.Password,
		}
	}
	*/
INITCONSUL:
/*
	var err error
	if r.consulClient, err = api.NewClient(clientConfig); err != nil {
		log.Debug("failed to connect to consul", "error", err)
		time.Sleep(1 * time.Second)
		goto INITCONSUL
	}
	self, err := r.consulClient.Agent().Self()
	if err != nil {
		log.Debug("failed to connect to consul", "error", err)
		time.Sleep(1 * time.Second)
		goto INITCONSUL
	}
	if cfg, ok := self["Config"]; ok {
		b, _ := json.Marshal(cfg)
		log.Debug("consul agent config:", "agent config", string(b))
	}
	*/
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var service *api.AgentServiceRegistration
	ttlCheckID := ""
	if s.HealthKind != HealthKindNone {
		service = &api.AgentServiceRegistration{
			ID:      s.ID,
			Name:    s.Name,
			Address: s.Address,
			Port:    s.Port,
			//Tags:    p.Cfg.ServiceRegistration.Tags,
			Checks: api.AgentServiceChecks{
				{
					TTL:                            defaultRegistrationCheckInterval.String(),
					DeregisterCriticalServiceAfter: (defaultMaxServiceFail * defaultRegistrationCheckInterval).String(),
				},
			},
		}

		ttlCheckID = strings.Join([]string{"service", s.ID, "1"}, ":")

		service.Checks = append(service.Checks, &api.AgentServiceCheck{
			GRPC:                           s.Address + ":" + strconv.Itoa(s.Port),
			GRPCUseTLS:                     true,
			Interval:                       defaultRegistrationCheckInterval.String(),
			TLSSkipVerify:                  true,
			DeregisterCriticalServiceAfter: (defaultMaxServiceFail * defaultRegistrationCheckInterval).String(),
		})
		//ttlCheckID = ttlCheckID + ":1"
	} else {
		service = &api.AgentServiceRegistration{
			ID:      s.ID,
			Name:    s.Name,
			Address: s.Address,
			Port:    s.Port,
			//Tags:    p.Cfg.ServiceRegistration.Tags,
		}
	}

	b, _ := json.Marshal(service)
	log.Debug("consul register service", "service", string(b))

	if err := r.consulClient.Agent().ServiceRegister(service); err != nil {
		log.Debug("consul register service failed", "error", err)
		time.Sleep(1 * time.Second)
		goto INITCONSUL
	}

	ticker := &time.Ticker{}
	if s.HealthKind != HealthKindNone {
		if err := r.consulClient.Agent().UpdateTTL(ttlCheckID, "", api.HealthPassing); err != nil {
			log.Debug("consul failed to pass TTL check", "error", err)
		}
		ticker = time.NewTicker(defaultRegistrationCheckInterval / 2)
	}

	for {
		select {
		case <-ticker.C:
			if err := r.consulClient.Agent().UpdateTTL(ttlCheckID, "", api.HealthPassing); err != nil {
				log.Debug("consul failed to pass TTL check", "error", err)
			}
		case <-ctx.Done():
			r.consulClient.Agent().UpdateTTL(ttlCheckID, ctx.Err().Error(), api.HealthCritical)
			ticker.Stop()
			goto INITCONSUL
		case <-stopCh:
			r.log.Debug("deregister...")
			r.consulClient.Agent().ServiceDeregister(s.ID)
			ticker.Stop()
			return
		}
	}
}

// servicename : controllername + worker
//
func (r *consul) Query(ctx context.Context, serviceName string, tags []string) ([]*Service, error) {
	se, _, err := r.consulClient.Health().ServiceMultipleTags(serviceName, tags, true, &api.QueryOptions{})
	if err != nil {
		return nil, err
	}
	servcies := []*Service{}
	for _, srv := range se {
		addr := srv.Service.Address
		if addr == "" {
			addr = srv.Node.Address
		}
		servcies = append(servcies, &Service{
			ID: srv.Service.ID,
			//Address: net.JoinHostPort(addr, strconv.Itoa(srv.Service.Port)),
			Address: addr,
			Port:    srv.Service.Port,
			Tags:    srv.Service.Tags,
		})
	}
	return servcies, nil
}

func (r *consul) Watch(ctx context.Context, serviceName string, tags []string) chan *ServiceResponse {
	ch := make(chan *ServiceResponse)
	go r.WatchCh(ctx, serviceName, tags, ch)
	return ch
}

func (r *consul) WatchCh(ctx context.Context, serviceName string, tags []string, ch chan *ServiceResponse) {
	log := r.log.WithValues("serviceName", serviceName)
	watchTimeout := defaultWatchTimeout
	cfn, ok := r.cfn[serviceName]
	if ok {
		cfn()
	}
	ctx, r.cfn[serviceName] = context.WithCancel(ctx)

	var index uint64
	qOpts := &api.QueryOptions{
		WaitIndex: index,
		WaitTime:  watchTimeout,
	}
	var err error
	// long blocking watch
	for {
		select {
		case <-ctx.Done():
			ch <- &ServiceResponse{
				ServiceName: serviceName,
				Err:         ctx.Err(),
			}
			return
		default:
			log.Debug("(re)starting watch", "index", qOpts.WaitIndex)

			index, err = r.watch(qOpts.WithContext(ctx), serviceName, tags, ch)
			if err != nil {
				log.Debug("watch error", "error", err)
			}
			if index == 1 {
				// this is considered a bug in consul
				qOpts.WaitIndex = 1
				time.Sleep(2 * time.Second)
				continue
			}
			// expected sunce waitindex is bigger than the previous value
			if index > qOpts.WaitIndex {
				qOpts.WaitIndex = index
			}
			// reset WaitIndex if the returned index decreases because a waitIndex should always increase
			// https://www.consul.io/api-docs/features/blocking#implementation-details
			if index < qOpts.WaitIndex {
				// restart to get back in sync
				qOpts.WaitIndex = 0
			}
		}
	}
}

func (r *consul) watch(qOpts *api.QueryOptions, serviceName string, tags []string, sChan chan<- *ServiceResponse) (uint64, error) {
	log := r.log.WithValues("serviceName", serviceName)
	se, meta, err := r.consulClient.Health().ServiceMultipleTags(serviceName, tags, true, qOpts)
	if err != nil {
		return 0, err
	}
	if meta == nil {
		meta = new(api.QueryMeta)
	}
	if meta.LastIndex == qOpts.WaitIndex {
		log.Debug("service did not change", "index", meta.LastIndex)
		return meta.LastIndex, nil
	}
	if err != nil {
		return meta.LastIndex, err
	}
	if len(se) == 0 {
		return 1, nil
	}
	newSrvs := make([]*Service, 0)
	for _, srv := range se {
		addr := srv.Service.Address
		if addr == "" {
			addr = srv.Node.Address
		}
		newSrvs = append(newSrvs, &Service{
			ID: srv.Service.ID,
			//Address: net.JoinHostPort(addr, strconv.Itoa(srv.Service.Port)),
			Address: addr,
			Port:    srv.Service.Port,
			Tags:    srv.Service.Tags,
		})
	}
	sChan <- &ServiceResponse{
		ServiceName:      serviceName,
		ServiceInstances: newSrvs,
		Err:              nil,
	}
	return meta.LastIndex, nil
}

func (r *consul) StopWatch(serviceName string) {
	r.m.Lock()
	defer r.m.Unlock()
	if serviceName == "" {
		for _, cfn := range r.cfn {
			cfn()
		}
		return
	}
	if cfn, ok := r.cfn[serviceName]; ok {
		cfn()
	}
}
