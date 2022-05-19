package registrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yndd/ndd-runtime/pkg/logging"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
)

const (
	serviceNameLabelKey    = "_service_name"
	serviceIDLabelKey      = "_service_id"
	serviceAddressLabelKey = "_service_address"
	servicePortLabelKey    = "_service_port"
)

func newK8sRegistrator(ctx context.Context, clientSet *kubernetes.Clientset, namespace string, opts ...Option) (Registrator, error) {
	if namespace == "" {
		namespace = "ndd-system"
	}
	r := &k8sRegistrator{
		namespace: namespace,
		clientset: clientSet,
		watches:   make(map[string]watch.Interface),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// k8sRegistrator implements the Registrator interface
type k8sRegistrator struct {
	clientset *kubernetes.Clientset
	log       logging.Logger
	namespace string
	//
	m              *sync.RWMutex
	acquiredLeases map[string]*acquiredLease
	// k8s watch interface
	watches map[string]watch.Interface
}

type acquiredLease struct {
	lease    *coordinationv1.Lease
	doneChan chan struct{}
}

func (r *k8sRegistrator) WithLogger(log logging.Logger) {
	r.log = log
}

func (r *k8sRegistrator) Register(ctx context.Context, s *Service) {
	doneChan := make(chan struct{})
	var err error
	l := r.serviceToLease(s)
	// create/hold loop
	for {
		select {
		case <-ctx.Done():
			r.log.Info("register context done", "error", ctx.Err())
			return
		case <-doneChan:
			r.log.Info("lease done", "lease", l.Name)
			return
		default:
			now := metav1.NowMicro()
			var ol *coordinationv1.Lease
			// get or create
			ol, err = r.clientset.CoordinationV1().Leases(r.namespace).Get(ctx, l.Name, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					r.log.Info("failed to get Leases", "error", err)
					time.Sleep(defaultWaitTime)
					continue
				}
				// create lease
				r.log.Info("lease not found, creating it", "lease", l.Name)
				l.Spec.AcquireTime = &now
				l.Spec.RenewTime = &now
				ol, err = r.clientset.CoordinationV1().Leases(r.namespace).Create(ctx, l, metav1.CreateOptions{})
				if err != nil {
					r.log.Info("failed to create Lease", "error", err)
					time.Sleep(defaultWaitTime)
					continue
				}
				r.m.Lock()
				r.acquiredLeases[l.Name] = &acquiredLease{
					lease:    ol,
					doneChan: doneChan,
				}
				r.m.Unlock()
				time.Sleep(defaultRegistrationCheckInterval / 2)
				continue
			}
			// obtained, compare
			if ol != nil && ol.Spec.HolderIdentity != nil && *ol.Spec.HolderIdentity != "" {
				r.log.Info("%q held by other instance: %v", ol.Name, *ol.Spec.HolderIdentity != s.Name)
				r.log.Info("%q lease has renewTime: %v", ol.Name, ol.Spec.RenewTime != nil)
				if *ol.Spec.HolderIdentity != s.Name && ol.Spec.RenewTime != nil {
					expectedRenewTime := ol.Spec.RenewTime.Add(time.Duration(*ol.Spec.LeaseDurationSeconds) * time.Second)
					r.log.Info("%q existing lease renew time %v", ol.Name, ol.Spec.RenewTime)
					r.log.Info("%q expected lease renew time %v", ol.Name, expectedRenewTime)
					r.log.Info("%q renew time passed: %v", ol.Name, expectedRenewTime.Before(now.Time))
					if !expectedRenewTime.Before(now.Time) {
						r.log.Info("%q is currently held by %s", ol.Name, *ol.Spec.HolderIdentity)
						time.Sleep(defaultRegistrationCheckInterval)
						continue
					}
				}
			}
			r.log.Info("taking over lease %q", l.Name)
			// update the lease
			now = metav1.NowMicro()
			l.Spec.AcquireTime = &now
			l.Spec.RenewTime = &now
			// set resource version to the latest value known
			l.SetResourceVersion(ol.GetResourceVersion())
			r.log.Info("%q updating with %+v", l.Name, l)
			ol, err = r.clientset.CoordinationV1().Leases(r.namespace).Update(ctx, l, metav1.UpdateOptions{})
			if err != nil {
				r.log.Info("failed to update Lease", "error", err)
				time.Sleep(defaultWaitTime)
				continue
			}
			r.m.Lock()
			if lc, ok := r.acquiredLeases[l.Name]; ok {
				lc.lease = ol
			} else {
				r.acquiredLeases[l.Name] = &acquiredLease{lease: ol, doneChan: doneChan}
			}
			r.m.Unlock()
			time.Sleep(defaultRegistrationCheckInterval / 2)
			continue
		}
	}
}

func (r *k8sRegistrator) DeRegister(ctx context.Context, id string) {
	r.m.Lock()
	defer r.m.Unlock()
	if l, ok := r.acquiredLeases[id]; ok {
		close(l.doneChan)
		delete(r.acquiredLeases, id)
		err := r.clientset.CoordinationV1().Leases(r.namespace).Delete(ctx, l.lease.Name, metav1.DeleteOptions{})
		if err != nil {
			r.log.Info("failed to delete lease", "lease", id, "error", err)
		}
	}
}

func (r *k8sRegistrator) Query(ctx context.Context, serviceName string, tags []string) ([]*Service, error) {
	validSelector, err := buildSelector(serviceName, tags)
	if err != nil {
		return nil, err
	}
	// TODO: use limit/continue
	leaseList, err := r.clientset.CoordinationV1().Leases(r.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: validSelector,
	})
	if err != nil {
		return nil, err
	}
	ss := make([]*Service, 0, len(leaseList.Items))
	for _, l := range leaseList.Items {
		ss = append(ss, leaseToService(l))
	}
	return ss, nil
}

func (r *k8sRegistrator) GetEndpointAddress(ctx context.Context, serviceName string, tags []string) (string, error) {
	ss, err := r.Query(ctx, serviceName, tags)
	if err != nil {
		return "", err
	}
	if len(ss) == 0 {
		return "", nil
	}
	return fmt.Sprintf("%s:%d", ss[0].Address, ss[0].Port), nil
}

func (r *k8sRegistrator) Watch(ctx context.Context, serviceName string, tags []string, opts WatchOptions) chan *ServiceResponse {
	ch := make(chan *ServiceResponse)
	go r.WatchCh(ctx, serviceName, tags, opts, ch)
	return ch
}

func (r *k8sRegistrator) WatchCh(ctx context.Context, serviceName string, tags []string, opts WatchOptions, ch chan *ServiceResponse) {
	log := r.log.WithValues("serviceName", serviceName)
	wi, ok := r.watches[serviceName]
	if ok && wi != nil {
		wi.Stop()
	}
	validSelector, _ := buildSelector(serviceName, tags)
WATCH:
	wi, err := r.clientset.CoordinationV1().Leases(r.namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: validSelector,
		Watch:         true,
	})
	if err != nil {
		log.Info("failed to create watch", "error", err)
		time.Sleep(defaultWaitTime)
		goto WATCH
	}
	for {
		select {
		case _, ok := <-wi.ResultChan():
			if !ok {
				return
			}
			sr := &ServiceResponse{
				ServiceName: serviceName,
			}
			if opts.RetriveServices {
				sr.ServiceInstances, sr.Err = r.Query(ctx, serviceName, tags)
			}
			ch <- sr
		}
	}
}

func (r *k8sRegistrator) StopWatch(serviceName string) {
	r.m.Lock()
	defer r.m.Unlock()
	wi, ok := r.watches[serviceName]
	if ok && wi != nil {
		wi.Stop()
	}
	delete(r.watches, serviceName)
}

//
func tagsToMap(tags []string) map[string]string {
	labels := make(map[string]string)
	for _, t := range tags {
		if t == "" {
			continue
		}
		i := strings.Index(t, "=")
		switch {
		case i < 0:
			labels[t] = ""
		case i >= 0:
			labels[t[:i]] = labels[t[i+1:]]
		}
	}
	return labels
}

func (r *k8sRegistrator) serviceToLease(s *Service) *coordinationv1.Lease {
	labels := map[string]string{
		serviceNameLabelKey:    s.Name,
		serviceIDLabelKey:      s.ID,
		serviceAddressLabelKey: s.Address,
		servicePortLabelKey:    strconv.Itoa(s.Port),
	}
	for k, v := range tagsToMap(s.Tags) {
		labels[k] = v
	}
	leaseName := fmt.Sprintf("%s-%s", s.Name, s.ID)
	leaseName = strings.ReplaceAll(leaseName, "/", "-")
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: r.namespace,
			Labels:    labels,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       pointer.String(s.Name),
			LeaseDurationSeconds: pointer.Int32(int32(defaultRegistrationCheckInterval / time.Second)),
		},
	}
}

func leaseToService(l coordinationv1.Lease) *Service {
	s := &Service{
		Tags: make([]string, 0, len(l.GetLabels())),
	}
	for k, v := range l.GetLabels() {
		switch k {
		case serviceNameLabelKey:
			s.Name = v
		case serviceIDLabelKey:
			s.ID = v
		case serviceAddressLabelKey:
			s.Address = v
		case servicePortLabelKey:
			s.Port, _ = strconv.Atoi(v)
		default:
			s.Tags = append(s.Tags, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return s
}

func buildSelector(serviceName string, tags []string) (string, error) {
	labelsSet := map[string]string{
		serviceAddressLabelKey: serviceName,
	}
	for k, v := range tagsToMap(tags) {
		labelsSet[k] = v
	}
	validatedLabels, err := labels.ValidatedSelectorFromSet(labelsSet)
	if err != nil {
		return "", err
	}
	return validatedLabels.String(), nil
}
