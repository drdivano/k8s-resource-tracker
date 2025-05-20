package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	KindDeployment  = "Deployment"
	KindStatefulSet = "StatefulSet"
	KindPod         = "Pod"

	MaxPodRestartsBeforeError = 3
	LogTailLines              = 100
	DefaultTickerInterval     = 2 * time.Second
)

// ResourceIdentifier defines a Kubernetes resource to watch.
type ResourceIdentifier struct {
	Kind      string
	Namespace string
	Name      string
}

func (r ResourceIdentifier) String() string {
	return fmt.Sprintf("%s/%s/%s", r.Kind, r.Namespace, r.Name)
}

// watchTargetInternal holds the state for a resource being watched.
type watchTargetInternal struct {
	identifier ResourceIdentifier
	uid        metav1.UID // UID of the resource, useful for owner references

	isSatisfied bool
	err         error

	// For Deployments/StatefulSets
	selector labels.Selector
}

// ResourceWatcher watches Kubernetes resources.
type ResourceWatcher struct {
	clientset       kubernetes.Interface
	informerFactory informers.SharedInformerFactory

	podLister         corev1listers.PodLister
	deploymentLister  appsv1listers.DeploymentLister
	statefulSetLister appsv1listers.StatefulSetLister

	targets         map[string]*watchTargetInternal // key: ResourceIdentifier.String()
	knownPodLogKeys map[string]bool                 // key: "namespace/podName" -> true if logs already fetched for error
	mux             sync.Mutex
}

// NewResourceWatcher creates a new ResourceWatcher.
func NewResourceWatcher(clientset kubernetes.Interface) *ResourceWatcher {
	// Use a resync period of 0 to rely on API server watch events.
	factory := informers.NewSharedInformerFactory(clientset, 0)
	return &ResourceWatcher{
		clientset:         clientset,
		informerFactory:   factory,
		podLister:         factory.Core().V1().Pods().Lister(),
		deploymentLister:  factory.Apps().V1().Deployments().Lister(),
		statefulSetLister: factory.Apps().V1().StatefulSets().Lister(),
		targets:           make(map[string]*watchTargetInternal),
		knownPodLogKeys:   make(map[string]bool),
	}
}

func (rw *ResourceWatcher) getPodLogs(namespace, name string) string {
	podLogOpts := corev1.PodLogOptions{}
	tailLines := int64(LogTailLines)
	podLogOpts.TailLines = &tailLines

	req := rw.clientset.CoreV1().Pods(namespace).GetLogs(name, &podLogOpts)
	podLogs, err := req.Stream(context.Background()) // Use a background context for log streaming
	if err != nil {
		return fmt.Sprintf("[failed to get logs: %v]", err)
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return fmt.Sprintf("[failed to read log stream: %v]", err)
	}
	return buf.String()
}

func (rw *ResourceWatcher) handlePodEvent(pod *corev1.Pod) {
	rw.mux.Lock()
	defer rw.mux.Unlock()

	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	var totalRestarts int32
	for _, cs := range pod.Status.ContainerStatuses {
		totalRestarts += cs.RestartCount
	}

	// Check if this pod is directly tracked
	directTargetKey := ResourceIdentifier{Kind: KindPod, Namespace: pod.Namespace, Name: pod.Name}.String()
	if target, exists := rw.targets[directTargetKey]; exists && target.err == nil && !target.isSatisfied {
		if totalRestarts > MaxPodRestartsBeforeError {
			if !rw.knownPodLogKeys[podKey] {
				logs := rw.getPodLogs(pod.Namespace, pod.Name)
				target.err = fmt.Errorf("tracked pod %s restarted %d times (max %d). Logs:\n%s",
					podKey, totalRestarts, MaxPodRestartsBeforeError, logs)
				rw.knownPodLogKeys[podKey] = true
			} else {
				target.err = fmt.Errorf("tracked pod %s restarted %d times (max %d)",
					podKey, totalRestarts, MaxPodRestartsBeforeError)
			}
		}
	}

	// Check if this pod is owned by a tracked Deployment or StatefulSet
	for _, ownerRef := range pod.GetOwnerReferences() {
		if ownerRef.Controller == nil || !*ownerRef.Controller {
			continue
		}
		ownerKind := ownerRef.Kind
		if ownerKind != KindDeployment && ownerKind != KindStatefulSet {
			continue
		}

		ownerTargetKey := ResourceIdentifier{Kind: ownerKind, Namespace: pod.Namespace, Name: ownerRef.Name}.String()
		if target, exists := rw.targets[ownerTargetKey]; exists && target.err == nil && !target.isSatisfied {
			// Check if the owner UID matches (in case of same name, different resource)
			if target.uid != "" && target.uid != ownerRef.UID {
				continue
			}
			if totalRestarts > MaxPodRestartsBeforeError {
				if !rw.knownPodLogKeys[podKey] {
					logs := rw.getPodLogs(pod.Namespace, pod.Name)
					target.err = fmt.Errorf("pod %s (owned by %s) restarted %d times (max %d). Logs:\n%s",
						podKey, ownerTargetKey, totalRestarts, MaxPodRestartsBeforeError, logs)
					rw.knownPodLogKeys[podKey] = true
				} else {
					target.err = fmt.Errorf("pod %s (owned by %s) restarted %d times (max %d)",
						podKey, ownerTargetKey, totalRestarts, MaxPodRestartsBeforeError)
				}
			} else if pod.Status.Phase == corev1.PodFailed { // Pod failed but not due to restarts
				if !rw.knownPodLogKeys[podKey] {
					logs := rw.getPodLogs(pod.Namespace, pod.Name)
					target.err = fmt.Errorf("pod %s (owned by %s) failed. Logs:\n%s",
						podKey, ownerTargetKey, logs)
					rw.knownPodLogKeys[podKey] = true
				} else {
					target.err = fmt.Errorf("pod %s (owned by %s) failed", podKey, ownerTargetKey)
				}
			}
		}
	}
}

// Watch starts watching the specified resources until they are ready or timeout occurs.
func (rw *ResourceWatcher) Watch(ctx context.Context, resources []ResourceIdentifier, timeout time.Duration) error {
	if len(resources) == 0 {
		return fmt.Errorf("no resources specified to watch")
	}

	watchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Initialize targets
	rw.mux.Lock()
	for _, r := range resources {
		rw.targets[r.String()] = &watchTargetInternal{identifier: r}
	}
	rw.mux.Unlock()

	// Setup pod informer handlers
	rw.informerFactory.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				rw.handlePodEvent(pod)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				rw.handlePodEvent(pod)
			}
		},
		// DeleteFunc: Optional: handle pod deletions if necessary for specific logic
	})

	// Start informers
	stopCh := make(chan struct{})
	defer close(stopCh) // Signal informers to stop when Watch returns
	rw.informerFactory.Start(stopCh)

	// Wait for caches to sync
	log.Println("Waiting for informer caches to sync...")
	if !cache.WaitForCacheSync(stopCh,
		rw.informerFactory.Core().V1().Pods().Informer().HasSynced,
		rw.informerFactory.Apps().V1().Deployments().Informer().HasSynced,
		rw.informerFactory.Apps().V1().StatefulSets().Informer().HasSynced) {
		return fmt.Errorf("failed to sync informer caches")
	}
	log.Println("Informer caches synced.")

	// Populate initial UIDs and selectors for Deployments/StatefulSets
	if err := rw.populateInitialTargetDetails(); err != nil {
		return fmt.Errorf("failed to populate initial target details: %w", err)
	}

	ticker := time.NewTicker(DefaultTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-watchCtx.Done():
			// Timeout occurred
			rw.mux.Lock()
			var pendingResources []string
			var errorMessages []string
			allSatisfied := true
			for _, target := range rw.targets {
				if target.err != nil {
					errorMessages = append(errorMessages, fmt.Sprintf("%s: %s", target.identifier.String(), target.err.Error()))
				}
				if !target.isSatisfied && target.err == nil {
					allSatisfied = false
					pendingResources = append(pendingResources, target.identifier.String())
				}
			}
			rw.mux.Unlock()

			if len(errorMessages) > 0 {
				return fmt.Errorf("watch encountered errors: %s", strings.Join(errorMessages, "; "))
			}
			if !allSatisfied {
				return fmt.Errorf("watch timed out after %s. Pending resources: %s", timeout, strings.Join(pendingResources, ", "))
			}
			return nil // All satisfied just before timeout

		case <-ticker.C:
			if done, err := rw.checkResourceStatuses(); err != nil {
				return err // An error occurred (e.g., pod crash limit)
			} else if done {
				log.Println("All specified resources have reached their desired state.")
				return nil // All resources are ready
			}
			// Continue waiting
		}
	}
}

func (rw *ResourceWatcher) populateInitialTargetDetails() error {
	rw.mux.Lock()
	defer rw.mux.Unlock()

	for key, target := range rw.targets {
		switch target.identifier.Kind {
		case KindDeployment:
			dep, err := rw.deploymentLister.Deployments(target.identifier.Namespace).Get(target.identifier.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					target.err = fmt.Errorf("Deployment %s not found", key)
					continue
				}
				return fmt.Errorf("failed to get Deployment %s: %w", key, err)
			}
			target.uid = dep.UID
			sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
			if err != nil {
				target.err = fmt.Errorf("failed to parse selector for Deployment %s: %w", key, err)
				continue
			}
			target.selector = sel
		case KindStatefulSet:
			ss, err := rw.statefulSetLister.StatefulSets(target.identifier.Namespace).Get(target.identifier.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					target.err = fmt.Errorf("StatefulSet %s not found", key)
					continue
				}
				return fmt.Errorf("failed to get StatefulSet %s: %w", key, err)
			}
			target.uid = ss.UID
			sel, err := metav1.LabelSelectorAsSelector(ss.Spec.Selector)
			if err != nil {
				target.err = fmt.Errorf("failed to parse selector for StatefulSet %s: %w", key, err)
				continue
			}
			target.selector = sel
		case KindPod:
			pod, err := rw.podLister.Pods(target.identifier.Namespace).Get(target.identifier.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					target.err = fmt.Errorf("Pod %s not found", key)
					continue
				}
				return fmt.Errorf("failed to get Pod %s: %w", key, err)
			}
			target.uid = pod.UID
		}
	}
	return nil
}

func (rw *ResourceWatcher) checkResourceStatuses() (allDone bool, Derror error) {
	rw.mux.Lock()
	defer rw.mux.Unlock()

	allSatisfied := true
	for key, target := range rw.targets {
		if target.isSatisfied {
			continue // Already satisfied
		}
		if target.err != nil {
			return true, target.err // Propagate error
		}

		allSatisfied = false // At least one target is not yet satisfied and has no error

		switch target.identifier.Kind {
		case KindDeployment:
			dep, err := rw.deploymentLister.Deployments(target.identifier.Namespace).Get(target.identifier.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					target.err = fmt.Errorf("Deployment %s disappeared", key)
					return true, target.err
				}
				log.Printf("Error getting Deployment %s from lister: %v. Will retry.", key, err)
				continue // Temporary error, retry on next tick
			}
			// Update selector if it changed (rare, but possible)
			if dep.Spec.Selector != nil {
				sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
				if err == nil && (target.selector == nil || target.selector.String() != sel.String()) {
					log.Printf("Selector changed for Deployment %s", key)
					target.selector = sel
				}
			}

			// A deployment is considered available if all its replicas are updated and available.
			// Status.ObservedGeneration should match Metadata.Generation.
			// Status.UpdatedReplicas == Spec.Replicas
			// Status.ReadyReplicas == Spec.Replicas
			// Status.AvailableReplicas == Spec.Replicas
			desiredReplicas := int32(1) // Default if not specified
			if dep.Spec.Replicas != nil {
				desiredReplicas = *dep.Spec.Replicas
			}

			if dep.Status.ObservedGeneration >= dep.Generation &&
				dep.Status.UpdatedReplicas == desiredReplicas &&
				dep.Status.ReadyReplicas == desiredReplicas &&
				dep.Status.AvailableReplicas == desiredReplicas {
				// Additionally verify that no pods belonging to this deployment are in a permanently failed state
				// (crash loop backoff is handled by handlePodEvent setting target.err)
				if target.selector != nil {
					pods, err := rw.podLister.Pods(dep.Namespace).List(target.selector)
					if err != nil {
						log.Printf("Error listing pods for Deployment %s: %v. Will retry.", key, err)
						continue
					}
					hasFailedOwnedPod := false
					for _, pod := range pods {
						isOwned := false
						for _, ownerRef := range pod.GetOwnerReferences() {
							if ownerRef.UID == dep.UID {
								isOwned = true
								break
							}
						}
						if isOwned && pod.Status.Phase == corev1.PodFailed {
							// A pod owned by this deployment has failed (not due to restarts, which target.err would cover)
							// This could be due to image pull error, node issues etc.
							podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
							if !rw.knownPodLogKeys[podKey] {
								logs := rw.getPodLogs(pod.Namespace, pod.Name)
								target.err = fmt.Errorf("owned pod %s for Deployment %s failed. Logs:\n%s", podKey, key, logs)
								rw.knownPodLogKeys[podKey] = true
							} else {
								target.err = fmt.Errorf("owned pod %s for Deployment %s failed", podKey, key)
							}
							return true, target.err // Propagate this failure
						}
					}
				}
				log.Printf("Deployment %s is ready.", key)
				target.isSatisfied = true
			}

		case KindStatefulSet:
			ss, err := rw.statefulSetLister.StatefulSets(target.identifier.Namespace).Get(target.identifier.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					target.err = fmt.Errorf("StatefulSet %s disappeared", key)
					return true, target.err
				}
				log.Printf("Error getting StatefulSet %s from lister: %v. Will retry.", key, err)
				continue
			}
			// Update selector if it changed
			if ss.Spec.Selector != nil {
				sel, err := metav1.LabelSelectorAsSelector(ss.Spec.Selector)
				if err == nil && (target.selector == nil || target.selector.String() != sel.String()) {
					log.Printf("Selector changed for StatefulSet %s", key)
					target.selector = sel
				}
			}

			desiredReplicas := int32(1)
			if ss.Spec.Replicas != nil {
				desiredReplicas = *ss.Spec.Replicas
			}

			// For StatefulSets, all replicas must be running and ready.
			// Status.ObservedGeneration should match Metadata.Generation.
			// Status.ReadyReplicas == Spec.Replicas
			// Status.CurrentReplicas == Spec.Replicas (or UpdatedReplicas for rolling updates)
			if ss.Status.ObservedGeneration >= ss.Generation &&
				ss.Status.ReadyReplicas == desiredReplicas &&
				ss.Status.CurrentReplicas == desiredReplicas && // Ensure all pods are on current revision
				ss.Status.Replicas == desiredReplicas { // Ensure correct number of total pods exist

				if target.selector != nil {
					pods, err := rw.podLister.Pods(ss.Namespace).List(target.selector)
					if err != nil {
						log.Printf("Error listing pods for StatefulSet %s: %v. Will retry.", key, err)
						continue
					}
					for _, pod := range pods {
						isOwned := false
						for _, ownerRef := range pod.GetOwnerReferences() {
							if ownerRef.UID == ss.UID {
								isOwned = true
								break
							}
						}
						if isOwned && pod.Status.Phase == corev1.PodFailed {
							podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
							if !rw.knownPodLogKeys[podKey] {
								logs := rw.getPodLogs(pod.Namespace, pod.Name)
								target.err = fmt.Errorf("owned pod %s for StatefulSet %s failed. Logs:\n%s", podKey, key, logs)
								rw.knownPodLogKeys[podKey] = true
							} else {
								target.err = fmt.Errorf("owned pod %s for StatefulSet %s failed", podKey, key)
							}
							return true, target.err
						}
					}
				}
				log.Printf("StatefulSet %s is ready.", key)
				target.isSatisfied = true
			}

		case KindPod:
			pod, err := rw.podLister.Pods(target.identifier.Namespace).Get(target.identifier.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					target.err = fmt.Errorf("Pod %s disappeared", key)
					return true, target.err
				}
				log.Printf("Error getting Pod %s from lister: %v. Will retry.", key, err)
				continue
			}

			if pod.Status.Phase == corev1.PodSucceeded {
				log.Printf("Pod %s completed successfully.", key)
				target.isSatisfied = true
			} else if pod.Status.Phase == corev1.PodFailed {
				// This specific pod failed. If not already caught by restart handler.
				podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
				if !rw.knownPodLogKeys[podKey] {
					logs := rw.getPodLogs(pod.Namespace, pod.Name)
					target.err = fmt.Errorf("tracked pod %s failed. Logs:\n%s", key, logs)
					rw.knownPodLogKeys[podKey] = true
				} else {
					target.err = fmt.Errorf("tracked pod %s failed", key)
				}
				return true, target.err // Propagate error
			}
		}
	}
	return allSatisfied, nil
}
