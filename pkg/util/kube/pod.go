package kube

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fatedier/frp/pkg/util/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type LabelPrefix string
type LabelOp string

var labelCounterMap sync.Map

const (
	CustomDomainLabelPrefix LabelPrefix = "custom.domain"
	OrgIdLabelPrefix        LabelPrefix = "org.id"
	TcpPortLabelPrefix      LabelPrefix = "tcp.port"
	AddLabel                LabelOp     = "add"
	RemoveLabel             LabelOp     = "remove"
)

func IsKubernetes() bool {
	_, hostExists := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	_, portExists := os.LookupEnv("KUBERNETES_SERVICE_PORT")
	return hostExists && portExists
}

func AddRemoveLabelPodWithPrefix(ctx context.Context, clientset kubernetes.Interface, op LabelOp, labelPrefix LabelPrefix, label string) error {
	if len(label) == 0 {
		return fmt.Errorf("no custom domain provided for labeling pod")
	}

	podName := os.Getenv("HOSTNAME")
	namespace := os.Getenv("POD_NAMESPACE")
	proxyLabel := fmt.Sprintf("%s/%s", labelPrefix, label)
	proxyLabelPatch := strings.ReplaceAll(proxyLabel, "/", "~1")
	val, _ := labelCounterMap.LoadOrStore(proxyLabel, new(atomic.Int64))
	counter := val.(*atomic.Int64)

	old := counter.Load()

	if op == AddLabel {
		counter.Add(1)
	}

	if op == RemoveLabel && old > 0 {
		counter.Add(-1)
	}

	new := counter.Load()

	var patchData string

	if old == 0 && new > 0 {
		fmt.Printf("adding label %s to pod %s in namespace %s\n", proxyLabel, podName, namespace)
		patchData = fmt.Sprintf(`[{"op": "add", "path": "/metadata/labels/%s", "value": "true"}]`, proxyLabelPatch)
	}

	if old > 0 && new == 0 {
		fmt.Printf("removing label %s from pod %s in namespace %s\n", proxyLabel, podName, namespace)
		patchData = fmt.Sprintf(`[{"op": "remove", "path": "/metadata/labels/%s"}]`, proxyLabelPatch)
	}

	if patchData == "" {
		return nil
	}

	_, err := clientset.CoreV1().Pods(namespace).Patch(
		ctx,
		podName,
		types.JSONPatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("error labeling pod %s in namespace %s: %v", podName, namespace, err)
	}

	return nil
}

func RemoveAllPrefixLabelsFromPod(ctx context.Context, clientset kubernetes.Interface) error {
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		return fmt.Errorf("unable to get pod name via environment variable") // No pod name available, cannot label
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		return fmt.Errorf("unable to get pod namespace via environment variable") // No pod namespace available, cannot label
	}

	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting pod %s in namespace %s: %v", podName, namespace, err)
	}

	updatedLabels := util.Clone(pod.Labels)
	for labelKey := range updatedLabels {
		if strings.HasPrefix(labelKey, string(CustomDomainLabelPrefix)) || strings.HasPrefix(labelKey, string(TcpPortLabelPrefix)) {
			delete(updatedLabels, labelKey)
		}
	}

	if maps.Equal(pod.Labels, updatedLabels) {
		return nil
	}

	_, err = clientset.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating pod %s in namespace %s: %v", podName, namespace, err)
	}

	return nil
}
