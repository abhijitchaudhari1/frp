package kube

import (
	"context"
	"fmt"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	CustomDomainLabelPrefix = "custom.domain"
)

func IsKubernetes() bool {
	_, hostExists := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	_, portExists := os.LookupEnv("KUBERNETES_SERVICE_PORT")
	return hostExists && portExists
}

func LabelPodWithCustomDomain(ctx context.Context, clientset kubernetes.Interface, customDomain string) error {
	if len(customDomain) == 0 {
		return fmt.Errorf("no custom domain provided for labeling pod")
	}

	podName := os.Getenv("HOSTNAME")
	namespace := os.Getenv("POD_NAMESPACE")
	patchData := fmt.Sprintf(`[{"op": "add", "path": "/metadata/labels/%s", "value": "true"}]`, fmt.Sprintf("%s~1%s", CustomDomainLabelPrefix, customDomain))

	_, err := clientset.CoreV1().Pods(namespace).Patch(
		ctx,
		podName,
		types.JSONPatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("error patching pod %s in namespace %s: %v", podName, namespace, err)
	}

	return nil
}

func RemoveAllCustomDomainLabelsFromPod(ctx context.Context, clientset kubernetes.Interface) error {
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		return fmt.Errorf("unable to get pod name via environment variable") // No pod name available, cannot label
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		return fmt.Errorf("unable to get pod namespace via environment variable") // No pod namespace available, cannot label
	}

	// Remove all labels that start with "custom-domain-"
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting pod %s in namespace %s: %v", podName, namespace, err)
	}

	for labelKey := range pod.Labels {
		if strings.HasPrefix(labelKey, CustomDomainLabelPrefix) && pod.Labels[labelKey] == "true" {
			delete(pod.Labels, labelKey)
		}
	}

	_, err = clientset.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating pod %s in namespace %s: %v", podName, namespace, err)
	}

	return nil
}

func RemoveCustomDomainLabelFromPod(ctx context.Context, clientset kubernetes.Interface, customDomain string) error {
	if len(customDomain) == 0 {
		return fmt.Errorf("no custom domain provided for removing pod label	")
	}

	podName := os.Getenv("HOSTNAME")
	namespace := os.Getenv("POD_NAMESPACE")

	// Create JSON patch to remove the label
	patchData := fmt.Sprintf(`[{"op": "remove", "path": "/metadata/labels/%s"}]`, fmt.Sprintf("%s~1%s", CustomDomainLabelPrefix, customDomain))

	// Apply the patch
	_, err := clientset.CoreV1().Pods(namespace).Patch(
		ctx,
		podName,
		types.JSONPatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("error patching pod %s in namespace %s: %v", podName, namespace, err)
	}

	return nil
}
