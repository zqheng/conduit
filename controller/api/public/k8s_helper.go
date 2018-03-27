package public

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var resourceTypes = []string{
	"deployments",
	"replicaSets",
	"services",
	"pods",
}

func (h *handler) getAllResourceMetrics(context context.Context, namespace string) (map[string]map[string]labelledSample, error) {
	depResult, err := h.getDeploymentMetrics(context, namespace)
	if err != nil {
		return nil, err
	}
	podResult, err := h.getPodMetrics(context, namespace)
	if err != nil {
		return nil, err
	}

	result := map[string]map[string]labelledSample{
		"deployments": depResult,
		"pods":        podResult,
	}
	return result, nil
}

func (h *handler) getDeploymentMetrics(context context.Context, namespace string) (map[string]labelledSample, error) {
	if namespace == "" {
		namespace = apiv1.NamespaceDefault
	}
	metricsResult := make(map[string]labelledSample)

	log.Printf("Listing deployments in namespace %q:\n", namespace)
	deploymentsClient := h.k8sClient.AppsV1beta1().Deployments(namespace)
	list, err := deploymentsClient.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, item := range list.Items {
		fmt.Println(item.Name, item.Namespace, item.Labels)
		metricsResult[item.Name] = labelledSample{}
	}

	queryResult := h.deployRequestRate(context, namespace)
	if err != nil {
		return nil, err
	}

	for _, m := range queryResult.res {
		metricsResult[m.Labels["k8s_deployment"]] = *m
	}
	return metricsResult, nil
}

func (h *handler) getPodMetrics(context context.Context, namespace string) (map[string]labelledSample, error) {
	if namespace == "" {
		namespace = apiv1.NamespaceDefault
	}
	metricsResult := make(map[string]labelledSample)

	log.Printf("Listing pods in namespace %q:\n", namespace)
	podList, err := h.k8sClient.CoreV1().Pods(namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, item := range podList.Items {
		fmt.Println(item.Name, item.Namespace, item.OwnerReferences, item.Labels["pod-template-hash"])
		metricsResult[item.Name] = labelledSample{}
	}

	queryResult := h.podRequestRate(context, namespace)
	if err != nil {
		return nil, err
	}

	for _, m := range queryResult.res {
		metricsResult[m.Labels["k8s_pod_template_hash"]] = *m
	}
	return metricsResult, nil
}
