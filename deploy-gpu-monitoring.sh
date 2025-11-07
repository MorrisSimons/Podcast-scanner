#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
NAMESPACE="gpu-monitoring"
CHART_NAME="dcgm-exporter"
RELEASE_NAME="dcgm-exporter"
VALUES_FILE="$DIR/k8s/monitoring/dcgm-exporter/values.yaml"

kubectl apply -f "$DIR/k8s/monitoring/namespace.yaml"

helm repo add gpu-helm-charts https://nvidia.github.io/dcgm-exporter/helm-charts --force-update
helm repo update

helm upgrade --install "$RELEASE_NAME" gpu-helm-charts/"$CHART_NAME" \
  -n "$NAMESPACE" \
  -f "$VALUES_FILE"

kubectl apply -f "$DIR/k8s/monitoring/prometheus/servicemonitor-dcgm.yaml"
kubectl apply -f "$DIR/k8s/monitoring/vgpu-monitor/serviceaccount.yaml"
kubectl apply -f "$DIR/k8s/monitoring/vgpu-monitor/daemonset.yaml"
kubectl apply -f "$DIR/k8s/monitoring/vgpu-monitor/service.yaml"
kubectl apply -f "$DIR/k8s/monitoring/vgpu-monitor/servicemonitor.yaml"
kubectl apply -f "$DIR/k8s/monitoring/prometheus/prometheusrule-gpu.yaml"

kubectl get pods -n "$NAMESPACE"
kubectl get servicemonitors.monitoring.coreos.com -n "$NAMESPACE"
kubectl get prometheusrules.monitoring.coreos.com -n "$NAMESPACE"

