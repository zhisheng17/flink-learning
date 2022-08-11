#!/bin/bash

[ ! $1 ] && echo "未配置 kubernetes.cluster-id" && exit

echo "配置的 kubernetes.cluster-id 为 $1"

[ ! $2 ] && echo "未配置 namespace" && exit

echo "配置的 namespace 为 $2"

cat ./ingress_template.yaml | sed 's/$K8S_CLUSTER_ID/'"$1"'/g' | sed 's/$K8S_NAMESPACE/'"$2"'/g' >  $1-ingress.yaml
kubectl apply -f $1-ingress.yaml
