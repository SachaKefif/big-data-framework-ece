# Module 2: Kubernetes labs

Use a local minikube cluster for lab 1. From the repository root:

```bash
minikube start
kubectl apply -f 02.containerization-and-kubernetes/lab-1/
kubectl rollout status deployment/kubernetes-bootcamp
kubectl get pods,services
minikube service kubernetes-bootcamp
kubectl scale deployment/kubernetes-bootcamp --replicas=5
kubectl rollout status deployment/kubernetes-bootcamp
kubectl scale deployment/kubernetes-bootcamp --replicas=2
```

The deployment and service manifests complete the upstream lab's blanks.
Lab 2 uses a ConfigMap and a Secret as environment variables and mounted files:

```bash
kubectl create secret generic s3-credentials \
  --from-literal=access_key="$AWS_ACCESS_KEY_ID" \
  --from-literal=secret_key="$AWS_SECRET_ACCESS_KEY"
kubectl apply -f 02.containerization-and-kubernetes/lab-2/
kubectl wait --for=condition=complete job/pipeline-config-check --timeout=120s
kubectl logs job/pipeline-config-check
```

Lab 3 includes a four-Pod Job and a CronJob. Apply
`02.containerization-and-kubernetes/lab-3/jobs.yaml`, inspect Jobs and logs,
then suspend the CronJob before cleanup:

```bash
kubectl apply -f 02.containerization-and-kubernetes/lab-3/jobs.yaml
kubectl wait --for=condition=complete job/batch-export --timeout=120s
kubectl get jobs,cronjobs,pods
kubectl patch cronjob bronze-to-silver -p '{"spec":{"suspend":true}}'
kubectl delete -f 02.containerization-and-kubernetes/lab-3/jobs.yaml
kubectl delete -f 02.containerization-and-kubernetes/lab-2/
kubectl delete secret s3-credentials
kubectl delete -f 02.containerization-and-kubernetes/lab-1/
minikube stop
```

The [upstream labs](https://github.com/adaltas/ece-bigdata-2026-fall/tree/main/02.containerization-and-kubernetes)
also include interactive Pod inspection, image rollouts, and Secret creation
methods. Run those with `kubectl` in the cluster; they cannot be recorded as
completed until a cluster is available.
