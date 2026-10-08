# Lab 2 S3 command checklist

Run these commands only in the configured Onyxia service. They intentionally use
the `default` profile, whose temporary credentials should not be committed.

```bash
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default || echo "https://$AWS_S3_ENDPOINT")
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
aws s3 --profile default cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile default cp orders.csv "s3://$LAB_BUCKET_NAME/bronze/orders.csv"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
```

Create the Kubernetes inputs and run the included Job:

```bash
kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile default)" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"
aws configure export-credentials --profile default --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env
kubectl apply -f k8s/job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=120s
kubectl logs job/upload-bronze
```

Clean up the Kubernetes objects after verifying the two bronze objects:

```bash
kubectl delete -f k8s/job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
```
