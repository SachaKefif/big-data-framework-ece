#!/usr/bin/env bash
# Run inside the Onyxia workspace after exporting the course S3 credentials.
set -euo pipefail
: "${KUBERNETES_NAMESPACE:?set KUBERNETES_NAMESPACE}"
: "${AWS_S3_ENDPOINT:?set AWS_S3_ENDPOINT}"
: "${AWS_ACCESS_KEY_ID:?export S3 credentials from the default profile}"
: "${AWS_SECRET_ACCESS_KEY:?export S3 credentials from the default profile}"
: "${AWS_SESSION_TOKEN:?export temporary S3 credentials from the default profile}"
: "${POLARIS_ROOT_SECRET:?set a private root password for this Polaris installation}"

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LAB_BUCKET_NAME="${LAB_BUCKET_NAME:-$KUBERNETES_NAMESPACE}"
export POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
export POLARIS_MGMT_URI="${POLARIS_MGMT_URI:-http://polaris-mgmt:8182}"
export POLARIS_CATALOG="${POLARIS_CATALOG:-$LAB_BUCKET_NAME}"
case "$AWS_S3_ENDPOINT" in
  http://*|https://*) default_s3_endpoint="$AWS_S3_ENDPOINT" ;;
  *) default_s3_endpoint="https://$AWS_S3_ENDPOINT" ;;
esac
export POLARIS_S3_ENDPOINT="${POLARIS_S3_ENDPOINT:-$default_s3_endpoint}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

if ! kubectl get secret polaris-db >/dev/null 2>&1; then
  db_password="$(openssl rand -hex 24)"
  kubectl create secret generic polaris-db \
    --from-literal=username=polaris \
    --from-literal=password="$db_password" \
    --from-literal=postgres-password="$db_password" \
    --from-literal="jdbcUrl=jdbc:postgresql://polaris-postgresql:5432/polaris"
fi
if ! kubectl get secret polaris-credential >/dev/null 2>&1; then
  kubectl create secret generic polaris-credential \
    --from-literal="POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,$POLARIS_ROOT_SECRET"
fi
kubectl create secret generic polaris-s3 \
  --from-literal="AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  --from-literal="AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  --from-literal="AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

helm dependency update "$repo/09.polaris/deployment"
helm upgrade --install polaris "$repo/09.polaris/deployment" --wait --timeout 10m
python3 "$repo/09.polaris/bootstrap.py"
