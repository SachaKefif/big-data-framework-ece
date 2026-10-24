# Lab 2 S3 command checklist

Run these commands only in the configured Onyxia service. They intentionally use
the `default` profile, whose temporary credentials should not be committed.

First check the masked profile configuration and your existing bucket. If the
bucket is absent, open Onyxia's file explorer to create the assigned bucket;
do not create a different bucket.

```bash
aws configure list --profile default
aws s3 --profile default ls
```

```bash
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default || echo "https://$AWS_S3_ENDPOINT")
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

# A small round-trip checks the actual PUT and GET path before the full datasets.
printf 'ece-s3-smoke-test\n' > s3-smoke-test.txt
aws s3 --profile default cp s3-smoke-test.txt "s3://$LAB_BUCKET_NAME/lab-smoke-test.txt"
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/lab-smoke-test.txt" s3-smoke-test-downloaded.txt
cmp s3-smoke-test.txt s3-smoke-test-downloaded.txt
aws s3 --profile default rm "s3://$LAB_BUCKET_NAME/lab-smoke-test.txt"
rm s3-smoke-test.txt s3-smoke-test-downloaded.txt

uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
aws s3 --profile default cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile default cp orders.csv "s3://$LAB_BUCKET_NAME/bronze/orders.csv"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive
```

## Object storage demonstrations

The course uses `users.csv` for these exercises. Keep a copy at
`bronze/users.csv` for later labs. Run each operation against your assigned
bucket only.

```bash
# Listing, download, rename, deletion, immediate consistency.
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users_downloaded.csv
cmp users.csv users_downloaded.csv
aws s3 --profile default cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users_renamed.csv"
aws s3 --profile default mv "s3://$LAB_BUCKET_NAME/bronze/users_renamed.csv" "s3://$LAB_BUCKET_NAME/bronze/users_temporary.csv"
aws s3 --profile default rm "s3://$LAB_BUCKET_NAME/bronze/users_temporary.csv"
aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users_immediate.csv
cmp users.csv users_immediate.csv
rm users_downloaded.csv users_immediate.csv

# System and user metadata. A single-part ETag can be compared to md5sum.
aws s3api --profile default head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv
md5sum users.csv
aws s3 --profile default cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv" \
  --metadata 'source=dataset_users.py,version=1,rows=50'
aws s3api --profile default head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv \
  --query Metadata

# Keep the signed URL in a shell variable; never print or commit it.
url=$(aws s3 --profile default presign "s3://$LAB_BUCKET_NAME/bronze/users.csv" --expires-in 300)
curl --fail --silent "$url" -o users_presigned.csv
cmp users.csv users_presigned.csv
rm users_presigned.csv
unset url

# Multipart upload and inspection. The suffix of a multipart ETag counts parts.
dd if=/dev/urandom of=large_dataset.bin bs=1M count=200
aws s3 --profile default cp large_dataset.bin "s3://$LAB_BUCKET_NAME/large/dataset.bin"
aws s3api --profile default head-object --bucket "$LAB_BUCKET_NAME" --key large/dataset.bin --query ETag
aws s3api --profile default list-multipart-uploads --bucket "$LAB_BUCKET_NAME"
aws s3 --profile default rm "s3://$LAB_BUCKET_NAME/large/dataset.bin"
rm large_dataset.bin
```

The official lab also demonstrates a presigned PUT URL with boto3,
bucket versioning, lifecycle rules, a temporary deny policy and an ACL read.
Use the exact [official lab commands](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/03.object-storage/lab-2-s3.md#presigned-urls)
for those administrative operations after checking the bucket's permissions.
Record `AccessDenied` or `NotImplemented` and continue as the lab directs.
Remove a temporary policy and lifecycle rule afterwards. Versioning can only
be suspended once enabled; all versions and delete markers must be removed
separately. Keep `bronze/users.csv` and `bronze/orders.csv` for the next labs.

The credentials in Onyxia expire. If an operation returns `ExpiredToken`,
restart the service and repeat the profile setup. If a small upload still
returns `InternalError`, save only the error code, request ID and timestamp in
`LAB_STATUS.md`; redact credentials, authorization headers and signed URLs.

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
kubectl auth can-i create jobs
kubectl auth can-i create secrets
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
