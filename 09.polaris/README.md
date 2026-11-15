# Module 9: Polaris

This module needs the course Onyxia cluster and an S3 bucket containing the
module 7 Iceberg warehouse. To deploy the pinned Polaris and PostgreSQL Helm
dependencies and bootstrap the `dev` and `analyst` role chains:

```bash
eval "$(aws configure export-credentials --profile default --format env)"
export POLARIS_ROOT_SECRET="$(openssl rand -hex 24)"
bash 09.polaris/deploy.sh
```

Keep `POLARIS_ROOT_SECRET` in a private password store; a rerun needs the same
value. The helper writes newly generated principal credentials to
`~/.polaris-credentials.env` with owner-only permissions. Its Helm settings
follow the [upstream deployment lab](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/09.polaris/lab-1-deployment.md).

From the Onyxia workspace, set up the same S3 variables used for module 7:

```bash
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
set -a; . /home/onyxia/.polaris-credentials.env; set +a
python 09.polaris/lab_polaris.py --principal dev
python 09.polaris/lab_polaris.py --principal analyst
```

The dev run registers the latest metadata JSON for `silver.orders`, verifies
the row count against the Hadoop catalog, creates `silver.users` through
Polaris, and evolves its schema. If module 8 already added `country`, this
run adds `polaris_tag` instead. The analyst run verifies reads and checks
that an insert and schema change are denied. Run the principals in
separate Python processes so the Spark catalog and credentials are refreshed.

Once the live table has changed, download only actual Iceberg metadata for
review and commit:

```bash
aws s3 --profile default sync "s3://$LAB_BUCKET_NAME/warehouse/" ./warehouse/ \
  --exclude '*' --include 'silver/*/metadata/*'
git status --short
git add warehouse/silver/orders/metadata warehouse/silver/users/metadata
git diff --cached --check
git commit -m "feat(polaris-lab): add Polaris table metadata"
```

The registered orders table must remain in Polaris for module 10. Polaris
checks catalog access, while the S3 credentials supplied to Spark independently
control direct object access on this Ceph setup.
