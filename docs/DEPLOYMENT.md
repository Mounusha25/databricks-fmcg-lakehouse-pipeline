# Databricks Bundle Deployment

This project uses a Databricks Declarative Automation Bundle to deploy three
Lakeflow Jobs from version-controlled source:

- `fmcg_prepare_storage`: creates the catalog, schemas, and managed landing volume
- `fmcg_initial_load`: manual historical initialization
- `fmcg_daily_incremental`: dimension refresh plus daily order ingestion

The development target writes to `fmcg_dev` and keeps the daily schedule
paused. The production target writes to `fmcg` and enables the 02:00 UTC
schedule.

## Prerequisites

- Databricks CLI `0.278.0` or newer
- A workspace identity allowed to create jobs and Unity Catalog objects
- Workspace access to the configured source URI (S3 for production or a managed Volume for development)
- Landing prefixes named `customers`, `products`, `gross_price`, and `orders`

## Local deployment

Authenticate the Databricks CLI, then run:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev fmcg_prepare_storage
./scripts/upload_demo_data.sh
databricks bundle run --target dev fmcg_initial_load
```

The development target uses `/Volumes/fmcg_dev/bronze/landing`. The upload
script copies the ignored local sample files into that managed Volume using the
`fmcg-free` CLI profile by default. Pass a different profile or catalog as the
first and second arguments when needed:

```bash
./scripts/upload_demo_data.sh my-profile my_catalog
```

Override a bundle variable without changing source control:

```bash
databricks bundle deploy --target dev \
  --var="source_root=s3://my-fmcg-landing,catalog=my_fmcg_dev"
```

After the historical load succeeds, test the incremental workflow manually:

```bash
databricks bundle run --target dev fmcg_daily_incremental
```

## Production deployment

Validate the effective production configuration before deployment:

```bash
databricks bundle validate --target prod
databricks bundle deploy --target prod
```

The production deployment unpauses the daily incremental schedule. The initial
historical job remains manual so a deployment cannot accidentally reload all
source data.

## GitHub Actions with OIDC

The manual `Deploy Databricks bundle` workflow follows Databricks' recommended
workload identity federation pattern. Create `dev` and `prod` GitHub
environments and define these environment variables in each:

- `DATABRICKS_HOST`: workspace URL, such as `https://dbc-....cloud.databricks.com`
- `DATABRICKS_CLIENT_ID`: application ID of the federated service principal

Configure that service principal with a GitHub federation policy matching this
repository and environment. No long-lived Databricks token is stored in
GitHub. Run the workflow manually, select the target, and choose whether to run
the initial historical load after deployment.

Official references:

- [Bundle configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
- [Bundle job resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
- [GitHub Actions integration](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github)

