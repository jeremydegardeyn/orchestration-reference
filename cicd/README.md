# CI/CD (GitHub Actions)

Deployment for all three components runs in **GitHub Actions** with **keyless Workload Identity
Federation** — no service-account keys stored anywhere. This replaces the prior Harness pipelines.

## Why GitHub Actions over Harness here

- **Keyless auth.** GitHub's OIDC token is exchanged for short-lived GCP credentials at runtime (WIF).
  Harness typically held a long-lived SA key or connector credential — this removes that secret entirely.
- **Co-located with code.** Workflows live next to the configs they deploy; a PR shows the pipeline change
  alongside the code change. No separate Harness pipeline to keep in sync.
- **Monorepo path filtering.** Each component deploys only when its own files change.
- **Free for this scale** and no external CD control plane to operate.

If you later need multi-service orchestration, approval chains across many services, or non-GitHub
sources, Harness still earns its place — but for these three components, GitHub Actions is simpler.

## Workflows

| Workflow | Trigger | Does |
|---|---|---|
| `ci.yml` | PR to main | Validates only what changed: Composer DAG generation, `dagster definitions validate`, analyzer import |
| `composer-deploy.yml` | push to main (`composer/**`) or manual | Builds env-specific DAGs, rsyncs to the Composer bucket |
| `dagster-deploy.yml` | push to main (`dagster/**`) or manual | Builds + pushes image, `helm upgrade` on GKE |
| `error-analyzer-deploy.yml` | push to main (`error-analyzer/**`) or manual | Builds + pushes image, updates Cloud Run |

**Promotion model:** push to `main` auto-deploys **dev**. Promote to **qa/prod** via *Run workflow* →
pick the environment. `prod` is gated by a GitHub **Environment protection rule** (required reviewers).

## One-time setup

### 1. Bootstrap WIF + the deploy SA (Terraform)

```bash
cd cicd/terraform
terraform init
terraform apply -var="github_repo=<owner>/<repo>"
terraform output wif_provider           # -> WIF_PROVIDER
terraform output deploy_service_account # -> DEPLOY_SA
```

### 2. Create GitHub Environments

Create `dev`, `qa`, `prod` under **Settings → Environments**. Add **required reviewers** to `prod`.

### 3. Set per-environment variables

For each Environment, set these **Variables** (Settings → Environments → <env> → Variables):

| Variable | Example |
|---|---|
| `GCP_PROJECT` | `your-gcp-project-id` |
| `GCP_REGION` | `us-central1` |
| `WIF_PROVIDER` | (terraform output `wif_provider`) |
| `DEPLOY_SA` | (terraform output `deploy_service_account`) |
| `COMPOSER_ENV` | `jsd-composer-dev` |
| `ARTIFACT_REGISTRY` | `us-central1-docker.pkg.dev/your-gcp-project-id/edp` |
| `GKE_CLUSTER` | `dagster-dev` |
| `GKE_ZONE` | `us-central1-c` |
| `GKE_NAMESPACE` | `dagster` |

No secrets are required for auth (WIF handles it). Runtime secrets (Teams webhook, DB password) are
managed by each component's own Terraform / Kubernetes secrets, not by the workflows.

## Notes

- The Composer build **bakes env values into the generated DAGs** at build time (sourcing
  `composer/env/<env>.env`), matching the original Harness behavior — each env gets DAGs compiled with
  its own project ids.
- `dagster-deploy` and `error-analyzer-deploy` only update the image; infra/config stays in Terraform.
- To restrict which branches can deploy prod, narrow the `principalSet` in `cicd/terraform/wif.tf` to a
  specific `attribute.ref`.
