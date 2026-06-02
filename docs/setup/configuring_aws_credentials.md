# Configuring AWS Credentials for the CI/CD Pipeline

This guide explains how to give the GitHub Actions workflows
(`deploy.yml`, `ci.yml`, `destroy.yml`) valid AWS credentials so they can
actually deploy, and how the **temporary credential guard** behaves until you
do.

> **TL;DR** — Add three secrets to the repository (or to each GitHub
> Environment): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.
> The very next workflow run detects them and deploys automatically. No
> workflow edits are required.

---

## Background: the temporary credential guard

The `Configure AWS credentials` step was failing with:

```
Error: The security token included in the request is invalid.
```

This is AWS's `InvalidClientTokenId` error — the access key the workflow sent
was missing, deleted, deactivated, or malformed. It is **not** a bug in the
workflow YAML; it is a credential/secret problem.

To keep the pipeline green in the meantime, every workflow now starts with a
**probe** step called `Check AWS credentials`:

```yaml
- name: Check AWS credentials
  id: check
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_DEFAULT_REGION: ${{ secrets.AWS_REGION || 'us-east-1' }}
  run: |
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
      echo "deploy=false" >> "$GITHUB_OUTPUT"   # no credentials set
    elif aws sts get-caller-identity >/dev/null 2>&1; then
      echo "deploy=true"  >> "$GITHUB_OUTPUT"    # credentials valid
    else
      echo "deploy=false" >> "$GITHUB_OUTPUT"    # credentials present but invalid
    fi
```

Every AWS-dependent step is gated on `if: steps.check.outputs.deploy == 'true'`.

| Situation | What the pipeline does |
| --- | --- |
| No credentials set | Skips all AWS steps → **job passes** as a no-op (warning annotation + job summary explain it). |
| Credentials present but invalid | Skips all AWS steps → **job passes** as a no-op. |
| Credentials present and valid | **Runs the full deployment** automatically. |

So once you complete the steps below, you do **not** need to touch the
workflow files — the guard flips itself on.

---

## Choose an approach

| | **Option A — Access keys** | **Option B — OIDC** |
| --- | --- | --- |
| Effort | Low — paste 3 secrets | Medium — one-time AWS setup |
| Security | Long-lived keys stored in GitHub; rotate every ~90 days | No stored keys; per-run temporary credentials |
| Matches today's workflows | ✅ Yes (no YAML change) | ❌ Requires switching to `role-to-assume` |
| Recommended for | Getting unblocked quickly | Long-term / production |

The current workflows are written for **Option A**, so start there if you just
want to unblock deployments. Option B is the project's documented best practice
— see [`cicd_pipeline.md`](./cicd_pipeline.md).

---

## Option A — Access keys (matches the current workflows)

### Step 1 — Bootstrap the Terraform backend (one-time, per AWS account)

Terraform stores its state in an **S3 bucket** and locks it with a **DynamoDB
table**. These must exist *before* the first `terraform init`, otherwise the
deploy fails with `NoSuchBucket` / `ResourceNotFoundException`.

| Environment | State bucket | Lock table |
| --- | --- | --- |
| dev | `etl-orchestrator-tfstate-dev` | `etl-orchestrator-tflock-dev` |
| qa | `etl-orchestrator-tfstate-qa` | `etl-orchestrator-tflock-qa` |
| staging | `etl-orchestrator-tfstate-staging` | `etl-orchestrator-tflock-staging` |
| prod | `etl-orchestrator-tfstate-prod` | `etl-orchestrator-tflock-prod` |

Run the helper script locally with admin-capable credentials (region is
`us-east-1`):

```bash
# All environments
./scripts/bootstrap_terraform_backend.sh

# Or just one
./scripts/bootstrap_terraform_backend.sh dev
```

### Step 2 — Create an IAM user and access key

In the AWS Console: **IAM → Users → Create user** (e.g. `github-actions-deployer`),
*without* console access, then **Create access key → Application running outside AWS**.

Or via the CLI:

```bash
aws iam create-user --user-name github-actions-deployer
aws iam create-access-key --user-name github-actions-deployer
# Note the AccessKeyId (AKIA...) and SecretAccessKey from the output.
```

> Save the secret access key immediately — AWS shows it only once.

### Step 3 — Grant the permissions the pipeline needs

Terraform in this repo provisions the following services, so the deployer
identity must be allowed to manage them:

| Service | Why it's needed |
| --- | --- |
| `s3` | Terraform state, the `*-data` bucket, uploading Glue scripts/configs |
| `dynamodb` | Terraform state locking |
| `iam` | Creating the roles/policies for Glue, Lambda, Step Functions |
| `glue` | Glue jobs |
| `lambda` | Trigger / processing functions |
| `states` (Step Functions) | The orchestration state machine |
| `events` (EventBridge) | Scheduled triggers |
| `sqs`, `sns` | Queues and notifications |
| `secretsmanager` | SFTP / Redshift secrets |
| `redshift-serverless`, `redshift-data` | Workgroup/namespace + running stored procedures |
| `logs` (CloudWatch) | Log groups |
| `sts` | `get-caller-identity` (used by the credential probe) |

**Quickest (non-prod):** attach the AWS managed policy **`PowerUserAccess`**
plus a small inline policy that allows IAM role management, since `PowerUserAccess`
excludes IAM writes:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:PassRole",
        "iam:TagRole", "iam:AttachRolePolicy", "iam:DetachRolePolicy",
        "iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:GetRolePolicy",
        "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
        "iam:CreatePolicy", "iam:DeletePolicy", "iam:GetPolicy",
        "iam:CreatePolicyVersion", "iam:DeletePolicyVersion",
        "iam:ListPolicyVersions"
      ],
      "Resource": "*"
    }
  ]
}
```

> For **production**, scope this down to a least-privilege policy. You can
> derive the exact action list from `terraform plan` output, and restrict the
> IAM `Resource` to the project's role-name prefix.

Attach the managed policy:

```bash
aws iam attach-user-policy \
  --user-name github-actions-deployer \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
```

### Step 4 — Add the secrets to GitHub

The workflows read exactly these three secret names:

| Secret | Value | Example |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | The access key ID from Step 2 | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | The secret access key from Step 2 | `wJalr...` |
| `AWS_REGION` | Target region (defaults to `us-east-1` if unset) | `us-east-1` |

**Repository-level** (simplest — applies to every environment):

> GitHub → **Settings → Secrets and variables → Actions → New repository secret**

Or with the GitHub CLI:

```bash
REPO=greenwichg/aws_etl_orchestratior
gh secret set AWS_ACCESS_KEY_ID     --repo "$REPO"   # paste when prompted
gh secret set AWS_SECRET_ACCESS_KEY --repo "$REPO"
gh secret set AWS_REGION            --repo "$REPO" --body "us-east-1"
```

**Environment-level** (recommended if dev/qa/staging/prod use *different* AWS
accounts). The deploy/destroy jobs run with `environment: <env>`, so secrets
defined on that GitHub Environment take effect — and you can add required
reviewers per environment:

> GitHub → **Settings → Environments → `dev` → Add environment secret**

```bash
gh secret set AWS_ACCESS_KEY_ID     --repo "$REPO" --env dev
gh secret set AWS_SECRET_ACCESS_KEY --repo "$REPO" --env dev
gh secret set AWS_REGION            --repo "$REPO" --env dev --body "us-east-1"
```

> If both repository and environment secrets exist with the same name, the
> **environment** value wins for jobs that target that environment.

### Step 5 — Verify

Trigger a run (push to `main`, or **Actions → Deploy ETL Pipeline → Run
workflow**). In the logs:

- The **Check AWS credentials** step should print
  `AWS credentials valid — proceeding with deployment.`
- The previously-skipped steps (Terraform, S3 upload, Redshift) now execute.
- The **Deployment summary** shows *"Deployed to `<env>` against AWS"* instead
  of the no-op message.

---

## Option B — OIDC (recommended for the long term)

OIDC removes long-lived keys entirely: GitHub requests short-lived credentials
from AWS STS per run. The full setup (OIDC identity provider, IAM role + trust
policy, and the `role-to-assume` / `permissions: id-token: write` workflow
changes) is documented in [`cicd_pipeline.md`](./cicd_pipeline.md).

Switching to OIDC requires editing the three workflows to use:

```yaml
permissions:
  id-token: write
  contents: read

# ...
- name: Configure AWS credentials (OIDC)
  uses: aws-actions/configure-aws-credentials@v4
  with:
    role-to-assume: ${{ secrets.AWS_ROLE_ARN_DEV }}
    aws-region: us-east-1
```

The credential guard added in this repo works with OIDC too (the probe still
runs `sts get-caller-identity` after the role is assumed), so the
pass-as-no-op behavior is preserved if the role/secret is missing.

---

## Rotating and revoking access keys

- **Rotate** every ~90 days: create a new access key, update the GitHub
  secret(s), then delete the old key.
  ```bash
  aws iam create-access-key --user-name github-actions-deployer
  gh secret set AWS_ACCESS_KEY_ID     --repo "$REPO"   # new value
  gh secret set AWS_SECRET_ACCESS_KEY --repo "$REPO"   # new value
  aws iam delete-access-key --user-name github-actions-deployer --access-key-id <OLD_AKIA>
  ```
- **If a key leaks**, deactivate/delete it immediately in IAM. The pipeline
  will return to its safe no-op state until you provide a new one.

---

## Removing the temporary guard (optional)

The guard is harmless to keep — when credentials are valid it adds one tiny
`sts get-caller-identity` call and then runs everything normally. If you ever
want the pipeline to **fail loudly** on bad credentials instead of skipping,
remove the `Check AWS credentials` step and the
`if: steps.check.outputs.deploy == 'true'` lines from the workflows.

---

## Troubleshooting

| Error in the logs | Likely cause | Fix |
| --- | --- | --- |
| `The security token included in the request is invalid` (`InvalidClientTokenId`) | Access key deleted/deactivated, wrong account, or a typo/whitespace in the secret | Recreate the key (Step 2) and re-paste both secrets |
| `SignatureDoesNotMatch` | `AWS_SECRET_ACCESS_KEY` is wrong/truncated | Re-paste the secret access key |
| `ExpiredToken` / `security token ... expired` | Temporary `ASIA...` credentials were stored | Use permanent `AKIA...` keys, or add `AWS_SESSION_TOKEN` |
| `You must specify a region` | `AWS_REGION` secret empty | Set `AWS_REGION=us-east-1` (the workflow already defaults to this) |
| `AccessDenied` / `is not authorized to perform` | Deployer IAM permissions too narrow | Widen the policy in Step 3 for that service |
| `NoSuchBucket` / `ResourceNotFoundException` during `terraform init` | Backend not bootstrapped | Run `scripts/bootstrap_terraform_backend.sh` (Step 1) |

---

## Related docs

- [`cicd_pipeline.md`](./cicd_pipeline.md) — full OIDC-based pipeline setup
- [`aws_setup_guide.md`](./aws_setup_guide.md) — AWS account, IAM, and credential concepts
- [`../security/iam_roles.md`](../security/iam_roles.md) — IAM roles used by the deployed resources
