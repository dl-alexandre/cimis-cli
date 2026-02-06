# Building with Private Dependencies in GitHub Actions

This guide shows how to access a private `cimis-tsdb` repository during the `cimis-cli` release workflow.

## Option 1: GITHUB_TOKEN (Recommended - Same Account)

**Best for:** Both repos under the same GitHub account/org

**Pros:**
- No manual token management
- Automatically provided by GitHub Actions
- Scoped to the workflow run
- No expiration issues

**Setup:**

1. Update the checkout step in `.github/workflows/release.yml`:

```yaml
- name: Checkout cimis-tsdb (private)
  uses: actions/checkout@v4
  with:
    repository: dl-alexandre/cimis-tsdb
    token: ${{ secrets.GITHUB_TOKEN }}
    path: cimis-tsdb-dep
```

2. Ensure the workflow has proper permissions (add at top of workflow):

```yaml
permissions:
  contents: write
  # No additional permissions needed for same-account private repos
```

**That's it!** The built-in `GITHUB_TOKEN` automatically has access to private repos in the same account.

---

## Option 2: Personal Access Token (PAT)

**Best for:** Different accounts, or need more control

**Pros:**
- Works across different GitHub accounts
- Can be scoped to specific repos
- Fine-grained control with Fine-grained PATs

**Setup:**

### Step 1: Create a Personal Access Token

1. Go to GitHub Settings → Developer settings → Personal access tokens → Fine-grained tokens
2. Click "Generate new token"
3. Configure:
   - **Name:** `cimis-cli-access-to-tsdb`
   - **Expiration:** 1 year (or custom)
   - **Repository access:** Only select repositories → `cimis-tsdb`
   - **Permissions:**
     - Contents: Read-only
4. Click "Generate token" and copy it

### Step 2: Add Token as Secret

1. Go to `cimis-cli` repository → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `TSDB_ACCESS_TOKEN`
4. Paste the PAT
5. Click "Add secret"

### Step 3: Update Workflow

```yaml
- name: Checkout cimis-tsdb (private)
  uses: actions/checkout@v4
  with:
    repository: dl-alexandre/cimis-tsdb
    token: ${{ secrets.TSDB_ACCESS_TOKEN }}
    path: cimis-tsdb-dep
```

**Important:** Remember to renew the token before expiration!

---

## Option 3: SSH Deploy Keys

**Best for:** Maximum security, read-only access

**Pros:**
- Most secure (repository-specific keys)
- No expiration
- Read-only by default
- Separate keys for each repo

**Setup:**

### Step 1: Generate SSH Key Pair

```bash
ssh-keygen -t ed25519 -C "cimis-cli-build-access" -f ~/.ssh/cimis_tsdb_deploy_key -N ""
```

This creates:
- `~/.ssh/cimis_tsdb_deploy_key` (private key)
- `~/.ssh/cimis_tsdb_deploy_key.pub` (public key)

### Step 2: Add Public Key to cimis-tsdb

1. Go to `cimis-tsdb` repository → Settings → Deploy keys
2. Click "Add deploy key"
3. Title: `cimis-cli-build-access`
4. Key: Paste contents of `~/.ssh/cimis_tsdb_deploy_key.pub`
5. **Do NOT** check "Allow write access"
6. Click "Add key"

### Step 3: Add Private Key to cimis-cli Secrets

1. Go to `cimis-cli` repository → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `TSDB_DEPLOY_KEY`
4. Value: Paste contents of `~/.ssh/cimis_tsdb_deploy_key` (private key)
5. Click "Add secret"

### Step 4: Update Workflow

```yaml
- name: Setup SSH for private repo
  uses: webfactory/ssh-agent@v0.9.0
  with:
    ssh-private-key: ${{ secrets.TSDB_DEPLOY_KEY }}

- name: Checkout cimis-tsdb (private via SSH)
  uses: actions/checkout@v4
  with:
    repository: dl-alexandre/cimis-tsdb
    ssh-key: ${{ secrets.TSDB_DEPLOY_KEY }}
    path: cimis-tsdb-dep
```

---

## Comparison Table

| Feature | GITHUB_TOKEN | PAT | SSH Deploy Key |
|---------|--------------|-----|----------------|
| Setup Complexity | ⭐ Easiest | ⭐⭐ Medium | ⭐⭐⭐ Advanced |
| Security | High | Medium-High | Highest |
| Cross-Account | ❌ No | ✅ Yes | ✅ Yes |
| Expiration | Never | 1 year max | Never |
| Scope | Automatic | Configurable | Repo-specific |
| Maintenance | None | Renew token | None |

## Recommendation

**Use GITHUB_TOKEN** if both repos are under `dl-alexandre` - it's the simplest and most maintainable.

If you need cross-account access or more control, use **SSH Deploy Keys** for production or **PAT** for simplicity.

---

## Verification

After setup, test by creating a draft release:

```bash
git tag v0.0.1-test
git push origin v0.0.1-test
```

Check the Actions tab to see if the workflow can access `cimis-tsdb` successfully.

Delete the test tag after verification:
```bash
git tag -d v0.0.1-test
git push origin :refs/tags/v0.0.1-test
```

---

## Troubleshooting

### Error: "Repository not found"

**GITHUB_TOKEN:** Ensure both repos are under the same account/org

**PAT:** Check token permissions include Contents: Read for the target repo

**SSH:** Verify the deploy key was added to the correct repository

### Error: "Permission denied"

**PAT:** Token may have expired or insufficient permissions

**SSH:** Private key may be incorrect or missing

### Go module errors during build

If the build fails with module not found errors, ensure the local replace directive is removed during release:

```yaml
- name: Update go.mod for release
  run: |
    go mod edit -dropreplace github.com/dl-alexandre/cimis-tsdb
    go mod tidy
```
