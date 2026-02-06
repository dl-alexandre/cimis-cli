# GitHub Personal Access Token Setup

## Issue
The CI workflows need to access the private `cimis-tsdb` repository. The default `GITHUB_TOKEN` doesn't have permission to access other private repositories.

## Solution
Create a Personal Access Token (PAT) with `repo` scope and add it as a GitHub secret.

## Steps

### 1. Create Personal Access Token

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
   - Or visit: https://github.com/settings/tokens

2. Click "Generate new token" → "Generate new token (classic)"

3. Configure the token:
   - **Note**: `CIMIS CLI - Private Repo Access`
   - **Expiration**: Choose appropriate expiration (recommend "No expiration" or "1 year")
   - **Scopes**: Check `repo` (Full control of private repositories)
     - This includes: `repo:status`, `repo_deployment`, `public_repo`, `repo:invite`, `security_events`

4. Click "Generate token"

5. **IMPORTANT**: Copy the token immediately - you won't be able to see it again!

### 2. Add Secret to Repository

1. Go to your `cimis-cli` repository on GitHub
   - https://github.com/dl-alexandre/cimis-cli

2. Navigate to Settings → Secrets and variables → Actions

3. Click "New repository secret"

4. Configure the secret:
   - **Name**: `GH_PAT`
   - **Value**: Paste the token you copied in step 1

5. Click "Add secret"

### 3. Add TAP Token (for Homebrew releases)

The release workflow also needs a token to update the Homebrew tap. You can use the same PAT:

1. In the same Secrets page, click "New repository secret"

2. Configure:
   - **Name**: `TAP_GITHUB_TOKEN`
   - **Value**: Paste the same token

3. Click "Add secret"

## Verify Setup

After adding the secrets:

1. Push a commit to trigger CI
2. Check the workflow run at: https://github.com/dl-alexandre/cimis-cli/actions
3. The "Checkout cimis-tsdb" step should now succeed

## Security Notes

- The PAT grants access to ALL your private repositories
- Keep it secure - never commit it to code
- Rotate it periodically for security
- You can delete it anytime from GitHub settings
- Consider using a fine-grained PAT (beta) for better security control

## Troubleshooting

**If CI still fails after adding PAT:**

1. Verify the secret name is exactly `GH_PAT` (case-sensitive)
2. Verify the token has `repo` scope enabled
3. Ensure the token hasn't expired
4. Check that the token is from an account with access to both repositories

**If release workflow fails:**

1. Verify `TAP_GITHUB_TOKEN` secret exists
2. Ensure the token has write access to the homebrew-tap repository
