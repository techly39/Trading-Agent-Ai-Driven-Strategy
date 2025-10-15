# Applying the DF1 Market Data Feed Changes

This project uses a standard GitHub pull request (PR) workflow. Follow the steps below to bring the DF1 market data feed changes into your local repository and merge them.

## 1. Fetch the PR locally

```bash
git fetch origin pull/<PR_NUMBER>/head:df1-market-data-feed
```

Replace `<PR_NUMBER>` with the PR identifier shown in GitHub. The command above creates a local branch named `df1-market-data-feed` tracking the PR contents.

## 2. Review the diff

```bash
git checkout df1-market-data-feed
git status
git diff main...df1-market-data-feed
```

Inspect the changes, run tests (see `README.md`), and ensure everything looks correct.

## 3. Merge or rebase into `main`

Once satisfied, integrate the branch:

```bash
git checkout main
git pull origin main
# Option A – merge
git merge df1-market-data-feed
# Option B – rebase (keeps history linear)
# git rebase df1-market-data-feed
```

Resolve any conflicts if prompted.

## 4. Push the updated `main`

```bash
git push origin main
```

The PR on GitHub will automatically update and can then be marked as merged or closed.

## 5. Clean up local branches (optional)

```bash
git branch -d df1-market-data-feed
```

These steps ensure the DF1 market data feed implementation lands in your repository while preserving review history and enabling rollback if needed.
