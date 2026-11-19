# Publish this portfolio project to GitHub

This project is not currently hosted on GitHub. Publishing requires your own GitHub account and authorization.

## Option A — GitHub CLI (macOS, Linux or Windows)

1. Download and extract the repository ZIP.
2. Install Git and GitHub CLI (`gh`); authenticate with `gh auth login`.
3. In a terminal inside the extracted project directory:

```bash
git init
git add .
git commit -m "Build synthetic pharma lakehouse portfolio demo"
git branch -M main
gh repo create pharma-enterprise-lakehouse --public --source=. --remote=origin --push
```

If you have an existing Git repo, skip `git init` and adapt the remote commands.

## Option B — GitHub website

1. Create a public repository with name `pharma-enterprise-lakehouse` at https://github.com/new . Don't auto-create conflicting files.
2. Use GitHub Desktop or Git to push the extracted project directory. Uploading only the ZIP *as a file* is not the same as publishing the repository contents.
3. Confirm `README.md`, `notebooks/`, `src/`, `sql/`, `docs/`, `sample_outputs/` and `.github/workflows/ci.yml` appear in the GitHub code view.
4. Check the Actions tab: tests should pass; the Spark+Delta job exercises local JVM Spark/Delta in CI, but **not** a live AWS/Databricks deployment. It may need environment/dependency troubleshooting.

## Suggested GitHub repository description

"Synthetic pharma AWS/Databricks lakehouse: real PySpark streaming, incremental Delta CDC MERGE, SCD Type 2, replay-safe quality gates, skew join optimization and Gold analytics."

## Good topics (GitHub repository settings)

`databricks` · `aws` · `delta-lake` · `pyspark` · `data-engineering` · `lakehouse` · `cdc` · `data-quality` · `unity-catalog`

## CV / interview presentation

Call this an independent **portfolio proof-of-concept** and distinguish the executed local tests from a cloud deployment. If you later deploy it and capture real Databricks screenshots, add those results with run dates and compute details.
