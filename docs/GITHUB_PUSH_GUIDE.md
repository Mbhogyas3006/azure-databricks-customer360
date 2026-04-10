# GitHub Push Guide — Step by Step
## azure-databricks-customer360

Follow these exact steps to push the project to GitHub from VS Code.

---

## Step 1 — Create the GitHub repository

1. Go to https://github.com/new
2. Repository name: `azure-databricks-customer360`
3. Description: `Azure Databricks Customer 360 Medallion Lakehouse — Banking analytics platform with Delta Lake Bronze/Silver/Gold, SCD Type 2, PySpark, ADF, and Snowflake serving layer`
4. Set to **Public**
5. Do NOT initialize with README (you have one already)
6. Click **Create repository**
7. Copy the repo URL — looks like: `https://github.com/YOUR_USERNAME/azure-databricks-customer360.git`

---

## Step 2 — Open VS Code and initialize git

Open VS Code terminal (Ctrl + ` or Terminal → New Terminal):

```bash
# Navigate to your project folder
cd path/to/customer360

# Initialize git (if not already done)
git init

# Set your identity (first time only)
git config --global user.name "Bhogya Swetha Malladi"
git config --global user.email "bhogya-swetha.malladi@capgemini.com"
```

---

## Step 3 — Generate synthetic data (required before first push)

```bash
python data/generate_synthetic_data.py
```

The CSVs are in `.gitignore` — only the generator script is committed.

---

## Step 4 — Run tests to confirm everything works

```bash
pip install -r requirements.txt
pytest tests/ -v
```

All 14 tests should pass before you push.

---

## Step 5 — Stage and commit all files

```bash
# Check what files will be committed
git status

# Stage everything
git add .

# Verify staged files (CSVs should NOT appear)
git status

# Initial commit
git commit -m "feat: initial Customer 360 Medallion Lakehouse

- Bronze/Silver/Gold Delta Lake medallion architecture
- SCD Type 2 for customer profile history
- PySpark transformations for 5 banking source entities
- Rule-based data quality framework (35+ checks)
- ADF master pipeline orchestration design
- Snowflake serving layer DDL with RBAC
- 10 business analytics SQL queries
- pytest unit tests for all PySpark logic
- Synthetic data generator (5,015 rows across 5 tables)"
```

---

## Step 6 — Connect to GitHub and push

```bash
# Add GitHub remote
git remote add origin https://github.com/YOUR_USERNAME/azure-databricks-customer360.git

# Rename branch to main (GitHub default)
git branch -M main

# Push to GitHub
git push -u origin main
```

If prompted for credentials, use your GitHub username and a **Personal Access Token** (not your password):
- GitHub → Settings → Developer settings → Personal access tokens → Generate new token (classic)
- Scopes: check `repo`
- Use the token as your password when git prompts

---

## Step 7 — Make the repo look professional

### Add topics (GitHub tags)
On your repo page → About (gear icon) → Topics:
```
azure databricks delta-lake pyspark snowflake medallion-architecture
data-engineering banking financial-services scd-type-2 adf python sql
```

### Pin the repo to your profile
GitHub profile → Customize your pins → select `azure-databricks-customer360`

### Add a repo description
```
Azure Databricks Customer 360 Medallion Lakehouse — 
Banking analytics platform with Delta Lake Bronze/Silver/Gold, 
SCD Type 2, PySpark, ADF orchestration, and Snowflake serving layer
```

---

## Step 8 — Future updates (after initial push)

Every time you make changes:

```bash
git add .
git commit -m "feat: add Gold segment metrics table"
git push
```

### Good commit message conventions
```
feat: add Gold segment metrics aggregation
fix: correct SCD2 merge condition for unchanged rows
docs: add Power BI dashboard mockup screenshots
test: add churn risk flag edge case test
perf: add Z-order on Silver transactions account_id column
refactor: extract DQ engine into reusable module
```

---

## Step 9 — Add screenshots to the repo

Create a `docs/screenshots/` folder and add:

1. **architecture_diagram.png** — Draw in draw.io (free at app.diagrams.net), export as PNG
2. **bronze_delta_table.png** — Screenshot from Databricks: Delta table with metadata columns
3. **silver_scd2_history.png** — Show a customer with 2 SCD2 versions
4. **gold_customer_360_sample.png** — Sample output of Customer 360 table
5. **dq_results_table.png** — DQ audit table showing PASS/FAIL counts
6. **snowflake_schema.png** — Snowflake schema browser showing Gold tables
7. **powerbi_mockup.png** — Power BI dashboard screenshot (even a mockup is fine)

```bash
git add docs/screenshots/
git commit -m "docs: add architecture diagram and pipeline screenshots"
git push
```

---

## Optional — GitHub Actions CI (makes it look very professional)

Create `.github/workflows/ci.yml`:

```yaml
name: CI — Data Quality Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '11'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run unit tests
        run: pytest tests/ -v --tb=short
```

```bash
git add .github/
git commit -m "ci: add GitHub Actions test workflow"
git push
```

This adds a green checkmark badge to your repo — extremely professional for interviews.

---

## Final checklist before sharing the link

- [ ] `README.md` renders correctly on GitHub
- [ ] Architecture diagram image is visible
- [ ] Topics are added
- [ ] Repo is pinned on your profile
- [ ] GitHub Actions CI is passing (green checkmark)
- [ ] At least one screenshot in `docs/screenshots/`
- [ ] `git log --oneline` shows meaningful commit history

---

*You're ready. Share the link as: `github.com/YOUR_USERNAME/azure-databricks-customer360`*
