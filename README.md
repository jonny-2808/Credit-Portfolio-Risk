# Credit Portfolio Risk — Lending Club

**Business question:** Which loan applications should we approve, and what loss should we expect from the loans we hold?

This project takes a real consumer-lending dataset (Lending Club, 2007–2018, ~2.2M loans) and builds an end-to-end credit risk pipeline: data engineering in PySpark on Databricks, an analytical layer in Snowflake, a credit scorecard in SAS, and a portfolio dashboard in Tableau. The aim is to produce decisions in £, not just charts — what cutoff to approve at, and what expected loss the portfolio carries under different scenarios.

---

## Status
| Phase | Description | Status |
|------|------------|--------|
| 1 | Setup, ingest, repo | ✅ Done |
| 2 | Target definition, quality scan, vintage analysis | ✅ Done |
| 3 | ETL & analytical table | ✅ Done |
| 4 | Feature engineering (binning, encoding, date features) | ✅ Done |
| 5 | WoE transformation, IV feature selection, time-based train/test split | ✅ Done |
| 6 | Logistic regression scorecard (train, validate, scale to points) | ⏳ Next |
| 7 | PD / LGD / EAD and expected loss calculation | ⏳ |
| 8 | Approval strategy & cutoff analysis | ⏳ |
| 9 | Snowflake load + SQL reporting views | ⏳ |
| 10 | Tableau dashboard | ⏳ |
| 11 | Write-up, scorecard documentation, model governance note | ⏳ |

---

## Toolstack

- **Databricks Free Edition** (PySpark) — ingestion, cleaning, ETL, feature engineering
- **Snowflake** (free trial) — analytical warehouse
- **SAS OnDemand for Academics** — scorecard development
- **Tableau Public** — portfolio dashboard
- **GitHub** — version control

---

## Repo layout

```
credit-portfolio-risk/
├── README.md
├── notebooks/
│   ├── 01_data_overview.ipynb         # Days 1–3: ingest, target, ETL, analytical table
│   └── 04_feature_engineering.ipynb   # Day 4: binning, encoding, date features
├── docs/                               # write-up, governance note, dashboard exports (coming)
└── sql/                                # Snowflake DDL & views (coming)
```

---

## Data

- **Source:** Lending Club accepted loans CSV, Kaggle (publicly available, 2007–2018).
- **Granularity:** one row per loan, final snapshot status (not a monthly history). Vintage analysis is therefore simplified — noted in the write-up.
- **Initial volume:** ~2.2M loans, 151 columns.

---

## Day 1 — Setup & first look

**Goal:** load the data, confirm what's in it, define the target on paper.

**Actions**
- Downloaded Lending Club accepted loans dataset from Kaggle and converted to CSV (the original `.xlsx` was at Excel's row limit and risked truncation).
- Uploaded to a Databricks Unity Catalog Volume.
- Created notebook `01_data_overview` on a Serverless cluster.
- Loaded the CSV with `spark.read.csv(path, header=True, inferSchema=True)`.
- Inspected `loan_status` value counts to see what the snapshot looks like.

**Target definition (locked in)**
- **Bad (`is_bad = 1`):** `Charged Off`, `Default`
- **Good (`is_bad = 0`):** `Fully Paid`
- **Drop (`is_bad = NULL`):** `Current`, `In Grace Period`, `Late (16–30 days)`, `Late (31–120 days)` — outcomes not yet observed, cannot be labelled.

**Day 1 findings**
- Total rows: ~2.2M
- Default rate decreases as letter grade improves (A → G), as expected. Sanity check passed.
- Bureau-rich fields (`bc_util`, `il_util`, etc.) are systematically null for older vintages — a real data quality finding, kept for the controls write-up.

---

## Day 2 — Target, filtering, missingness scan

**Goal:** create the modellable population, compute baseline default rates, and identify which columns are mostly empty.

**Actions**
- Built `is_bad` using a `when().when().otherwise(None)` block in PySpark.
- Filtered to `is_bad IS NOT NULL` to produce `df_model` (1,345,349 rows — the modellable population).
- Computed overall default rate as the mean of `is_bad` (0/1 average = a rate).
- Ran a missingness scan across all 151 columns.

**Day 2 findings**
- Modellable rows: 1,345,349 (~60% of raw — the rest are still in-flight loans).
- Overall default rate: ~20%.
- Default rate rises noticeably for 2015–2016 vintages — consistent with Lending Club's known underwriting drift in that period.
- ~40 columns are >95% null (hardship, joint-app, settlement blocks). Flagged for drop.

---

## Day 3 — ETL & analytical table

**Goal:** turn the raw dataframe into a clean analytical table — junk columns dropped, leakage columns killed, data types fixed.

**Actions**
- Dropped ~40 columns >95% null (hardship/joint-app/settlement blocks, `member_id`, `desc`, `url`, `next_pymnt_d`).
- Killed 14 leakage columns (`recoveries`, `total_pymnt`, `last_pymnt_amnt`, etc.) — post-issuance fields that would leak the outcome.
- Cast `int_rate`, `revol_util` from `"13.5%"` strings to numeric.
- Parsed `term` (`" 36 months"` → 36) into `term_months`.
- Parsed `emp_length` (`"< 1 year"`, `"10+ years"`) into `emp_length_years`.
- Created `has_public_record` and `has_prior_delinq` flag columns (preserving "null = didn't happen" as information).
- Saved as managed table `workspace.default.lc_analytical`.

**Day 3 outputs**
- Analytical table: 1,345,349 rows.
- Leakage discipline applied — no post-issuance fields retained.
- Carry-forward note: cleanup retained ~99 columns instead of the intended ~50–60; numeric casts didn't fully persist across the saved table. Mitigated on Day 4 with a defensive re-cast on read.

---

## Day 4 — Feature engineering

**Goal:** transform the cleaned analytical table into a modelling-ready feature set.

**Actions**
- Re-cast `annual_inc`, `dti`, `revol_util`, `loan_amnt`, `int_rate` to numeric using `try_cast` (defensive against CSV corruption surfacing only on read).
- Re-joined `issue_d` from raw CSV (accidentally dropped during Day 3 cleanup).
- Parsed `issue_d` and `earliest_cr_line` from `"MMM-yyyy"` strings to dates, with a regex guard rejecting malformed values (e.g. interest rates that leaked into date columns on ~30 shifted rows).
- Derived `issue_year`, `issue_month`, `credit_history_months` (months between earliest credit line and loan issue).
- Binned `annual_inc`, `dti`, `revol_util` into 5 quantile buckets each (`QuantileDiscretizer`); nulls routed to their own bucket via `handleInvalid="keep"`.
- Ordinal-encoded `grade` (A–G → 1–7).
- Index-encoded `home_ownership`, `purpose`, `verification_status` for downstream WoE / one-hot.

**Monotonicity check (all features pass)**
- `inc_bin`: default rate 23.5% → 15.9% across bins 0 → 4 (falls cleanly — higher income, lower risk).
- `dti_bin`: 15.0% → 27.0% (rises cleanly — higher debt burden, higher risk).
- `util_bin`: 15.8% → 22.6% (rises; flattens at top, noted).

**Output:** `workspace.default.lc_features` — ~1.34M rows × ~20 columns.

---

## Day 5 — WoE transformation and IV-based feature selection

**Goal:** transform features into Weight of Evidence space and select by Information Value, with proper out-of-time validation setup.

**Actions**
- Time-based train/test split on `issue_year`: train ≤ 2016 (1,119,710 rows / 83%), test ≥ 2017 (225,639 rows / 17%).
- Implemented reusable WoE/IV function with Laplace smoothing (+0.5) to handle zero-count bins.
- Computed IV for 13 candidate features on training data only — prevents test-set leakage into the WoE mapping.
- Applied training-set WoE mapping to both train and test sets (fit on train, transform both).

**IV ranking (training set)**
| Feature | IV | Verdict |
|---|---|---|
| `grade_num` | 0.4677 | Strong — Lending Club's own risk grade |
| `term_months` | 0.1972 | Medium |
| `dti_bin` | 0.0727 | Weak-medium |
| `verification_status_idx` | 0.0532 | Weak |
| `issue_year` | 0.0307 | Weak (captures vintage/macro effects) |
| `inc_bin` | 0.0288 | Weak |
| `home_ownership_idx` | 0.0265 | Weak |
| `purpose_idx` | 0.0201 | Borderline, kept |
| `util_bin` | 0.0186 | **Dropped** (below 0.02 threshold) |
| `credit_history_months` | 0.0152 | **Dropped** (raw continuous, requires binning) |
| `has_public_record` | 0.0069 | **Dropped** |
| `has_prior_delinq` | 0.0020 | **Dropped** |
| `emp_length_years` | — | Not present in feature table — gap noted for Day 6 |

**Modelling decision flagged for Day 6**
- `grade_num` IV is ~2.4× the next strongest feature. Logistic regression will lean heavily on it, since `grade` is itself a model output from Lending Club's underwriting. Day 6 will train two models — with and without `grade_num` — to measure incremental signal from the remaining features. The "without grade" model is the more interesting CV artefact: it demonstrates the scorecard adds independent risk signal beyond LC's own grade.

**Output:** `workspace.default.lc_train_woe` (1,119,710 rows × 10 cols), `workspace.default.lc_test_woe` (225,639 rows × 10 cols).

---

## Next up — Day 6

Logistic regression scorecard on `lc_train_woe`. Evaluate on `lc_test_woe` using AUC, KS statistic, and Gini coefficient. Scale coefficients to standard scorecard points (e.g. 600 base, 20 PDO). Compare grade-inclusive vs grade-excluded models.
