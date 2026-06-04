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
| 6 | Logistic regression scorecard (train, validate, scale to points) | ✅ Done |
| 7 | PD / LGD / EAD and expected loss calculation | ✅ Done  |
| 8 | Approval strategy & cutoff analysis | ✅ Done |
| 9 | Snowflake load + SQL reporting views | ⏳ Next |
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

## Day 6 — Logistic regression scorecard

**Goal:** train a logistic regression PD model on the WoE-transformed training set, evaluate on the out-of-time test set, and scale coefficients into standard scorecard points.

**Feature decision pre-fit**
- Dropped `issue_year_woe` from the candidate feature list. Under a time-based split (train ≤ 2016, test ≥ 2017), 100% of test-set `issue_year` values are unseen in training, producing null WoE mappings. Vintage and macroeconomic effects belong in the PD calibration / stress overlay (Day 7), not the borrower-level scorecard.
- Filled a small residual null pocket in `dti_bin_woe` (266 train / 334 test rows, ~0.02–0.15%) with neutral WoE = 0.

**Final feature set (7 features)**
`inc_bin_woe`, `dti_bin_woe`, `grade_num_woe`, `home_ownership_idx_woe`, `purpose_idx_woe`, `verification_status_idx_woe`, `term_months_woe`.

**Models trained**
1. **Full model** — all 7 features including `grade_num_woe`
2. **No-grade model** — 6 features excluding `grade_num_woe`, to measure independent signal beyond Lending Club's own underwriting grade

**Coefficients (full model)**
All 7 coefficients are negative, consistent with the WoE convention (higher WoE = safer borrower → lower default probability). No sign inversions, no broken monotonicity. Intercept: -1.4054.

| Feature | Coefficient | Points per WoE unit |
|---|---|---|
| `home_ownership_idx_woe` | -0.8901 | 25.68 |
| `grade_num_woe` | -0.7588 | 21.89 |
| `dti_bin_woe` | -0.5642 | 16.28 |
| `inc_bin_woe` | -0.5607 | 16.18 |
| `term_months_woe` | -0.5515 | 15.91 |
| `verification_status_idx_woe` | -0.3000 | 8.66 |
| `purpose_idx_woe` | -0.1925 | 5.56 |

Note: `home_ownership` carries more weight than `grade` in the fitted model despite having lower univariate IV. This reflects redundancy between `grade` and the other features in the model (verification, term, DTI all correlate with grade) — the LR fit redistributes credit accordingly. To be revisited in Day 7 with VIF / correlation diagnostics.

**Out-of-time test results (test set: 225,639 loans issued 2017+)**

| Model | AUC | KS | Gini |
|---|---|---|---|
| Full (with grade) | 0.6923 | 0.2804 | 0.3847 |
| No-grade | 0.6453 | 0.2113 | 0.2906 |
| Δ (grade contribution) | +0.0470 | +0.0691 | +0.0941 |

**Honest interpretation**
- Full model performance is in the normal industry range for Lending Club PD scorecards (AUC 0.68–0.72). No leakage indicators.
- Grade carries roughly 60% of the discriminatory power; the remaining 6 borrower-level features carry ~40%. The no-grade model at AUC 0.6453 demonstrates the scorecard adds independent risk signal beyond LC's own grade, but is not strong enough to stand alone.
- KS of 0.28 on the full model is consistent with the AUC and acceptable for a behavioural scorecard; production scorecards typically target KS > 0.30, so this is borderline.

**Scorecard scaling**
Standard banking parameters: Base = 600, PDO = 20, target odds = 50:1 at base.
- factor = 28.85, offset = 487.12
- Score = 487.12 + 28.85 × (-logit(PD)), bounded in practice to roughly 350–850.

**Outputs**
- `workspace.default.lc_test_scored` — 225,639 scored test rows with predicted PD
- `workspace.default.lc_scorecard_points` — 7-row points table for documentation

**Open items flagged for later phases**
- Run multicollinearity diagnostics (VIF, correlation matrix) on the WoE feature set — `home_ownership` outranking `grade` in the LR fit suggests redundancy worth quantifying.
- Calibrate the model's raw PD output to observed default rates per score band on the test set (Day 7).
- Consider whether `emp_length` and `cr_hist_bin`, both lost upstream, should be reintroduced to push KS over 0.30.

---

## Day 7 — Expected Loss (EL = PD × LGD × EAD)

- Realised LGD = 0.8903, measured on charged-off loans (loss / exposure-at-default).
  Legitimate use of post-outcome columns: LGD is conditional on observed default.
- EAD proxied by funded_amnt (fixed-term loans; no CCF — that's for revolving credit).
- EL = PD × 0.8903 × funded_amnt across 225,639 out-of-time test loans.
- Total expected loss $598.1M | EL rate 18.35%.
- Out-of-time calibration: predicted EL $598.1M vs realised loss $608.1M → Pred/Actual 0.98.
- PD ranks monotonically by grade (A 6.1% → G 49.9%), confirming scorecard discrimination.

**Limitations:** 2017+ vintages partly right-censored (understates realised loss);
LGD is a realised assumption, not a fitted model. id was lost in the Day 4 feature
step and rebuilt from raw — passthrough keys should be preserved through all pipeline stages.

---

## Day 8 — Approval Strategy & Cutoff Analysis

**Goal:** turn per-loan PD/EL into a lending policy — where to set the approval cutoff, and prove the scorecard beats a naive grade rule.

**Actions**
- VIF (multicollinearity) check on the 7 WoE scorecard features via inverse-correlation-matrix method (statsmodels unavailable on Databricks Free Edition). Max VIF [x.x] ([feature]) — [below 5, no action needed].
- Rebuilt `lc_test_el`: the Day 7 saveAsTable silently never ran, so the expected-loss table was missing. Reconstructed directly off `lc_test_scored_final` (already carried id, pd_prediction, funded_amnt, int_rate, grade — no join required). Validated against Day 7 totals: EL $598.1M, EL rate 18.35% — faithful match.
- Approval cutoff swept 10%→100% on PD (monotonic with scorecard points, so equivalent to a score cutoff). Income vs expected loss tallied at each step.
- Net contribution (income − EL) is concave in approval rate — peaks at ~30% approval, PD cutoff ≈ 0.12, then declines as marginal loans destroy value.
- Swap-set vs naive grade cutoff (A–D approve): scorecard swaps in [n] loans the grade rule would decline — the incrementality case for a custom scorecard.

**Honest limitations**
- The optimal cutoff is biased too tight: interest income is one year (funded_amnt × int_rate) while EL is lifetime, an apples-to-oranges horizon. Directional trade-off is robust; the exact "30%" is NOT a real policy number.
- Income is a crude proxy — no funding cost, opex, prepayment, or time value. Relative cutoff ranking, not a P&L.
- LGD reused as Day 7 realised constant (0.8903), not a fitted model.
- 2017+ test vintages partly right-censored → realised loss understated.

**Pipeline lesson:** a `saveAsTable` that doesn't run leaves no error but loses the table. Verify outputs persisted (`SHOW TABLES`) at the end of each session.

**Outputs:** workspace.default.lc_test_el (rebuilt), workspace.default.lc_approval_sweep


