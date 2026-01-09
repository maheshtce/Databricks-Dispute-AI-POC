# Databricks notebook source
# MAGIC %skip
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC db = "dispute_ai"   # change if your schema is different
# MAGIC
# MAGIC raw = spark.table(f"{db}.raw_disputes")
# MAGIC
# MAGIC clean = (
# MAGIC     raw
# MAGIC     # dates
# MAGIC     .withColumn("service_date", F.to_date("service_date"))
# MAGIC     .withColumn("submitted_date", F.to_date("submitted_date"))
# MAGIC     .withColumn(
# MAGIC         "days_to_submit",
# MAGIC         F.when(F.col("days_to_submit").isNotNull(), F.col("days_to_submit").cast("int"))
# MAGIC          .otherwise(F.datediff(F.col("submitted_date"), F.col("service_date")))
# MAGIC     )
# MAGIC     # normalize strings
# MAGIC     .withColumn("program", F.initcap(F.trim(F.col("program"))))
# MAGIC     .withColumn("channel", F.when(F.lower(F.col("channel")).contains("340b"), F.lit("340B"))
# MAGIC                            .when(F.col("channel").isNull(), F.lit("Unknown"))
# MAGIC                            .otherwise(F.lit("Non-340B")))
# MAGIC     .withColumn("outcome", F.upper(F.trim(F.col("outcome"))))
# MAGIC     .withColumn("reason_code", F.upper(F.trim(F.col("reason_code"))))
# MAGIC     # safe numeric casts
# MAGIC     .withColumn("units", F.col("units").cast("double"))
# MAGIC     .withColumn("billed_amount", F.col("billed_amount").cast("double"))
# MAGIC     .withColumn("paid_amount", F.col("paid_amount").cast("double"))
# MAGIC     .withColumn("expected_amount", F.col("expected_amount").cast("double"))
# MAGIC )
# MAGIC
# MAGIC clean.write.format("delta").mode("overwrite").saveAsTable(f"{db}.silver_clean_disputes")
# MAGIC
# MAGIC
# MAGIC
# MAGIC spark.table(f"{db}.silver_clean_disputes").select(
# MAGIC     F.count("*").alias("rows"),
# MAGIC     F.countDistinct("dispute_id").alias("distinct_dispute_id"),
# MAGIC     F.sum(F.col("reason_code").isNull().cast("int")).alias("null_reason_code")
# MAGIC ).show()
# MAGIC
# MAGIC
# MAGIC snips = spark.table(f"{db}.policy_snippets")
# MAGIC
# MAGIC snips_clean = (
# MAGIC     snips
# MAGIC     .withColumn("program", F.initcap(F.trim(F.col("program"))))
# MAGIC     .withColumn("state", F.upper(F.trim(F.col("state"))))
# MAGIC     .withColumn("payer", F.upper(F.trim(F.col("payer"))))
# MAGIC     .withColumn("reason_code", F.upper(F.trim(F.col("reason_code"))))
# MAGIC     .withColumn("snippet_text", F.trim(F.col("snippet_text")))
# MAGIC )
# MAGIC
# MAGIC snips_clean.write.format("delta").mode("overwrite").saveAsTable(f"{db}.silver_policy_snippets")
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Base tables
# MAGIC d = spark.table(f"{db}.silver_clean_disputes")
# MAGIC
# MAGIC # Lookup tables (select only what you need + rename collision columns)
# MAGIC r = (
# MAGIC     spark.table(f"{db}.payer_rules")
# MAGIC     .select(
# MAGIC         "payer", "program",
# MAGIC         "timely_filing_days", "pa_strictness", "appeal_success_bias"
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC n = (
# MAGIC     spark.table(f"{db}.ndc_reference")
# MAGIC     .select(
# MAGIC         "ndc11",
# MAGIC         F.col("therapeutic_class").alias("ndc_therapeutic_class"),
# MAGIC         F.col("manufacturer").alias("ndc_manufacturer")
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC p = (
# MAGIC     spark.table(f"{db}.plan_reference")
# MAGIC     .select("plan_id", "coverage_generosity")
# MAGIC )
# MAGIC
# MAGIC # ✅ Use string-based join keys so Spark doesn't duplicate payer/program/ndc11/plan_id
# MAGIC df = (
# MAGIC     d
# MAGIC     .join(r, on=["payer", "program"], how="left")
# MAGIC     .join(n, on=["ndc11"], how="left")
# MAGIC     .join(p, on=["plan_id"], how="left")
# MAGIC )
# MAGIC
# MAGIC features = (
# MAGIC     df
# MAGIC     .withColumn("delta_amount", F.col("expected_amount") - F.col("paid_amount"))
# MAGIC     .withColumn(
# MAGIC         "delta_pct",
# MAGIC         F.when(F.col("expected_amount") == 0, F.lit(0.0))
# MAGIC          .otherwise(F.abs(F.col("delta_amount")) / F.col("expected_amount"))
# MAGIC     )
# MAGIC     .withColumn("is_340b_flag", (F.col("channel") == "340B").cast("int"))
# MAGIC     .withColumn(
# MAGIC         "timely_flag",
# MAGIC         F.when(F.col("timely_filing_days").isNull(), F.lit(None))
# MAGIC          .otherwise((F.col("days_to_submit") <= F.col("timely_filing_days")).cast("int"))
# MAGIC     )
# MAGIC     .withColumn("notes_lc", F.lower(F.coalesce(F.col("dispute_notes"), F.lit(""))))
# MAGIC     .withColumn("rej_lc", F.lower(F.coalesce(F.col("rejection_code_text"), F.lit(""))))
# MAGIC     .withColumn("kw_340b", (F.col("notes_lc").contains("340b") | F.col("rej_lc").contains("340b")).cast("int"))
# MAGIC     .withColumn("kw_duplicate", (F.col("notes_lc").contains("duplicate") | F.col("rej_lc").contains("duplicate")).cast("int"))
# MAGIC     .withColumn("kw_timely", (F.col("notes_lc").contains("timely") | F.col("rej_lc").contains("timely")).cast("int"))
# MAGIC     .withColumn("kw_pa", (F.col("notes_lc").contains("prior auth") | F.col("rej_lc").contains("prior auth") | F.col("rej_lc").contains("pa ")).cast("int"))
# MAGIC     .withColumn("kw_ndc", (F.col("notes_lc").contains("ndc") | F.col("rej_lc").contains("ndc")).cast("int"))
# MAGIC     .withColumn("kw_eligibility", (F.col("notes_lc").contains("eligible") | F.col("rej_lc").contains("eligible")).cast("int"))
# MAGIC     .drop("notes_lc", "rej_lc")
# MAGIC )
# MAGIC
# MAGIC features.write.format("delta").mode("overwrite").saveAsTable(f"{db}.gold_features_disputes")
# MAGIC
# MAGIC spark.table(f"{db}.gold_features_disputes").groupBy("reason_code").count().orderBy(F.desc("count")).show(20, False)
# MAGIC spark.table(f"{db}.gold_features_disputes").select("delta_pct","is_340b_flag","timely_flag","kw_pa","kw_ndc").describe().show()
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC gold = spark.table(f"{db}.gold_features_disputes")
# MAGIC
# MAGIC # Row count + uniqueness
# MAGIC gold.select(
# MAGIC     F.count("*").alias("rows"),
# MAGIC     F.countDistinct("dispute_id").alias("distinct_dispute_id")
# MAGIC ).show()
# MAGIC
# MAGIC # Label distribution
# MAGIC gold.groupBy("reason_code").count().orderBy(F.desc("count")).show(50, False)
# MAGIC gold.groupBy("outcome").count().show()
# MAGIC
# MAGIC # Missing critical feature checks
# MAGIC gold.select(
# MAGIC     F.sum(F.col("delta_amount").isNull().cast("int")).alias("null_delta_amount"),
# MAGIC     F.sum(F.col("delta_pct").isNull().cast("int")).alias("null_delta_pct"),
# MAGIC     F.sum(F.col("is_340b_flag").isNull().cast("int")).alias("null_is_340b"),
# MAGIC     F.sum(F.col("timely_flag").isNull().cast("int")).alias("null_timely_flag")
# MAGIC ).show()
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC gold = spark.table(f"{db}.gold_features_disputes")
# MAGIC
# MAGIC # deterministic split based on hash of dispute_id
# MAGIC split = gold.withColumn(
# MAGIC     "split",
# MAGIC     F.when((F.abs(F.xxhash64("dispute_id")) % 100) < 80, F.lit("TRAIN")).otherwise(F.lit("TEST"))
# MAGIC )
# MAGIC
# MAGIC split.write.format("delta").mode("overwrite").saveAsTable(f"{db}.gold_features_split")
# MAGIC
# MAGIC train = spark.table(f"{db}.gold_features_split").filter("split = 'TRAIN'")
# MAGIC test  = spark.table(f"{db}.gold_features_split").filter("split = 'TEST'")
# MAGIC
# MAGIC train.write.format("delta").mode("overwrite").saveAsTable(f"{db}.train_disputes")
# MAGIC test.write.format("delta").mode("overwrite").saveAsTable(f"{db}.test_disputes")
# MAGIC
# MAGIC
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.metrics import f1_score, classification_report, confusion_matrix
# MAGIC from sklearn.preprocessing import OneHotEncoder
# MAGIC from sklearn.compose import ColumnTransformer
# MAGIC from sklearn.pipeline import Pipeline
# MAGIC from sklearn.impute import SimpleImputer
# MAGIC from sklearn.linear_model import LogisticRegression
# MAGIC #import mlflow
# MAGIC import databricks.connect as db_connect
# MAGIC import mlflow.tracking._model_registry.utils
# MAGIC
# MAGIC # Workaround to set the registry URI manually
# MAGIC mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"
# MAGIC
# MAGIC train_sdf = spark.table(f"{db}.train_disputes")
# MAGIC test_sdf  = spark.table(f"{db}.test_disputes")
# MAGIC
# MAGIC train_pd = train_sdf.toPandas()
# MAGIC test_pd  = test_sdf.toPandas()
# MAGIC
# MAGIC label_col = "reason_code"
# MAGIC
# MAGIC numeric_cols = ["delta_amount","delta_pct","is_340b_flag","timely_flag",
# MAGIC                 "kw_340b","kw_duplicate","kw_timely","kw_pa","kw_ndc","kw_eligibility",
# MAGIC                 "units","billed_amount","paid_amount","expected_amount","days_to_submit"]
# MAGIC
# MAGIC cat_cols = ["program","state","payer","channel","brand_generic","therapeutic_class","manufacturer"]
# MAGIC
# MAGIC X_train = train_pd[numeric_cols + cat_cols]
# MAGIC y_train = train_pd[label_col]
# MAGIC
# MAGIC X_test = test_pd[numeric_cols + cat_cols]
# MAGIC y_test = test_pd[label_col]
# MAGIC
# MAGIC numeric_tf = Pipeline(steps=[
# MAGIC     ("imputer", SimpleImputer(strategy="median"))
# MAGIC ])
# MAGIC
# MAGIC cat_tf = Pipeline(steps=[
# MAGIC     ("imputer", SimpleImputer(strategy="most_frequent")),
# MAGIC     ("ohe", OneHotEncoder(handle_unknown="ignore"))
# MAGIC ])
# MAGIC
# MAGIC preprocess = ColumnTransformer(
# MAGIC     transformers=[
# MAGIC         ("num", numeric_tf, numeric_cols),
# MAGIC         ("cat", cat_tf, cat_cols),
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC clf = LogisticRegression(max_iter=2000, n_jobs=-1, multi_class="auto")
# MAGIC
# MAGIC pipe = Pipeline(steps=[("preprocess", preprocess),
# MAGIC                       ("model", clf)])
# MAGIC
# MAGIC with mlflow.start_run(run_name="reason_classifier_lr"):
# MAGIC     pipe.fit(X_train, y_train)
# MAGIC     pred = pipe.predict(X_test)
# MAGIC
# MAGIC     f1 = f1_score(y_test, pred, average="macro")
# MAGIC     mlflow.log_metric("f1_macro", float(f1))
# MAGIC
# MAGIC     # log classification report as text
# MAGIC     report = classification_report(y_test, pred)
# MAGIC     mlflow.log_text(report, "classification_report.txt")
# MAGIC
# MAGIC     # log confusion matrix as CSV
# MAGIC     labels = sorted(pd.unique(y_test))
# MAGIC     cm = confusion_matrix(y_test, pred, labels=labels)
# MAGIC     cm_df = pd.DataFrame(cm, index=labels, columns=labels)
# MAGIC     mlflow.log_text(cm_df.to_csv(index=True), "confusion_matrix.csv")
# MAGIC
# MAGIC     mlflow.sklearn.log_model(pipe, "model")
# MAGIC
# MAGIC
# MAGIC import pandas as pd
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC
# MAGIC from sklearn.metrics import f1_score, classification_report, confusion_matrix
# MAGIC from sklearn.preprocessing import OneHotEncoder, StandardScaler
# MAGIC from sklearn.compose import ColumnTransformer
# MAGIC from sklearn.pipeline import Pipeline
# MAGIC from sklearn.impute import SimpleImputer
# MAGIC from sklearn.linear_model import LogisticRegression
# MAGIC from mlflow.models.signature import infer_signature
# MAGIC
# MAGIC import databricks.connect as db_connect
# MAGIC import mlflow.tracking._model_registry.utils
# MAGIC
# MAGIC # Workaround to set the registry URI manually
# MAGIC mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"
# MAGIC
# MAGIC train_pd = spark.table(f"{db}.train_disputes").toPandas()
# MAGIC test_pd  = spark.table(f"{db}.test_disputes").toPandas()
# MAGIC
# MAGIC label_col = "reason_code"
# MAGIC
# MAGIC numeric_cols = ["delta_amount","delta_pct","is_340b_flag","timely_flag",
# MAGIC                 "kw_340b","kw_duplicate","kw_timely","kw_pa","kw_ndc","kw_eligibility",
# MAGIC                 "units","billed_amount","paid_amount","expected_amount","days_to_submit"]
# MAGIC
# MAGIC cat_cols = ["program","state","payer","channel","brand_generic","ndc_therapeutic_class","ndc_manufacturer"]
# MAGIC
# MAGIC X_train = train_pd[numeric_cols + cat_cols]
# MAGIC y_train = train_pd[label_col]
# MAGIC
# MAGIC X_test = test_pd[numeric_cols + cat_cols]
# MAGIC y_test = test_pd[label_col]
# MAGIC
# MAGIC numeric_tf = Pipeline(steps=[
# MAGIC     ("imputer", SimpleImputer(strategy="median")),
# MAGIC     ("scaler", StandardScaler())
# MAGIC ])
# MAGIC
# MAGIC cat_tf = Pipeline(steps=[
# MAGIC     ("imputer", SimpleImputer(strategy="most_frequent")),
# MAGIC     ("ohe", OneHotEncoder(handle_unknown="ignore"))
# MAGIC ])
# MAGIC
# MAGIC preprocess = ColumnTransformer(
# MAGIC     transformers=[
# MAGIC         ("num", numeric_tf, numeric_cols),
# MAGIC         ("cat", cat_tf, cat_cols),
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC clf = LogisticRegression(
# MAGIC     solver="saga",
# MAGIC     max_iter=8000,
# MAGIC     n_jobs=-1,
# MAGIC     class_weight="balanced"
# MAGIC )
# MAGIC
# MAGIC pipe = Pipeline(steps=[("preprocess", preprocess),
# MAGIC                       ("model", clf)])
# MAGIC
# MAGIC with mlflow.start_run(run_name="reason_classifier_lr_saga"):
# MAGIC     pipe.fit(X_train, y_train)
# MAGIC     pred = pipe.predict(X_test)
# MAGIC
# MAGIC     f1 = f1_score(y_test, pred, average="macro")
# MAGIC     mlflow.log_metric("f1_macro", float(f1))
# MAGIC
# MAGIC     report = classification_report(y_test, pred, zero_division=0)
# MAGIC     mlflow.log_text(report, "classification_report.txt")
# MAGIC
# MAGIC     labels = sorted(pd.unique(y_test))
# MAGIC     cm = confusion_matrix(y_test, pred, labels=labels)
# MAGIC     cm_df = pd.DataFrame(cm, index=labels, columns=labels)
# MAGIC     mlflow.log_text(cm_df.to_csv(index=True), "confusion_matrix.csv")
# MAGIC
# MAGIC     # MLflow signature (removes signature warning)
# MAGIC     X_sample = X_train.head(50)
# MAGIC     sig = infer_signature(X_sample, pipe.predict(X_sample))
# MAGIC
# MAGIC     mlflow.sklearn.log_model(
# MAGIC         pipe, "model",
# MAGIC         signature=sig,
# MAGIC         input_example=X_sample
# MAGIC     )
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC gold = spark.table(f"{db}.gold_features_disputes")
# MAGIC
# MAGIC # Normalize to uppercase just in case
# MAGIC gold = gold.withColumn("reason_code", F.upper(F.trim(F.col("reason_code"))))
# MAGIC
# MAGIC v2 = (
# MAGIC     gold
# MAGIC     .withColumn(
# MAGIC         "reason_code_v2",
# MAGIC         F.when(F.col("reason_code").isin(
# MAGIC             "TIMELY_FILING",
# MAGIC             "DUPLICATE_DISCOUNT_340B",
# MAGIC             "PRIOR_AUTH",
# MAGIC             "ELIGIBILITY",
# MAGIC             "NDC_MAPPING",
# MAGIC             "PLAN_EXCLUSION",
# MAGIC             "STEP_THERAPY"
# MAGIC         ), F.col("reason_code"))
# MAGIC         .otherwise(F.lit("PRICING_ADMIN_OTHER"))
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC v2.write.format("delta").mode("overwrite").saveAsTable(f"{db}.gold_features_disputes_v2")
# MAGIC
# MAGIC spark.table(f"{db}.gold_features_disputes_v2") \
# MAGIC   .groupBy("reason_code_v2") \
# MAGIC   .count() \
# MAGIC   .orderBy(F.desc("count")) \
# MAGIC   .show(50, False)
# MAGIC
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC v2 = spark.table(f"{db}.gold_features_disputes_v2")
# MAGIC
# MAGIC split = v2.withColumn(
# MAGIC     "split",
# MAGIC     F.when((F.abs(F.xxhash64("dispute_id")) % 100) < 80, F.lit("TRAIN")).otherwise(F.lit("TEST"))
# MAGIC )
# MAGIC
# MAGIC split.write.format("delta").mode("overwrite").saveAsTable(f"{db}.gold_features_split_v2")
# MAGIC
# MAGIC split.filter("split='TRAIN'").write.format("delta").mode("overwrite").saveAsTable(f"{db}.train_disputes_v2")
# MAGIC split.filter("split='TEST'").write.format("delta").mode("overwrite").saveAsTable(f"{db}.test_disputes_v2")
# MAGIC
# MAGIC import pandas as pd
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC
# MAGIC from sklearn.metrics import f1_score, classification_report, confusion_matrix
# MAGIC from sklearn.preprocessing import OneHotEncoder, StandardScaler
# MAGIC from sklearn.compose import ColumnTransformer
# MAGIC from sklearn.pipeline import Pipeline
# MAGIC from sklearn.impute import SimpleImputer
# MAGIC from sklearn.linear_model import LogisticRegression
# MAGIC from mlflow.models.signature import infer_signature
# MAGIC
# MAGIC import databricks.connect as db_connect
# MAGIC import mlflow.tracking._model_registry.utils
# MAGIC
# MAGIC # Workaround to set the registry URI manually
# MAGIC mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"
# MAGIC
# MAGIC train_pd = spark.table(f"{db}.train_disputes_v2").toPandas()
# MAGIC test_pd  = spark.table(f"{db}.test_disputes_v2").toPandas()
# MAGIC
# MAGIC label_col = "reason_code_v2"
# MAGIC
# MAGIC numeric_cols = ["delta_amount","delta_pct","is_340b_flag","timely_flag",
# MAGIC                 "kw_340b","kw_duplicate","kw_timely","kw_pa","kw_ndc","kw_eligibility",
# MAGIC                 "units","billed_amount","paid_amount","expected_amount","days_to_submit"]
# MAGIC
# MAGIC cat_cols = ["program","state","payer","channel","brand_generic","ndc_therapeutic_class","ndc_manufacturer"]
# MAGIC
# MAGIC X_train = train_pd[numeric_cols + cat_cols]
# MAGIC y_train = train_pd[label_col]
# MAGIC
# MAGIC X_test = test_pd[numeric_cols + cat_cols]
# MAGIC y_test = test_pd[label_col]
# MAGIC
# MAGIC numeric_tf = Pipeline(steps=[
# MAGIC     ("imputer", SimpleImputer(strategy="median")),
# MAGIC     ("scaler", StandardScaler())
# MAGIC ])
# MAGIC
# MAGIC cat_tf = Pipeline(steps=[
# MAGIC     ("imputer", SimpleImputer(strategy="most_frequent")),
# MAGIC     ("ohe", OneHotEncoder(handle_unknown="ignore"))
# MAGIC ])
# MAGIC
# MAGIC preprocess = ColumnTransformer(
# MAGIC     transformers=[
# MAGIC         ("num", numeric_tf, numeric_cols),
# MAGIC         ("cat", cat_tf, cat_cols),
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC clf = LogisticRegression(
# MAGIC     solver="saga",
# MAGIC     max_iter=8000,
# MAGIC     n_jobs=-1,
# MAGIC     class_weight="balanced"
# MAGIC )
# MAGIC
# MAGIC pipe = Pipeline(steps=[("preprocess", preprocess),
# MAGIC                       ("model", clf)])
# MAGIC
# MAGIC with mlflow.start_run(run_name="reason_classifier_lr_after_collapse"):
# MAGIC     pipe.fit(X_train, y_train)
# MAGIC     pred = pipe.predict(X_test)
# MAGIC
# MAGIC     f1 = f1_score(y_test, pred, average="macro")
# MAGIC     mlflow.log_metric("f1_macro", float(f1))
# MAGIC
# MAGIC     report = classification_report(y_test, pred, zero_division=0)
# MAGIC     mlflow.log_text(report, "classification_report.txt")
# MAGIC
# MAGIC     labels = sorted(pd.unique(y_test))
# MAGIC     cm = confusion_matrix(y_test, pred, labels=labels)
# MAGIC     cm_df = pd.DataFrame(cm, index=labels, columns=labels)
# MAGIC     mlflow.log_text(cm_df.to_csv(index=True), "confusion_matrix.csv")
# MAGIC
# MAGIC     # MLflow signature (removes signature warning)
# MAGIC     X_sample = X_train.head(50)
# MAGIC     sig = infer_signature(X_sample, pipe.predict(X_sample))
# MAGIC
# MAGIC     mlflow.sklearn.log_model(
# MAGIC         pipe, "model",
# MAGIC         signature=sig,
# MAGIC         input_example=X_sample
# MAGIC     )
# MAGIC
# MAGIC import pandas as pd
# MAGIC from datetime import datetime
# MAGIC from mlflow.tracking import MlflowClient
# MAGIC
# MAGIC import databricks.connect as db_connect
# MAGIC import mlflow.tracking._model_registry.utils
# MAGIC
# MAGIC import mlflow
# MAGIC
# MAGIC
# MAGIC # Workaround to set the registry URI manually
# MAGIC mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"
# MAGIC
# MAGIC client = MlflowClient()
# MAGIC # Get experiment_id from the active notebook context
# MAGIC active_run = mlflow.active_run()
# MAGIC
# MAGIC if active_run is None:
# MAGIC     raise RuntimeError("No active MLflow run found. Run this in a notebook with MLflow runs.")
# MAGIC
# MAGIC experiment_id = active_run.info.experiment_id
# MAGIC print("Experiment ID:", experiment_id)
# MAGIC
# MAGIC runs = client.search_runs(
# MAGIC     experiment_ids=[experiment_id],
# MAGIC     order_by=["start_time DESC"],
# MAGIC     max_results=5
# MAGIC )
# MAGIC
# MAGIC rows = []
# MAGIC for r in runs:
# MAGIC     rows.append({
# MAGIC         "run_id": r.info.run_id,
# MAGIC         "run_name": r.data.tags.get("mlflow.runName"),
# MAGIC         "f1_macro": r.data.metrics.get("f1_macro"),
# MAGIC         "start_time": datetime.fromtimestamp(r.info.start_time / 1000)
# MAGIC     })
# MAGIC
# MAGIC metrics_df = spark.createDataFrame(pd.DataFrame(rows))
# MAGIC metrics_df.write.format("delta").mode("overwrite").saveAsTable(
# MAGIC     f"{db}.model_metrics_reason_classifier"
# MAGIC )
# MAGIC
# MAGIC spark.table(f"{db}.model_metrics_reason_classifier").show(truncate=False)

# COMMAND ----------

# Batch scoring for LR + XGBoost using MLflow run URIs
# Output table: dispute_ai.predictions_disputes_scored

import pandas as pd
import numpy as np
from datetime import datetime, timezone

import mlflow
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
import mlflow.tracking._model_registry.utils

# Workaround to set the registry URI manually
mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"


# -----------------------------
# CONFIG
# -----------------------------
db = "dispute_ai"

source_table = f"{db}.gold_features_disputes_v2"
out_table    = f"{db}.predictions_disputes_scored"

# ✅ Your actual MLflow run IDs
LR_RUN_ID  = "a092e5dfaa5d4a01b5012b5a97b78656"
XGB_RUN_ID = "bd1a54cbdb8c4aad892b8db92bc5a21a"

# Feature columns (must match training)
numeric_cols = [
    "delta_amount","delta_pct","is_340b_flag","timely_flag",
    "kw_340b","kw_duplicate","kw_timely","kw_pa","kw_ndc","kw_eligibility",
    "units","billed_amount","paid_amount","expected_amount","days_to_submit"
]

cat_cols = [
    "program","state","payer","channel",
    "brand_generic","ndc_therapeutic_class","ndc_manufacturer"
]

feature_cols = numeric_cols + cat_cols
id_cols = ["dispute_id"]

# -----------------------------
# 1) LOAD DATA
# -----------------------------
sdf = spark.table(source_table)

missing = [c for c in (id_cols + feature_cols) if c not in sdf.columns]
if missing:
    raise ValueError(f"Missing columns in {source_table}: {missing}")

pdf = sdf.select(*(id_cols + feature_cols)).toPandas()
X = pdf[feature_cols]

# -----------------------------
# 2) LOAD MODELS
# -----------------------------
lr_uri  = f"runs:/{LR_RUN_ID}/model"
xgb_uri = f"runs:/{XGB_RUN_ID}/model"

lr_model  = mlflow.pyfunc.load_model(lr_uri)
xgb_model = mlflow.pyfunc.load_model(xgb_uri)

# -----------------------------
# 3) SCORE LOGISTIC REGRESSION
# -----------------------------
# Predictions (string labels)
pdf["pred_reason_lr"] = lr_model.predict(X)

# Confidence (max probability if available)
try:
    lr_proba = lr_model._model_impl.predict_proba(X)
    pdf["conf_lr"] = lr_proba.max(axis=1)
except Exception:
    pdf["conf_lr"] = np.nan

# -----------------------------
# 4) SCORE XGBOOST
# -----------------------------
# Numeric predictions (0..K-1)
xgb_pred_num = xgb_model.predict(X).astype(int)

# Confidence = max probability
xgb_proba = xgb_model._model_impl.predict_proba(X)
pdf["conf_xgb"] = xgb_proba.max(axis=1)

# Decode numeric classes → labels using MLflow artifact
client = MlflowClient()
mapping_path = client.download_artifacts(XGB_RUN_ID, "label_mapping.csv")
mapping_df = pd.read_csv(mapping_path)

idx_to_label = dict(
    zip(mapping_df["class_index"].astype(int),
        mapping_df["class_name"].astype(str))
)

pdf["pred_reason_xgb"] = [idx_to_label.get(i, "UNKNOWN") for i in xgb_pred_num]

# -----------------------------
# 5) METADATA + WRITE DELTA
# -----------------------------
pdf["scored_at_utc"] = datetime.now(timezone.utc).isoformat()
pdf["lr_model_uri"]  = lr_uri
pdf["xgb_model_uri"] = xgb_uri

out_sdf = spark.createDataFrame(pdf)

out_sdf.write.format("delta").mode("overwrite").saveAsTable(out_table)

print(f"✅ Batch scoring complete. Results saved to {out_table}")

# Quick sanity preview
spark.table(out_table) \
    .select(
        "dispute_id",
        "pred_reason_lr","conf_lr",
        "pred_reason_xgb","conf_xgb",
        "scored_at_utc"
    ) \
    .orderBy(F.desc("scored_at_utc")) \
    .show(20, truncate=False)


# COMMAND ----------

from pyspark.sql import functions as F

db = "dispute_ai"
table_in  = f"{db}.predictions_disputes_scored"
table_out = f"{db}.predictions_disputes_scored_routed"

df = spark.table(table_in)

# --- Default routing thresholds (tune as you like) ---
# conf >= 0.75  -> AUTO_ROUTE
# conf >= 0.50  -> REVIEW
# else          -> REQUEST_INFO

df_routed = (
    df
    .withColumn(
        "route_decision",
        F.when(F.col("conf_xgb").isNull(), F.lit("REQUEST_INFO"))
         .when(F.col("conf_xgb") >= F.lit(0.75), F.lit("AUTO_ROUTE"))
         .when(F.col("conf_xgb") >= F.lit(0.50), F.lit("REVIEW"))
         .otherwise(F.lit("REQUEST_INFO"))
    )
    .withColumn(
        "route_reason",
        F.when(F.col("conf_xgb").isNull(), F.lit("missing_confidence"))
         .when(F.col("conf_xgb") >= F.lit(0.75), F.lit("high_confidence"))
         .when(F.col("conf_xgb") >= F.lit(0.50), F.lit("medium_confidence"))
         .otherwise(F.lit("low_confidence"))
    )
    .withColumn("routed_at_utc", F.current_timestamp())
)

df_routed.write.format("delta").mode("overwrite").saveAsTable(table_out)

print(f"✅ Wrote routed table: {table_out}")

# Quick distribution check
spark.table(table_out) \
  .groupBy("route_decision") \
  .count() \
  .orderBy(F.desc("count")) \
  .show()


# COMMAND ----------

import pandas as pd
from datetime import datetime, timezone
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

db = "dispute_ai"
out_table = f"{db}.mlflow_run_metrics"

target_run_names = [
    "reason_classifier_lr_saga",
    "reason_classifier_lr_after_collapse",
    "reason_classifier_xgboost_v2"
]

client = MlflowClient()
experiments = client.search_experiments(max_results=10000)

rows = []
seen = set()

for exp in experiments:
    exp_id = exp.experiment_id
    runs = client.search_runs(
        experiment_ids=[exp_id],
        order_by=["attributes.start_time DESC"],
        max_results=500
    )

    for r in runs:
        run_name = r.data.tags.get("mlflow.runName") or r.data.tags.get("mlflow.run_name")
        if run_name in target_run_names:
            key = (r.info.run_id, run_name)
            if key in seen:
                continue
            seen.add(key)

            start_ts = r.info.start_time
            end_ts = r.info.end_time

            rows.append({
                "experiment_id": exp_id,
                "experiment_name": exp.name,
                "run_id": r.info.run_id,
                "run_name": run_name,
                "status": r.info.status,
                "start_time_utc": datetime.fromtimestamp(start_ts/1000, tz=timezone.utc).isoformat() if start_ts else None,
                "end_time_utc": datetime.fromtimestamp(end_ts/1000, tz=timezone.utc).isoformat() if end_ts else None,

                # ✅ always include these columns (None if missing)
                "f1_macro": float(r.data.metrics.get("f1_macro")) if r.data.metrics.get("f1_macro") is not None else None,
                "roc_auc": float(r.data.metrics.get("roc_auc")) if r.data.metrics.get("roc_auc") is not None else None,
                "avg_precision": float(r.data.metrics.get("avg_precision")) if r.data.metrics.get("avg_precision") is not None else None,

                "params": str(dict(r.data.params)),
                "tags": str(dict(r.data.tags)),
            })

if not rows:
    raise RuntimeError("No matching runs found. Check target_run_names match the MLflow UI run names exactly.")

pdf = pd.DataFrame(rows)

# keep latest per run_name (optional)
pdf = pdf.sort_values(["run_name", "start_time_utc"], ascending=[True, False])
pdf_latest = pdf.drop_duplicates(subset=["run_name"], keep="first")

sdf = spark.createDataFrame(pdf_latest)

# ✅ write stable schema
sdf.write.format("delta").mode("overwrite").saveAsTable(out_table)

# ✅ safe display
#spark.table(out_table) \
#    .select("run_name","experiment_id","run_id","status","f1_macro","roc_auc","avg_precision","start_time_utc") \
#    .orderBy(F.desc("start_time_utc")) \
#    .show(truncate=False)

#print(f"Saved: {out_table}")




# COMMAND ----------

# XGBoost multi-class reason classifier (collapsed labels) + MLflow logging
# Assumes you already have: {db}.train_disputes_v2 and {db}.test_disputes_v2 Delta tables

# XGBoost multi-class reason classifier with string labels -> numeric encoding + MLflow logging
# Requires: %pip install xgboost  (then restart Python)
import pandas as pd
import numpy as np

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

from xgboost import XGBClassifier

from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import f1_score, classification_report, confusion_matrix

import databricks.connect as db_connect
import mlflow.tracking._model_registry.utils

# Workaround to set the registry URI manually
mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"


# -----------------------------
# 0) Configure schema/database
# -----------------------------
db = "dispute_ai"  # <-- change if needed

# -----------------------------
# 1) Load train/test (v2 labels)
# -----------------------------
train_pd = spark.table(f"{db}.train_disputes_v2").toPandas()
test_pd  = spark.table(f"{db}.test_disputes_v2").toPandas()

label_col = "reason_code_v2"

numeric_cols = [
    "delta_amount","delta_pct","is_340b_flag","timely_flag",
    "kw_340b","kw_duplicate","kw_timely","kw_pa","kw_ndc","kw_eligibility",
    "units","billed_amount","paid_amount","expected_amount","days_to_submit"
]

cat_cols = [
    "program","state","payer","channel",
    "brand_generic","ndc_therapeutic_class","ndc_manufacturer"
]

required = set([label_col] + numeric_cols + cat_cols)
missing_train = sorted(list(required - set(train_pd.columns)))
missing_test  = sorted(list(required - set(test_pd.columns)))
if missing_train:
    raise ValueError(f"Missing columns in TRAIN: {missing_train}")
if missing_test:
    raise ValueError(f"Missing columns in TEST: {missing_test}")

X_train = train_pd[numeric_cols + cat_cols]
y_train_raw = train_pd[label_col].astype(str)

X_test  = test_pd[numeric_cols + cat_cols]
y_test_raw  = test_pd[label_col].astype(str)

# -----------------------------
# 2) Encode labels to 0..K-1
# -----------------------------
le = LabelEncoder()
y_train = le.fit_transform(y_train_raw)
y_test  = le.transform(y_test_raw)
class_names = list(le.classes_)
n_classes = len(class_names)

# -----------------------------------------
# 3) Preprocess (impute + one-hot encoding)
# -----------------------------------------
numeric_tf = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median"))
])

cat_tf = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("ohe", OneHotEncoder(handle_unknown="ignore"))
])

preprocess = ColumnTransformer(
    transformers=[
        ("num", numeric_tf, numeric_cols),
        ("cat", cat_tf, cat_cols),
    ]
)

# -----------------------------
# 4) XGBoost model (multi-class)
# -----------------------------
xgb = XGBClassifier(
    objective="multi:softprob",
    num_class=n_classes,
    max_depth=8,
    n_estimators=400,
    learning_rate=0.08,
    subsample=0.8,
    colsample_bytree=0.8,
    eval_metric="mlogloss",
    tree_method="hist",
    random_state=42
)

# Train the preprocessing + model pipeline
pipe = Pipeline(steps=[
    ("preprocess", preprocess),
    ("model", xgb)
])

# --------------------------------
# 5) Train + evaluate + log MLflow
# --------------------------------
with mlflow.start_run(run_name="reason_classifier_xgboost_v2"):
    # params
    mlflow.log_param("label_col", label_col)
    mlflow.log_param("classes", ",".join(class_names))
    mlflow.log_param("n_classes", n_classes)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("n_estimators", 400)
    mlflow.log_param("learning_rate", 0.08)
    mlflow.log_param("subsample", 0.8)
    mlflow.log_param("colsample_bytree", 0.8)
    mlflow.log_param("tree_method", "hist")

    # fit
    pipe.fit(X_train, y_train)

    # predict numeric classes
    pred_num = pipe.predict(X_test).astype(int)

    # convert predictions back to string labels for reporting
    pred_lbl = le.inverse_transform(pred_num)

    # metrics (use string labels for human-readable reports)
    f1 = f1_score(y_test_raw, pred_lbl, average="macro")
    mlflow.log_metric("f1_macro", float(f1))

    report = classification_report(y_test_raw, pred_lbl, zero_division=0)
    mlflow.log_text(report, "classification_report.txt")

    labels = class_names
    cm = confusion_matrix(y_test_raw, pred_lbl, labels=labels)
    cm_df = pd.DataFrame(cm, index=labels, columns=labels)
    mlflow.log_text(cm_df.to_csv(index=True), "confusion_matrix.csv")

    # Log label encoder mapping as artifact (very useful for later)
    mapping_df = pd.DataFrame({"class_index": list(range(n_classes)), "class_name": class_names})
    mlflow.log_text(mapping_df.to_csv(index=False), "label_mapping.csv")

    # Model signature (input example + numeric output)
    X_sample = X_train.head(50)
    y_sample = pipe.predict(X_sample)
    sig = infer_signature(X_sample, y_sample)

    mlflow.sklearn.log_model(
        pipe,
        "model",
        signature=sig,
        input_example=X_sample
    )

print("Done. Check MLflow run 'reason_classifier_xgboost_v2' for f1_macro + artifacts.")



# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select * from dispute_ai.mlflow_run_metrics;
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %sql
# MAGIC select experiment_id, experiment_name, run_id, run_name, status, start_time_utc, end_time_utc, f1_macro, params, tags from dispute_ai.mlflow_run_metrics;

# COMMAND ----------

from pyspark.sql import functions as F

db = "dispute_ai"

# choose routed if exists, else scored (serverless-safe)
tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {db}").collect()]
source = f"{db}.predictions_disputes_scored_routed" if "predictions_disputes_scored_routed" in tables else f"{db}.predictions_disputes_scored"

s = spark.table(source).alias("s")

# Only select business context columns from gold (avoid bringing delta_amount at all)
g = spark.table(f"{db}.gold_features_disputes_v2").select(
    "dispute_id",
    "payer", "program", "state", "channel",
    "ndc11",
    "expected_amount", "paid_amount",
    "delta_pct",
    "days_to_submit", "timely_flag"
).alias("g")

df = s.join(g, on="dispute_id", how="left")

queue = (
    df
    # ✅ explicit business amounts (no ambiguity)
    .withColumn("recoverable_amount", (F.col("g.expected_amount") - F.col("g.paid_amount")).cast("double"))
    .withColumn("dispute_variance_amount", F.abs(F.col("recoverable_amount")).cast("double"))
    .withColumn(
        "variance_direction",
        F.when(F.col("recoverable_amount") > 0, F.lit("UNDERPAID_RECOVER"))
         .when(F.col("recoverable_amount") < 0, F.lit("OVERPAID_VERIFY"))
         .otherwise(F.lit("MATCH"))
    )
    # Priority uses magnitude + confidence (no delta_amount)
    .withColumn(
        "priority",
        F.when((F.col("s.conf_xgb") >= 0.85) & (F.col("dispute_variance_amount") >= 5000), F.lit("P1"))
         .when((F.col("s.conf_xgb") >= 0.75) & (F.col("dispute_variance_amount") >= 1000), F.lit("P2"))
         .otherwise(F.lit("P3"))
    )
    .withColumn(
        "sla_days",
        F.when(F.col("priority") == "P1", F.lit(2))
         .when(F.col("priority") == "P2", F.lit(5))
         .otherwise(F.lit(10))
    )
    .withColumn(
        "owner_queue",
        F.when(F.col("s.pred_reason_xgb") == "DUPLICATE_DISCOUNT_340B", F.lit("340B Team"))
         .when(F.col("s.pred_reason_xgb") == "TIMELY_FILING", F.lit("Timely Filing Team"))
         .when(F.col("s.pred_reason_xgb") == "PRIOR_AUTH", F.lit("PA Team"))
         .when(F.col("s.pred_reason_xgb").isin("PLAN_EXCLUSION","STEP_THERAPY","ELIGIBILITY","NDC_MAPPING"), F.lit("Policy Ops Team"))
         .otherwise(F.lit("Pricing/Admin Team"))
    )
    .withColumn("work_status", F.lit("OPEN"))
    .withColumn("created_at", F.current_timestamp())
    .select(
        "dispute_id",
        "work_status", "owner_queue", "priority", "sla_days",
        F.col("s.pred_reason_xgb").alias("pred_reason_xgb"),
        F.col("s.conf_xgb").alias("conf_xgb"),
        F.col("s.pred_reason_lr").alias("pred_reason_lr"),
        F.col("s.conf_lr").alias("conf_lr"),
        F.col("g.payer").alias("payer"),
        F.col("g.program").alias("program"),
        F.col("g.state").alias("state"),
        F.col("g.channel").alias("channel"),
        F.col("g.ndc11").alias("ndc11"),
        F.col("g.expected_amount").alias("expected_amount"),
        F.col("g.paid_amount").alias("paid_amount"),
        "recoverable_amount",
        "dispute_variance_amount",
        "variance_direction",
        F.col("g.delta_pct").alias("delta_pct"),
        F.col("g.days_to_submit").alias("days_to_submit"),
        F.col("g.timely_flag").alias("timely_flag"),
        "created_at"
    )
)

queue.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_work_queue")

print(f"✅ Created/updated: {db}.dispute_work_queue")

spark.table(f"{db}.dispute_work_queue") \
  .groupBy("priority","owner_queue") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(50, truncate=False)



# COMMAND ----------

from pyspark.sql import functions as F

db = "dispute_ai"
q = spark.table(f"{db}.dispute_work_queue")

# Queue summary by team/priority/reason
kpi_queue = (
    q.groupBy("owner_queue", "priority", "pred_reason_xgb")
     .agg(
         F.count("*").alias("open_cases"),
         F.avg("conf_xgb").alias("avg_confidence"),
         F.sum(F.col("dispute_variance_amount")).alias("total_variance_amount"),
         F.sum(F.when(F.col("recoverable_amount") > 0, F.col("recoverable_amount")).otherwise(F.lit(0.0))).alias("total_recoverable_amount")
     )
     .orderBy(F.desc("total_recoverable_amount"))
)

kpi_queue.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_queue_kpis")
print(f"✅ Created: {db}.dispute_queue_kpis")

kpi_queue.show(50, truncate=False)


# COMMAND ----------

kpi_payers = (
    q.groupBy("payer")
     .agg(
         F.count("*").alias("open_cases"),
         F.sum(F.col("dispute_variance_amount")).alias("total_variance_amount"),
         F.sum(F.when(F.col("recoverable_amount") > 0, F.col("recoverable_amount")).otherwise(F.lit(0.0))).alias("total_recoverable_amount")
     )
     .orderBy(F.desc("total_recoverable_amount"))
)

kpi_payers.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_queue_kpis_payer")
print(f"✅ Created: {db}.dispute_queue_kpis_payer")

kpi_payers.show(20, truncate=False)


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {db}.dispute_work_events (
  dispute_id STRING,
  event_type STRING,          -- CREATED / ASSIGNED / STATUS_CHANGE / COMMENT
  old_status STRING,
  new_status STRING,
  actor STRING,
  notes STRING,
  event_ts TIMESTAMP
) USING DELTA
""")

print(f"✅ Ready: {db}.dispute_work_events")


# COMMAND ----------

from pyspark.sql import functions as F

db = "dispute_ai"
q = spark.table(f"{db}.dispute_work_queue").select("dispute_id","work_status","created_at")

events = (
    q.withColumn("event_type", F.lit("CREATED"))
     .withColumn("old_status", F.lit(None).cast("string"))
     .withColumn("new_status", F.col("work_status"))
     .withColumn("actor", F.lit("system"))
     .withColumn("notes", F.lit("Work queue item created from batch scoring"))
     .withColumn("event_ts", F.col("created_at"))
     .select("dispute_id","event_type","old_status","new_status","actor","notes","event_ts")
)

events.write.format("delta").mode("append").saveAsTable(f"{db}.dispute_work_events")
print(f"✅ Seeded CREATED events into {db}.dispute_work_events")


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

db = "dispute_ai"

# Base tables
q = spark.table(f"{db}.dispute_work_queue").alias("q")
g = spark.table(f"{db}.gold_features_disputes_v2").alias("g")

# Optional reference tables (if present)
tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {db}").collect()]

payer_rules = spark.table(f"{db}.payer_rules").select(
    "payer","program","timely_filing_days"
).alias("r") if "payer_rules" in tables else None

plan_ref = spark.table(f"{db}.plan_reference").select(
    "plan_id","coverage_generosity"
).alias("p") if "plan_reference" in tables else None

ndc_ref = spark.table(f"{db}.ndc_reference").select(
    "ndc11","therapeutic_class","manufacturer"
).alias("n") if "ndc_reference" in tables else None

# Join queue back to gold features for richer evidence signals
# (keep only what we need to avoid column ambiguity)
base = (
    q.select(
        "dispute_id",
        "pred_reason_xgb","conf_xgb",
        "payer","program","state","channel","ndc11",
        "expected_amount","paid_amount",
        "recoverable_amount","dispute_variance_amount",
        "days_to_submit","timely_flag",
        "delta_pct"
    )
    .join(
        g.select(
            "dispute_id",
            "plan_id",
            "kw_340b","kw_duplicate","kw_timely","kw_pa","kw_ndc","kw_eligibility",
            "is_340b_flag"
        ),
        on="dispute_id",
        how="left"
    )
)

if payer_rules is not None:
    base = base.join(payer_rules, on=["payer","program"], how="left")

if plan_ref is not None:
    base = base.join(plan_ref, on=["plan_id"], how="left")

if ndc_ref is not None:
    base = base.join(ndc_ref, on=["ndc11"], how="left")


# ------------------------------------------------------------
# Evidence generation: create candidate evidence rows (union)
# Each evidence row has:
#   dispute_id, evidence_type, evidence_text, evidence_score
# ------------------------------------------------------------

# Helper to standardize columns
def ev(df, evidence_type, evidence_text_col, evidence_score_col):
    return df.select(
        "dispute_id",
        F.lit(evidence_type).alias("evidence_type"),
        evidence_text_col.alias("evidence_text"),
        evidence_score_col.cast("double").alias("evidence_score"),
        F.current_timestamp().alias("evidence_ts")
    )

evidence_dfs = []

# 1) High confidence signal (always useful)
evidence_dfs.append(
    ev(
        base,
        "MODEL_CONFIDENCE",
        F.concat(F.lit("Model confidence is "), F.format_number(F.col("conf_xgb"), 3),
                 F.lit(" for "), F.col("pred_reason_xgb")),
        (F.col("conf_xgb") * 100)
    )
)

# 2) Amount variance signal (business)
evidence_dfs.append(
    ev(
        base,
        "AMOUNT_VARIANCE",
        F.concat(
            F.lit("Variance amount = $"),
            F.format_number(F.col("dispute_variance_amount"), 2),
            F.lit("; recoverable = $"),
            F.format_number(F.col("recoverable_amount"), 2)
        ),
        F.when(F.col("dispute_variance_amount") >= 5000, F.lit(90))
         .when(F.col("dispute_variance_amount") >= 1000, F.lit(70))
         .otherwise(F.lit(40))
    )
)

# 3) Timely filing evidence (if we have payer rule)
if payer_rules is not None:
    evidence_dfs.append(
        ev(
            base,
            "TIMELY_FILING_RULE",
            F.when(
                F.col("timely_filing_days").isNotNull(),
                F.concat(
                    F.lit("Payer timely filing limit is "),
                    F.col("timely_filing_days").cast("string"),
                    F.lit(" days; submitted in "),
                    F.col("days_to_submit").cast("string"),
                    F.lit(" days.")
                )
            ).otherwise(F.lit(None)),
            F.when(
                (F.col("timely_filing_days").isNotNull()) & (F.col("days_to_submit") > F.col("timely_filing_days")),
                F.lit(95)
            ).when(
                (F.col("timely_filing_days").isNotNull()) & (F.col("days_to_submit") <= F.col("timely_filing_days")),
                F.lit(60)
            ).otherwise(F.lit(None))
        ).filter(F.col("evidence_text").isNotNull())
    )

# 4) Keyword evidence flags (works even without notes table)
kw_map = [
    ("KW_340B", "Keywords indicate 340B / duplicate discount context.", "kw_340b", 90),
    ("KW_DUPLICATE", "Keywords indicate duplicate / duplicate discount.", "kw_duplicate", 85),
    ("KW_TIMELY", "Keywords indicate timely filing context.", "kw_timely", 80),
    ("KW_PA", "Keywords indicate prior authorization context.", "kw_pa", 80),
    ("KW_NDC", "Keywords indicate NDC / mapping context.", "kw_ndc", 75),
    ("KW_ELIGIBILITY", "Keywords indicate eligibility/coverage context.", "kw_eligibility", 75),
]

for etype, msg, colname, score in kw_map:
    evidence_dfs.append(
        ev(
            base.filter(F.col(colname) == 1),
            etype,
            F.lit(msg),
            F.lit(score)
        )
    )

# 5) 340B channel / flag evidence
evidence_dfs.append(
    ev(
        base.filter((F.col("channel") == "340B") | (F.col("is_340b_flag") == 1)),
        "CHANNEL_340B",
        F.lit("Dispute is flagged as 340B (channel/flag indicates 340B involvement)."),
        F.lit(88)
    )
)

# 6) Plan evidence (if present)
if plan_ref is not None:
    evidence_dfs.append(
        ev(
            base.filter(F.col("coverage_generosity").isNotNull()),
            "PLAN_CONTEXT",
            F.concat(F.lit("Plan coverage generosity = "), F.col("coverage_generosity")),
            F.lit(55)
        )
    )

# 7) Drug reference evidence (if present)
if ndc_ref is not None:
    evidence_dfs.append(
        ev(
            base.filter(F.col("therapeutic_class").isNotNull()),
            "NDC_CONTEXT",
            F.concat(
                F.lit("Drug context: therapeutic class = "), F.col("therapeutic_class"),
                F.lit("; manufacturer = "), F.col("manufacturer")
            ),
            F.lit(50)
        )
    )

# Union all evidence candidates
evidence_all = None
for dfi in evidence_dfs:
    evidence_all = dfi if evidence_all is None else evidence_all.unionByName(dfi, allowMissingColumns=True)

# Persist candidates
evidence_all.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_evidence_candidates")
print(f"✅ Created: {db}.dispute_evidence_candidates")


# ------------------------------------------------------------
# Rank evidence per dispute and keep Top-3
# ------------------------------------------------------------
w = Window.partitionBy("dispute_id").orderBy(F.desc("evidence_score"), F.desc("evidence_ts"))

top3 = (
    spark.table(f"{db}.dispute_evidence_candidates")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

top3.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_evidence_top3")
print(f"✅ Created: {db}.dispute_evidence_top3")


# ------------------------------------------------------------
# Enrich queue: evidence array + evidence_text_block
# ------------------------------------------------------------
evidence_agg = (
    top3.groupBy("dispute_id")
        .agg(
            F.collect_list(
                F.concat(
                    F.lit("["), F.col("evidence_type"), F.lit("] "),
                    F.col("evidence_text"),
                    F.lit(" (score="), F.format_number(F.col("evidence_score"), 0), F.lit(")")
                )
            ).alias("evidence_list")
        )
        .withColumn("evidence_text_block", F.concat_ws("\n", F.col("evidence_list")))
)

enriched = (
    spark.table(f"{db}.dispute_work_queue").alias("q")
    .join(evidence_agg.alias("e"), on="dispute_id", how="left")
)

enriched.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_work_queue_enriched")
print(f"✅ Created: {db}.dispute_work_queue_enriched")

# Quick preview
spark.table(f"{db}.dispute_work_queue_enriched") \
  .select("dispute_id","pred_reason_xgb","conf_xgb","priority","owner_queue","evidence_text_block") \
  .show(10, truncate=False)


# COMMAND ----------

from pyspark.sql import functions as F

db = "dispute_ai"
src = spark.table(f"{db}.dispute_work_queue_enriched")

actionable = (
    src
    .withColumn(
        "requires_human_review",
        F.when(F.col("conf_xgb").isNull(), F.lit(True))
         .when(F.col("conf_xgb") < 0.75, F.lit(True))
         .otherwise(F.lit(False))
    )
    .withColumn(
        "recommended_action",
        F.when(
            (F.col("pred_reason_xgb") == "TIMELY_FILING") & (F.col("days_to_submit").isNotNull()) & (F.col("timely_flag") == 0),
            F.lit("Validate submission dates; request timely-filing exception or supporting proof; refile if allowed")
        ).when(
            (F.col("pred_reason_xgb") == "TIMELY_FILING") & (F.col("timely_flag") == 1),
            F.lit("Proceed with dispute citing timely filing compliance; attach submission proof")
        ).when(
            (F.col("pred_reason_xgb") == "DUPLICATE_DISCOUNT_340B"),
            F.lit("Run 340B duplicate discount validation; attach 340B claim/eligibility evidence; coordinate with covered entity if needed")
        ).when(
            (F.col("pred_reason_xgb") == "PRIOR_AUTH"),
            F.lit("Check PA requirement; validate PA number/status; attach PA approval or request retro-PA review")
        ).when(
            (F.col("pred_reason_xgb") == "ELIGIBILITY"),
            F.lit("Verify member/plan eligibility for DOS; validate coverage window; attach eligibility proof")
        ).when(
            (F.col("pred_reason_xgb") == "NDC_MAPPING"),
            F.lit("Validate NDC11 mapping (package/labeler); check crosswalk; attach correct NDC and product reference")
        ).when(
            (F.col("pred_reason_xgb") == "PLAN_EXCLUSION"),
            F.lit("Review formulary/coverage rules; verify exception criteria; submit medical necessity/coverage documentation")
        ).when(
            (F.col("pred_reason_xgb") == "STEP_THERAPY"),
            F.lit("Verify step therapy requirements; attach trial/failure evidence or exception documentation")
        ).otherwise(
            F.lit("Review pricing/contract alignment; validate units/contract terms; attach contract/rate evidence and recalculation details")
        )
    )
    .withColumn(
        "action_reason",
        F.concat(
            F.lit("Predicted="), F.col("pred_reason_xgb"),
            F.lit("; confidence="), F.format_number(F.col("conf_xgb"), 3),
            F.when(F.col("evidence_text_block").isNotNull(), F.concat(F.lit("; top evidence:\n"), F.col("evidence_text_block"))).otherwise(F.lit(""))
        )
    )
    .withColumn(
        "next_action_owner",
        F.when(F.col("pred_reason_xgb") == "DUPLICATE_DISCOUNT_340B", F.lit("340B Team"))
         .when(F.col("pred_reason_xgb") == "TIMELY_FILING", F.lit("Timely Filing Team"))
         .when(F.col("pred_reason_xgb") == "PRIOR_AUTH", F.lit("PA Team"))
         .when(F.col("pred_reason_xgb").isin("PLAN_EXCLUSION","STEP_THERAPY","ELIGIBILITY","NDC_MAPPING"), F.lit("Policy Ops Team"))
         .otherwise(F.lit("Pricing/Admin Team"))
    )
    .withColumn("action_created_at", F.current_timestamp())
)

actionable.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dispute_work_queue_actionable")
print(f"✅ Created: {db}.dispute_work_queue_actionable")

spark.table(f"{db}.dispute_work_queue_actionable") \
  .select("dispute_id","pred_reason_xgb","conf_xgb","priority","next_action_owner","recommended_action","requires_human_review") \
  .show(10, truncate=False)


# COMMAND ----------

from pyspark.sql import functions as F

db = "dispute_ai"
src = spark.table(f"{db}.dispute_work_queue_actionable")

# Keep prompts small enough for token limits
prompts = (
    src
    .select(
        "dispute_id",
        "pred_reason_xgb","conf_xgb",
        "priority","owner_queue","next_action_owner",
        "expected_amount","paid_amount","recoverable_amount","dispute_variance_amount",
        "days_to_submit","timely_flag",
        "recommended_action",
        "evidence_text_block"
    )
    .withColumn(
        "llm_prompt",
        F.concat(
            F.lit(
                "You are a medicaid market access disputes assistant. "
                "Write an audit-ready explanation and next steps.\n\n"
                "Return JSON with keys: summary, rationale, next_steps, missing_info, risk_level.\n\n"
            ),
            F.lit("Case:\n"),
            F.lit("- predicted_reason: "), F.col("pred_reason_xgb"), F.lit("\n"),
            F.lit("- confidence: "), F.format_number(F.col("conf_xgb"), 3), F.lit("\n"),
            F.lit("- priority: "), F.col("priority"), F.lit("\n"),
            F.lit("- expected_amount: "), F.col("expected_amount").cast("string"), F.lit("\n"),
            F.lit("- paid_amount: "), F.col("paid_amount").cast("string"), F.lit("\n"),
            F.lit("- recoverable_amount: "), F.col("recoverable_amount").cast("string"), F.lit("\n"),
            F.lit("- dispute_variance_amount: "), F.col("dispute_variance_amount").cast("string"), F.lit("\n"),
            F.lit("- days_to_submit: "), F.col("days_to_submit").cast("string"), F.lit("\n"),
            F.lit("- timely_flag: "), F.col("timely_flag").cast("string"), F.lit("\n\n"),
            F.lit("Recommended action:\n"),
            F.col("recommended_action"),
            F.lit("\n\nEvidence:\n"),
            F.coalesce(F.col("evidence_text_block"), F.lit("No evidence available.")),
            F.lit("\n\nRules:\n- Do not invent facts.\n- If evidence is insufficient, say what is missing.\n")
        )
    )
    .withColumn("prompt_created_at", F.current_timestamp())
)

prompts.write.format("delta").mode("overwrite").saveAsTable(f"{db}.llm_explanation_prompts")
print(f"✅ Created: {db}.llm_explanation_prompts")

spark.table(f"{db}.llm_explanation_prompts").select("dispute_id","llm_prompt").show(2, truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dispute_id,
# MAGIC   ai_query("databricks-gpt-5-1", llm_prompt) AS llm_response
# MAGIC FROM dispute_ai.llm_explanation_prompts
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dispute_ai.v_queue_dashboard AS
# MAGIC SELECT
# MAGIC   q.dispute_id,
# MAGIC   q.created_at,
# MAGIC   q.priority,
# MAGIC   q.owner_queue,
# MAGIC   q.work_status,
# MAGIC   q.pred_reason_xgb,
# MAGIC   q.conf_xgb,
# MAGIC   q.recommended_action,
# MAGIC   q.next_action_owner,
# MAGIC   q.requires_human_review,
# MAGIC   q.dispute_variance_amount,
# MAGIC   q.recoverable_amount,
# MAGIC   e.evidence_text_block
# MAGIC FROM dispute_ai.dispute_work_queue_actionable q
# MAGIC LEFT JOIN dispute_ai.dispute_work_queue_enriched e
# MAGIC   ON q.dispute_id = e.dispute_id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `workspace`; select * from `dispute_ai`.`plan_reference` limit 100;
