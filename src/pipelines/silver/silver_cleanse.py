"""
SILVER layer — written with the new Lakeflow Spark Declarative Pipelines API
(`from pyspark import pipelines as dp`), the successor to `import dlt`.

What happens here
  * Expectations (warn / drop / fail) enforce data quality; bad rows go to quarantine tables
  * AUTO CDC (formerly APPLY CHANGES) turns the patient CDC feed into an SCD Type-2 table
  * Unity Catalog column MASKs and ROW FILTERs are declared *inside* the pipeline,
    so they are enforced on every read of these streaming tables / materialized views
  * Light standardisation: types, trimming, derived columns (LOS, age band, etc.)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("hc.catalog")  # noqa: F821
GOV = f"{CAT}.governance"
FACILITY_FILTER = f"ROW FILTER {GOV}.facility_filter ON (facility_id)"
HOME_FACILITY_FILTER = f"ROW FILTER {GOV}.facility_filter ON (home_facility_id)"

# ---------------------------------------------------------------------------
# PATIENTS — CDC → SCD Type 2 with PHI masks
# ---------------------------------------------------------------------------
PATIENT_RULES = {
    "valid_patient_id": "patient_id IS NOT NULL AND patient_id LIKE 'PAT-%'",
    "valid_operation":  "operation IN ('INSERT','UPDATE','DELETE')",
    "valid_dob":        "dob IS NULL OR dob <= current_date()",
}


@dp.view(comment="Typed, validated patient change feed")
@dp.expect_all_or_drop(PATIENT_RULES)
@dp.expect("valid_email", "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'")
@dp.expect("valid_phone", "phone <> 'N/A'")
def patients_cdc_clean():
    return (
        spark.readStream.table("bronze.patients_raw")  # noqa: F821
        .select(
            "patient_id", "mrn",
            F.initcap(F.trim("first_name")).alias("first_name"),
            F.initcap(F.trim("last_name")).alias("last_name"),
            F.to_date("dob").alias("dob"),
            F.upper("gender").alias("gender"),
            F.when(F.col("phone") == "N/A", None).otherwise(F.col("phone")).alias("phone"),
            F.lower("email").alias("email"),
            "aadhaar_no", "address", "city", "blood_group", "home_facility_id",
            "operation", F.col("sequence_num").cast("bigint").alias("sequence_num"),
            F.to_timestamp("extracted_at").alias("extracted_at"),
        )
    )


dp.create_streaming_table(
    name="silver.patients",
    comment="[SILVER] Patient master, SCD Type 2 history. PHI columns masked via Unity Catalog.",
    table_properties={"quality": "silver", "contains_phi": "true"},
    schema=f"""
        patient_id        STRING NOT NULL,
        mrn               STRING MASK {GOV}.mask_mrn,
        first_name        STRING MASK {GOV}.mask_name,
        last_name         STRING MASK {GOV}.mask_name,
        dob               DATE   MASK {GOV}.mask_dob,
        gender            STRING,
        phone             STRING MASK {GOV}.mask_phone,
        email             STRING MASK {GOV}.mask_email,
        aadhaar_no        STRING MASK {GOV}.mask_aadhaar,
        address           STRING MASK {GOV}.mask_redact,
        city              STRING,
        blood_group       STRING,
        home_facility_id  STRING,
        extracted_at      TIMESTAMP,
        __START_AT        BIGINT,
        __END_AT          BIGINT
    """,
    row_filter=HOME_FACILITY_FILTER,
)

dp.create_auto_cdc_flow(
    target="silver.patients",
    source="patients_cdc_clean",
    keys=["patient_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2,
)


@dp.materialized_view(
    name="silver.patients_current",
    comment="[SILVER] Current version of each patient (open SCD2 rows). Masked + row-filtered.",
    table_properties={"quality": "silver", "contains_phi": "true"},
    schema=f"""
        patient_id STRING, mrn STRING MASK {GOV}.mask_mrn,
        first_name STRING MASK {GOV}.mask_name, last_name STRING MASK {GOV}.mask_name,
        dob DATE MASK {GOV}.mask_dob, age INT, age_band STRING, gender STRING,
        phone STRING MASK {GOV}.mask_phone, email STRING MASK {GOV}.mask_email,
        aadhaar_no STRING MASK {GOV}.mask_aadhaar, address STRING MASK {GOV}.mask_redact,
        city STRING, blood_group STRING, home_facility_id STRING
    """,
    row_filter=HOME_FACILITY_FILTER,
)
def patients_current():
    age = F.floor(F.months_between(F.current_date(), "dob") / 12).cast("int")
    return (
        spark.read.table("silver.patients")  # noqa: F821
        .where("__END_AT IS NULL")
        .withColumn("age", age)
        .withColumn("age_band", F.when(age < 18, "0-17").when(age < 40, "18-39")
                                 .when(age < 60, "40-59").otherwise("60+"))
        .select("patient_id", "mrn", "first_name", "last_name", "dob", "age", "age_band",
                "gender", "phone", "email", "aadhaar_no", "address", "city",
                "blood_group", "home_facility_id")
    )


# ---------------------------------------------------------------------------
# ENCOUNTERS — quarantine pattern (good rows → silver, bad rows → quarantine)
# ---------------------------------------------------------------------------
ENCOUNTER_RULES = {
    "has_facility":        "facility_id IS NOT NULL",
    "has_patient":         "patient_id IS NOT NULL",
    "non_negative_charge": "total_charges_inr >= 0",
    "discharge_after_admit": "discharge_ts IS NULL OR discharge_ts >= admit_ts",
}
QUARANTINE_EXPR = "NOT ({})".format(" AND ".join(f"({r})" for r in ENCOUNTER_RULES.values()))


def _typed_encounters():
    return (
        spark.readStream.table("bronze.encounters_raw")  # noqa: F821
        .withColumn("admit_ts", F.to_timestamp("admit_ts"))
        .withColumn("discharge_ts", F.to_timestamp("discharge_ts"))
        .withColumn("total_charges_inr", F.col("total_charges_inr").cast("decimal(18,2)"))
        .withWatermark("admit_ts", "2 days")
        .dropDuplicates(["encounter_id"])
    )


@dp.table(
    name="silver.encounters",
    comment="[SILVER] Clean encounters with LOS. Row-filtered by facility.",
    table_properties={"quality": "silver"},
    cluster_by=["facility_id", "admit_date"],
    row_filter=FACILITY_FILTER,
)
@dp.expect_all_or_drop(ENCOUNTER_RULES)
def encounters():
    return (
        _typed_encounters()
        .withColumn("admit_date", F.to_date("admit_ts"))
        .withColumn("los_days", F.round(
            (F.coalesce(F.col("discharge_ts"), F.current_timestamp()).cast("long")
             - F.col("admit_ts").cast("long")) / 86400, 2))
        .withColumn("is_active_inpatient",
                    (F.col("encounter_type") == "INPATIENT") & F.col("discharge_ts").isNull())
        .drop("_rescued_data", "extracted_at")
    )


@dp.table(name="ops.encounters_quarantine",
          comment="[OPS] Encounter rows that failed silver expectations — for steward review")
def encounters_quarantine():
    return _typed_encounters().where(QUARANTINE_EXPR).withColumn("_quarantined_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# CLAIMS
# ---------------------------------------------------------------------------
@dp.table(
    name="silver.claims",
    comment="[SILVER] Insurance claims with policy number masked.",
    table_properties={"quality": "silver"},
    schema=f"""
        claim_id STRING, encounter_id STRING, patient_id STRING, payer STRING,
        policy_no STRING MASK {GOV}.mask_redact,
        billed_amount_inr DECIMAL(18,2), approved_amount_inr DECIMAL(18,2),
        claim_status STRING, denial_reason STRING, submitted_at TIMESTAMP, submitted_date DATE
    """,
)
@dp.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dp.expect_or_drop("valid_status", "claim_status IN ('SUBMITTED','APPROVED','PARTIALLY_APPROVED','DENIED','PAID')")
@dp.expect("approved_le_billed", "approved_amount_inr IS NULL OR approved_amount_inr <= billed_amount_inr")
def claims():
    return (
        spark.readStream.table("bronze.claims_raw")  # noqa: F821
        .dropDuplicates(["claim_id"])
        .select(
            "claim_id", "encounter_id", "patient_id", "payer", "policy_no",
            F.col("billed_amount_inr").cast("decimal(18,2)").alias("billed_amount_inr"),
            F.col("approved_amount_inr").cast("decimal(18,2)").alias("approved_amount_inr"),
            "claim_status", "denial_reason",
            F.to_timestamp("submitted_at").alias("submitted_at"),
            F.to_date("submitted_at").alias("submitted_date"),
        )
    )


# ---------------------------------------------------------------------------
# INVENTORY
# ---------------------------------------------------------------------------
@dp.table(
    name="silver.inventory_txn",
    comment="[SILVER] Signed stock movements (receipts +, issues/expiry −).",
    table_properties={"quality": "silver"},
    row_filter=FACILITY_FILTER,
)
@dp.expect_or_drop("positive_qty", "quantity > 0")
@dp.expect_or_drop("known_txn_type", "txn_type IN ('RECEIPT','ISSUE','ADJUSTMENT','EXPIRED')")
def inventory_txn():
    sign = F.when(F.col("txn_type").isin("RECEIPT", "ADJUSTMENT"), 1).otherwise(-1)
    return (
        spark.readStream.table("bronze.inventory_raw")  # noqa: F821
        .dropDuplicates(["txn_id"])
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("expiry_date", F.to_date("expiry_date"))
        .withColumn("signed_qty", sign * F.col("quantity"))
        .withColumn("txn_value_inr", (F.col("quantity") * F.col("unit_cost_inr")).cast("decimal(18,2)"))
        .drop("_rescued_data", "extracted_at")
    )


# ---------------------------------------------------------------------------
# STAFF — salary + PAN masked for everyone outside HR/finance
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="silver.staff",
    comment="[SILVER] Latest staff master; salary and PAN masked.",
    table_properties={"quality": "silver"},
    schema=f"""
        staff_id STRING, full_name STRING, role STRING, department STRING, facility_id STRING,
        monthly_salary_inr INT MASK {GOV}.mask_salary,
        pan_no STRING MASK {GOV}.mask_redact,
        joined_on DATE
    """,
)
@dp.expect_or_drop("valid_staff_id", "staff_id IS NOT NULL")
def staff():
    from pyspark.sql.window import Window
    w = Window.partitionBy("staff_id").orderBy(F.col("_ingest_ts").desc())
    return (
        spark.read.table("bronze.staff_raw")  # noqa: F821
        .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
        .select("staff_id", "full_name", "role", "department", "facility_id",
                F.col("monthly_salary_inr").cast("int").alias("monthly_salary_inr"),
                "pan_no", F.to_date("joined_on").alias("joined_on"))
    )
