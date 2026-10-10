"""
BRONZE layer — written with the classic Delta Live Tables API (`import dlt`).
Auto Loader incrementally ingests every new JSON file that lands in the UC volume.
Bronze is raw + append-only; we only add lineage/audit columns and a rescued-data column.
"""
import dlt
from pyspark.sql import functions as F

LANDING = spark.conf.get("hc.landing_path")  # noqa: F821  (set in pipeline configuration)

ENTITIES = {
    "patients":   "Patient master CDC feed from HIS",
    "encounters": "Admissions / OPD / ER encounters",
    "claims":     "Insurance & TPA claims",
    "inventory":  "Pharmacy and supply stock movements",
    "staff":      "HR staff master",
}


def make_bronze(entity: str, comment: str):
    @dlt.table(
        name=f"bronze.{entity}_raw",
        comment=f"[BRONZE] {comment}. Raw Auto Loader ingest, append-only.",
        table_properties={
            "quality": "bronze",
            "pipelines.reset.allowed": "false",       # protect raw history from full refresh
            "delta.enableChangeDataFeed": "true",
        },
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(f"{LANDING}/{entity}/")
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_mod_ts", F.col("_metadata.file_modification_time"))
        )
    return _bronze


for _entity, _comment in ENTITIES.items():
    make_bronze(_entity, _comment)
