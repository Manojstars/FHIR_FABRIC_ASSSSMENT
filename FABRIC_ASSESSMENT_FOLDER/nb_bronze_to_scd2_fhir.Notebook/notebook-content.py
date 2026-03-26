# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "488995c7-c22b-4ea2-8875-1e538f017033",
# META       "default_lakehouse_name": "FHIR_Lakehouse",
# META       "default_lakehouse_workspace_id": "70296d80-3b83-4453-828e-8921e196c568",
# META       "known_lakehouses": [
# META         {
# META           "id": "488995c7-c22b-4ea2-8875-1e538f017033"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, sha2, to_json, struct, current_timestamp, lit

def apply_scd2(resource):

    bronze_path = f"Tables/Bronze_layer/{resource}"
    scd2_path = f"Tables/Scd2/{resource}"

    print(f"\nProcessing SCD2 for {resource}")
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *


def apply_scd2(resource):

    bronze_path = f"Tables/Bronze_layer/{resource}"
    scd2_path   = f"Tables/Scd2/{resource}"

    print(f"\nProcessing SCD2 for {resource}")

    df = spark.read.format("delta").load(bronze_path)

    # ---------------------------------------
    # REMOVE complex nested columns (FHIR fix)
    # ---------------------------------------

    complex_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (ArrayType, StructType, MapType))
    ]

    if complex_cols:
        df = df.drop(*complex_cols)

    # ---------------------------------------
    # Deduplicate by business key
    # ---------------------------------------

    df = df.dropDuplicates(["id"])

    # ---------------------------------------
    # Create row hash
    # ---------------------------------------

    df = df.withColumn(
        "row_hash",
        sha2(to_json(struct(*df.columns)), 256)
    )

    # ---------------------------------------
    # SCD2 columns
    # ---------------------------------------

    df = df \
        .withColumn("valid_from", current_timestamp()) \
        .withColumn("valid_to", lit("9999-12-31")) \
        .withColumn("is_current", lit(True))

    # ---------------------------------------
    # Initial load
    # ---------------------------------------

    if not DeltaTable.isDeltaTable(spark, scd2_path):

        df.write.format("delta") \
            .mode("overwrite") \
            .save(scd2_path)

        print("Initial SCD2 table created")
        return

    scd2_table = DeltaTable.forPath(spark, scd2_path)

    # ---------------------------------------
    # Expire changed records
    # ---------------------------------------

    scd2_table.alias("target").merge(
        df.alias("source"),
        "target.id = source.id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.row_hash <> source.row_hash",
        set={
            "valid_to": current_timestamp(),
            "is_current": lit(False)
        }
    ).execute()

    # ---------------------------------------
    # Insert new or changed records
    # ---------------------------------------

    df.alias("source").join(
        scd2_table.toDF().alias("target"),
        (col("source.id") == col("target.id")) &
        (col("target.is_current") == True) &
        (col("source.row_hash") == col("target.row_hash")),
        "left_anti"
    ).write.format("delta") \
        .mode("append") \
        .save(scd2_path)

    print("✅ SCD2 applied successfully")
    # Read bronze data
    df = spark.read.format("delta").load(bronze_path)

    # Remove duplicates on business key
    df = df.dropDuplicates(["id"])

    # Create row hash (only business columns)
    business_cols = [c for c in df.columns if c not in ["ingested_at"]]

    df = df.withColumn(
        "row_hash",
        sha2(to_json(struct(*[col(c) for c in business_cols])), 256)
    )

    # Add SCD metadata
    df = (
        df
        .withColumn("valid_from", current_timestamp())
        .withColumn("valid_to", lit("9999-12-31"))
        .withColumn("is_current", lit(True))
    )

    # If table doesn't exist → initial load
    if not DeltaTable.isDeltaTable(spark, scd2_path):

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema","true") \
            .save(scd2_path)

        print("✅ Initial SCD2 table created")
        return

    scd2_table = DeltaTable.forPath(spark, scd2_path)

    # Close old records if changed
    scd2_table.alias("target").merge(
        df.alias("source"),
        "target.id = source.id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.row_hash <> source.row_hash",
        set={
            "valid_to": current_timestamp(),
            "is_current": lit(False)
        }
    ).execute()

    # Insert new OR changed records
    df.alias("source").join(
        scd2_table.toDF().alias("target"),
        (col("source.id") == col("target.id")) &
        (col("target.is_current") == True) &
        (col("source.row_hash") == col("target.row_hash")),
        "left_anti"
    ).write.format("delta") \
        .mode("append") \
        .option("mergeSchema","true") \
        .save(scd2_path)

    print("✅ SCD2 applied successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n===== BRONZE → SCD2 Started =====")

for resource in RESOURCES:
    apply_scd2(resource)

print("\n SCD2 layer completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
