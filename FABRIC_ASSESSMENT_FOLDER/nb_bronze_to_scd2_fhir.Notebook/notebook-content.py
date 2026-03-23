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

def apply_scd2(resource):

    bronze_path = f"Tables/Bronze_layer/{resource}"
    scd2_path = f"Tables/Scd2/{resource}"

    print(f"\nProcessing SCD2 for {resource}")

    
    df = spark.read.format("delta").load(bronze_path)

    
    df = df.dropDuplicates(["id"])

    
    df = df.withColumn(
        "row_hash",
        sha2(to_json(struct(*[col(c) for c in df.columns])), 256)
    )

    
    df = (
        df
        .withColumn("valid_from", current_timestamp())
        .withColumn("valid_to", lit("9999-12-31"))
        .withColumn("is_current", lit(True))
    )


    if not DeltaTable.isDeltaTable(spark, scd2_path):

        df.write.format("delta") \
            .mode("overwrite") \
            .save(scd2_path)

        print("Initial SCD2 table created")
        return


    scd2_table = DeltaTable.forPath(spark, scd2_path)

    
    scd2_table.alias("target").merge(
        df.alias("source"),
        "target.id = source.id AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.row_hash != source.row_hash",
        set={
            "valid_to": current_timestamp(),
            "is_current": lit(False)
        }
    ).execute()

    df.alias("source").join(
        scd2_table.toDF().alias("target"),
        on="id",
        how="left_anti"
    ).write.format("delta").mode("append").save(scd2_path)

    print("SCD2 applied successfully")

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
