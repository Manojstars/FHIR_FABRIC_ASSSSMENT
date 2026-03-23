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

from pyspark.sql.functions import explode, col, lit, current_timestamp

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

def raw_to_bronze(resource):

    raw_path = f"Files/RAW/{resource}"
    bronze_path = f"Tables/Bronze_layer/{resource}"

    print(f"\nProcessing {resource}")

    # Read all RAW JSON files
    df = spark.read.option("multiLine", True).json(raw_path)

    # Extract resources from bundle
    df_bronze = (
        df
        .withColumn("entry", explode(col("entry")))
        .select("entry.resource.*")
    )

    # Add metadata
    df_bronze = df_bronze.withColumn("ingested_at", lit(current_timestamp()))

    # Write to Delta
    df_bronze.write.format("delta") \
        .mode("overwrite") \
        .save(bronze_path)

    print(f" Bronze table created: {bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n===== RAW → BRONZE Started =====")

for resource in RESOURCES:
    raw_to_bronze(resource)

print("\n Bronze ingestion completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
