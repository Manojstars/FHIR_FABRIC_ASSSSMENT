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

from pyspark.sql.functions import col

def silver_patient():

    path_in = "Tables/Scd2/Patient"
    path_out = "Tables/Silver_Layer/Patient"

    print("\nProcessing SILVER Patient")

    df = spark.read.format("delta").load(path_in)

    # Only current records (SCD2)
    df = df.filter(col("is_current") == True)

    # Build analytics-friendly schema
    df_silver = df.select(

        # Core identifiers
        col("id").alias("patient_id"),

        # Demographics
        col("active"),
        col("gender"),
        col("birthDate").alias("birth_date"),
        col("deceasedBoolean").alias("deceased"),

        # Metadata
        col("language"),
        col("ingested_at")
    )

    # Write Silver table
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_out)

    print("✅ Silver Patient created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

def silver_encounter():

    path_in = "Tables/Scd2/Encounter"
    path_out = "Tables/Silver_Layer/Encounter"

    print("\nProcessing SILVER Encounter")

    df = spark.read.format("delta").load(path_in)

    # Only current records
    df = df.filter(col("is_current") == True)

    # Safe projection using available columns
    df_silver = df.select(

        col("id").alias("encounter_id"),
        col("status"),

        # These fields MAY exist depending on your Bronze flattening
        col("subject_reference").alias("patient_reference") 
            if "subject_reference" in df.columns else col("id").alias("patient_reference"),

        col("patient_id") 
            if "patient_id" in df.columns else col("id").alias("patient_id"),

        col("period_start") 
            if "period_start" in df.columns else col("ingested_at").alias("period_start"),

        col("period_end") 
            if "period_end" in df.columns else col("ingested_at").alias("period_end"),

        col("provider_reference") 
            if "provider_reference" in df.columns else col("id").alias("provider_reference"),

        col("ingested_at")
    )

    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_out)

    print("✅ Silver Encounter created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

def silver_observation():

    path_in = "Tables/Scd2/Observation"
    path_out = "Tables/Silver_Layer/Observation"

    print("\nProcessing SILVER Observation")

    df = spark.read.format("delta").load(path_in)

    # Only current records
    df = df.filter(col("is_current") == True)

    # Build safe schema from available columns
    df_silver = df.select(

        col("id").alias("observation_id"),
        col("status"),

        # Patient info (if present)
        col("patient_id") 
            if "patient_id" in df.columns else col("id").alias("patient_id"),

        col("encounter_id") 
            if "encounter_id" in df.columns else col("id").alias("encounter_id"),

        # Observation details (may be absent)
        col("observation_code") 
            if "observation_code" in df.columns else col("id").alias("observation_code"),

        col("observation_display") 
            if "observation_display" in df.columns else col("id").alias("observation_display"),

        col("value_quantity") 
            if "value_quantity" in df.columns else col("ingested_at").alias("value_quantity"),

        col("value_unit") 
            if "value_unit" in df.columns else col("id").alias("value_unit"),

        col("value_string") 
            if "value_string" in df.columns else col("id").alias("value_string"),

        col("value_boolean") 
            if "value_boolean" in df.columns else col("id").alias("value_boolean"),

        col("effective_date") 
            if "effective_date" in df.columns else col("ingested_at").alias("effective_date"),

        col("ingested_at")
    )

    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_out)

    print("✅ Silver Observation created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

def silver_condition():

    path_in = "Tables/Scd2/Condition"
    path_out = "Tables/Silver_Layer/Condition"

    print("\nProcessing SILVER Condition")

    df = spark.read.format("delta").load(path_in)

    # Only current records
    df = df.filter(col("is_current") == True)

    df_silver = df.select(

        col("id").alias("condition_id"),

        # Patient / encounter (if available)
        col("patient_id")
            if "patient_id" in df.columns else col("id").alias("patient_id"),

        col("encounter_id")
            if "encounter_id" in df.columns else col("id").alias("encounter_id"),

        # Clinical info (if available)
        col("condition_code")
            if "condition_code" in df.columns else col("id").alias("condition_code"),

        col("condition_display")
            if "condition_display" in df.columns else col("id").alias("condition_display"),

        col("clinical_status")
            if "clinical_status" in df.columns else col("id").alias("clinical_status"),

        col("onset_date")
            if "onset_date" in df.columns else col("ingested_at").alias("onset_date"),

        col("recorded_date")
            if "recorded_date" in df.columns else col("ingested_at").alias("recorded_date"),

        col("ingested_at")
    )

    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path_out)

    print("✅ Silver Condition created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n===== SCD2 → SILVER Started =====")

silver_patient()
silver_encounter()
silver_observation()
silver_condition()

print("\n SILVER layer completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
