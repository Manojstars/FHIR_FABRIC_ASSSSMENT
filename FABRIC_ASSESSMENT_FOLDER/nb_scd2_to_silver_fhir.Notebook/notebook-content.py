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

def silver_patient():

    path_in = "Tables/Scd2/Patient"
    path_out = "Tables/Silver_Layer/Patient"

    df = spark.read.format("delta").load(path_in)

    # Only current records
    df = df.filter(col("is_current") == True)

    df_silver = df.select(
        col("id").alias("patient_id"),
        col("active"),
        col("gender"),
        col("birthDate").alias("birth_date"),
        col("deceasedBoolean").alias("deceased"),

        # Name (first entry)
        col("name")[0]["family"].alias("family_name"),
        col("name")[0]["given"][0].alias("given_name"),

        # Address
        col("address")[0]["city"].alias("city"),
        col("address")[0]["state"].alias("state"),
        col("address")[0]["country"].alias("country"),

        col("ingested_at")
    )

    df_silver.write.format("delta").mode("overwrite").save(path_out)

    print("Silver Patient created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_encounter():

    path_in = "Tables/Scd2/Encounter"
    path_out = "Tables/Silver_Layer/Encounter"

    df = spark.read.format("delta").load(path_in)

    df = df.filter(col("is_current") == True)

    df_silver = df.select(
        col("id").alias("encounter_id"),
        col("status"),
        col("class.code").alias("encounter_class"),

        col("subject.reference").alias("patient_reference"),
        regexp_extract(col("subject.reference"), r"Patient/(.+)", 1).alias("patient_id"),

        col("period.start").alias("period_start"),
        col("period.end").alias("period_end"),

        col("serviceProvider.reference").alias("provider_reference"),

        col("ingested_at")
    )

    df_silver.write.format("delta").mode("overwrite").save(path_out)

    print("Silver Encounter created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_observation():

    path_in = "Tables/Scd2/Observation"
    path_out = "Tables/Silver_Layer/Observation"

    df = spark.read.format("delta").load(path_in)

    df = df.filter(col("is_current") == True)

    df_silver = df.select(
        col("id").alias("observation_id"),
        col("status"),

        col("subject.reference").alias("patient_reference"),
        regexp_extract(col("subject.reference"), r"Patient/(.+)", 1).alias("patient_id"),

        col("encounter.reference").alias("encounter_reference"),
        regexp_extract(col("encounter.reference"), r"Encounter/(.+)", 1).alias("encounter_id"),

        col("code.coding")[0]["code"].alias("observation_code"),
        col("code.coding")[0]["display"].alias("observation_display"),

        col("valueQuantity.value").alias("value_quantity"),
        col("valueQuantity.unit").alias("value_unit"),

        col("valueString").alias("value_string"),
        col("valueBoolean").alias("value_boolean"),

        col("effectiveDateTime").alias("effective_date"),

        col("ingested_at")
    )

    df_silver.write.format("delta").mode("overwrite").save(path_out)

    print("Silver Observation created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def silver_condition():

    path_in = "Tables/Scd2/Condition"
    path_out = "Tables/Silver_Layer/Condition"

    df = spark.read.format("delta").load(path_in)

    df = df.filter(col("is_current") == True)

    df_silver = df.select(
        col("id").alias("condition_id"),

        col("subject.reference").alias("patient_reference"),
        regexp_extract(col("subject.reference"), r"Patient/(.+)", 1).alias("patient_id"),

        col("encounter.reference").alias("encounter_reference"),
        regexp_extract(col("encounter.reference"), r"Encounter/(.+)", 1).alias("encounter_id"),

        col("code.coding")[0]["code"].alias("condition_code"),
        col("code.coding")[0]["display"].alias("condition_display"),

        col("clinicalStatus.coding")[0]["code"].alias("clinical_status"),

        col("onsetDateTime").alias("onset_date"),
        col("recordedDate").alias("recorded_date"),

        col("ingested_at")
    )

    df_silver.write.format("delta").mode("overwrite").save(path_out)

    print("Silver Condition created")

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

print("\n✅ SILVER layer completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
