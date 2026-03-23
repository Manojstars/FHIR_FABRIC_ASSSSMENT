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

def gold_patient():

    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")
    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")
    o = spark.read.format("delta").load("Tables/Silver_Layer/Observation")
    c = spark.read.format("delta").load("Tables/Silver_Layer/Condition")

    # Counts per patient
    enc_cnt = e.groupBy("patient_id").count().withColumnRenamed("count", "encounter_count")
    obs_cnt = o.groupBy("patient_id").count().withColumnRenamed("count", "observation_count")
    cond_cnt = c.groupBy("patient_id").count().withColumnRenamed("count", "condition_count")

    gold = p \
        .join(enc_cnt, "patient_id", "left") \
        .join(obs_cnt, "patient_id", "left") \
        .join(cond_cnt, "patient_id", "left")

    gold = gold.fillna({
        "encounter_count": 0,
        "observation_count": 0,
        "condition_count": 0
    })

    gold.write.format("delta").mode("overwrite").save("Tables/Gold_Layer/Patient")

    print("GOLD.Patient created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gold_encounter():

    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")
    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")

    gold = e.join(
        p.select("patient_id", "gender", "birth_date"),
        "patient_id",
        "left"
    )

    gold.write.format("delta").mode("overwrite").save("Tables/Gold_Layer/Encounter")

    print("GOLD.Encounter created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gold_encounter():

    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")
    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")

    gold = e.join(
        p.select("patient_id", "gender", "birth_date"),
        "patient_id",
        "left"
    )

    gold.write.format("delta").mode("overwrite").save("Tables/Gold_Layer/Encounter")

    print("GOLD.Encounter created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gold_observation():

    o = spark.read.format("delta").load("Tables/Silver_Layer/Observation")
    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")

    gold = o.join(
        e.select("encounter_id", "period_start"),
        "encounter_id",
        "left"
    )

    gold.write.format("delta").mode("overwrite").save("Tables/Gold_Layer/Observation")

    print("GOLD.Observation created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def gold_condition():

    c = spark.read.format("delta").load("Tables/Silver_Layer/Condition")
    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")

    gold = c.join(
        p.select("patient_id", "gender"),
        "patient_id",
        "left"
    )

    gold.write.format("delta").mode("overwrite").save("Tables/Gold_Layer/Condition")

    print("GOLD.Condition created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n===== SILVER → GOLD Started =====")

gold_patient()
gold_encounter()
gold_observation()
gold_condition()

print("\n GOLD layer completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
