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

from pyspark.sql.functions import (
    col,
    current_date,
    datediff,
    when
)

def gold_patient():

    # =====================================
    # Read Silver tables
    # =====================================

    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")
    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")
    o = spark.read.format("delta").load("Tables/Silver_Layer/Observation")
    c = spark.read.format("delta").load("Tables/Silver_Layer/Condition")

    # =====================================
    # Counts per patient
    # =====================================

    enc_cnt = e.groupBy("patient_id") \
        .count() \
        .withColumnRenamed("count", "encounter_count")

    obs_cnt = o.groupBy("patient_id") \
        .count() \
        .withColumnRenamed("count", "observation_count")

    cond_cnt = c.groupBy("patient_id") \
        .count() \
        .withColumnRenamed("count", "condition_count")

    # =====================================
    # Join counts to patient table
    # =====================================

    gold = p \
        .join(enc_cnt, "patient_id", "left") \
        .join(obs_cnt, "patient_id", "left") \
        .join(cond_cnt, "patient_id", "left")

    # Replace NULL counts with 0
    gold = gold.fillna({
        "encounter_count": 0,
        "observation_count": 0,
        "condition_count": 0
    })

    # =====================================
    # Boolean cleanup
    # =====================================

    gold = gold.withColumn(
        "active",
        col("active").cast("boolean")
    ).fillna({"active": False})

    gold = gold.withColumn(
        "deceased",
        col("deceased").cast("boolean")
    ).fillna({"deceased": False})   # ✅ FIXED

    # =====================================
    # Age calculation
    # =====================================

    gold = gold.withColumn(
        "patient_age",
        when(
            col("birth_date").isNotNull(),
            datediff(current_date(), col("birth_date")) / 365
        )
    )

    # =====================================
    # Activity flags (great for dashboards)
    # =====================================

    gold = gold.withColumn(
        "has_encounter",
        col("encounter_count") > 0
    )

    gold = gold.withColumn(
        "has_observation",
        col("observation_count") > 0
    )

    gold = gold.withColumn(
        "has_condition",
        col("condition_count") > 0
    )

    # =====================================
    # Write Gold table
    # =====================================

    gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("Tables/Gold_Layer/Patient")

    print("✅ GOLD.Patient created (analytics-ready)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    to_date,
    coalesce,
    when,
    unix_timestamp,
    round,
    floor,
    lit,
    datediff
)

def gold_encounter():

    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")
    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")

    gold = e.join(
        p.select("patient_id", "gender", "birth_date"),
        "patient_id",
        "left"
    )

    # Cast timestamps
    gold = gold.withColumn("period_start", col("period_start").cast("timestamp")) \
               .withColumn("period_end", col("period_end").cast("timestamp"))

    # Encounter date
    gold = gold.withColumn(
        "encounter_date",
        coalesce(
            to_date("period_start"),
            to_date("period_end"),
            to_date("ingested_at")
        )
    )

    # Duration
    gold = gold.withColumn(
        "duration_minutes",
        when(
            col("period_start").isNotNull() &
            col("period_end").isNotNull() &
            (col("period_end") >= col("period_start")),

            round(
                (unix_timestamp("period_end") -
                 unix_timestamp("period_start")) / 60
            )
        )
    )

    # Age at encounter
    gold = gold.withColumn(
        "age_at_encounter",
        when(
            col("birth_date").isNotNull() &
            col("encounter_date").isNotNull(),
            floor(datediff(col("encounter_date"), col("birth_date")) / 365)
        )
    )

    # Status cleanup
    gold = gold.withColumn(
        "status",
        coalesce(col("status"), lit("unknown"))
    )

    # Flags
    gold = gold.withColumn("is_completed", col("status") == "finished")
    gold = gold.withColumn("has_provider", col("provider_reference").isNotNull())

    # Final schema
    gold = gold.select(
        "encounter_id",
        "patient_id",
        "status",
        "provider_reference",
        "period_start",
        "period_end",
        "encounter_date",
        "duration_minutes",
        "gender",
        "birth_date",
        "age_at_encounter",
        "is_completed",
        "has_provider",
        "ingested_at"
    )

    gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("Tables/Gold_Layer/Encounter")

    print("✅ GOLD.Encounter created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    coalesce,
    to_date,
    lit,
    when,
    datediff,
    floor,
    current_date
)

def gold_observation():

    # =====================================
    # Read Silver tables
    # =====================================

    o = spark.read.format("delta").load("Tables/Silver_Layer/Observation")
    e = spark.read.format("delta").load("Tables/Silver_Layer/Encounter")
    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")

    # =====================================
    # Join encounter info
    # =====================================

    df = o.join(
        e.select("encounter_id", "period_start"),
        "encounter_id",
        "left"
    )

    # =====================================
    # Join patient demographics
    # =====================================

    df = df.join(
        p.select("patient_id", "gender", "birth_date"),
        "patient_id",
        "left"
    )

    # =====================================
    # Standardize timestamps
    # =====================================

    df = df.withColumn(
        "effective_date",
        col("effective_date").cast("timestamp")
    ).withColumn(
        "period_start",
        col("period_start").cast("timestamp")
    ).withColumn(
        "ingested_at",
        col("ingested_at").cast("timestamp")
    )

    # =====================================
    # Create analytics-ready dataset
    # =====================================

    gold = df.select(

        # ---------- Keys ----------
        col("observation_id"),
        col("patient_id"),
        col("encounter_id"),

        # ---------- Clinical meaning ----------
        col("observation_code"),
        col("observation_display"),
        coalesce(col("status"), lit("unknown")).alias("status"),

        # ---------- Raw values ----------
        col("value_quantity").alias("numeric_value"),
        col("value_boolean").alias("boolean_value"),
        col("value_string").alias("text_value"),
        col("value_unit").alias("unit"),

        # ---------- Observation date hierarchy ----------
        to_date(
            coalesce(
                col("effective_date"),   # best clinical time
                col("period_start"),     # encounter time
                col("ingested_at")       # fallback
            )
        ).alias("observation_date"),

        to_date(col("period_start")).alias("encounter_date"),

        # ---------- Patient context ----------
        col("gender"),
        col("birth_date"),

        # ---------- Metadata ----------
        col("ingested_at")
    )

    # =====================================
    # Derived analytics fields
    # =====================================

    # Age at observation
    gold = gold.withColumn(
        "age_at_observation",
        when(
            col("birth_date").isNotNull() &
            col("observation_date").isNotNull(),
            floor(datediff(col("observation_date"), col("birth_date")) / 365)
        )
    )

    # Observation type classification
    gold = gold.withColumn(
        "value_type",
        when(col("numeric_value").isNotNull(), "Numeric")
        .when(col("boolean_value").isNotNull(), "Boolean")
        .when(col("text_value").isNotNull(), "Text")
        .otherwise("Unknown")
    )

    # Example clinical grouping (very useful KPI)
    gold = gold.withColumn(
        "observation_category",
        when(col("observation_display").rlike("(?i)weight|height"), "Vitals")
        .when(col("observation_display").rlike("(?i)blood|hemoglobin|platelet"), "Laboratory")
        .otherwise("Other")
    )

    # Data quality flag
    gold = gold.withColumn(
        "has_value",
        col("numeric_value").isNotNull() |
        col("boolean_value").isNotNull() |
        col("text_value").isNotNull()
    )

    # =====================================
    # Write GOLD table
    # =====================================

    gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("Tables/Gold_Layer/Observation")

    print("✅ GOLD.Observation created (enterprise-ready)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    when,
    current_date,
    datediff,
    count,
    coalesce,
    lit,
    to_date,
    floor
)

def gold_condition():

    target_path = "Tables/Gold_Layer/Condition"

    # =====================================
    # Read Silver tables
    # =====================================

    c = spark.read.format("delta").load("Tables/Silver_Layer/Condition")
    p = spark.read.format("delta").load("Tables/Silver_Layer/Patient")

    # =====================================
    # Ensure correct data types
    # =====================================

    c = c.withColumn("onset_date", to_date("onset_date")) \
         .withColumn("recorded_date", to_date("recorded_date"))

    p = p.withColumn("birth_date", to_date("birth_date"))

    # =====================================
    # Join patient demographics
    # =====================================

    gold = c.join(
        p.select("patient_id", "gender", "birth_date"),
        "patient_id",
        "left"
    )

    # =====================================
    # Best available clinical date
    # =====================================

    gold = gold.withColumn(
        "condition_date",
        coalesce(
            col("onset_date"),
            col("recorded_date"),
            col("ingested_at")
        )
    )

    # =====================================
    # Derived analytics fields
    # =====================================

    gold = gold.withColumn(
        "has_encounter",
        col("encounter_id").isNotNull()
    )

    # Status grouping
    gold = gold.withColumn(
        "condition_status_group",
        when(col("clinical_status") == "active", "Active")
        .when(col("clinical_status").isNull(), "Unknown")
        .otherwise("Inactive")
    )

    # Condition age in days
    gold = gold.withColumn(
        "condition_age_days",
        when(
            col("condition_date").isNotNull(),
            datediff(current_date(), col("condition_date"))
        )
    )

    # Patient age (years)
    gold = gold.withColumn(
        "patient_age",
        when(
            col("birth_date").isNotNull(),
            floor(datediff(current_date(), col("birth_date")) / 365.25)
        )
    )

    # Chronic flag (improved)
    gold = gold.withColumn(
        "is_chronic",
        when(
            (col("condition_age_days") > 180) &
            (col("clinical_status") == "active"),
            True
        ).otherwise(False)
    )

    # Recent condition flag (NEW KPI)
    gold = gold.withColumn(
        "is_recent",
        when(col("condition_age_days") <= 30, True)
        .otherwise(False)
    )

    # Condition count per patient
    cond_cnt = c.groupBy("patient_id") \
        .agg(count("*").alias("condition_count"))

    gold = gold.join(cond_cnt, "patient_id", "left")

    # Clean clinical_status
    gold = gold.withColumn(
        "clinical_status",
        coalesce(col("clinical_status"), lit("unknown"))
    )

    # =====================================
    # Final curated schema
    # =====================================

    gold = gold.select(

        # Keys
        "condition_id",
        "patient_id",
        "encounter_id",

        # Clinical info
        "condition_code",
        "condition_display",
        "clinical_status",
        "condition_status_group",

        # Dates
        "onset_date",
        "recorded_date",
        "condition_date",

        # Derived metrics
        "condition_age_days",
        "is_chronic",
        "is_recent",
        "has_encounter",
        "condition_count",

        # Patient context
        "gender",
        "birth_date",
        "patient_age",

        # Metadata
        "ingested_at"
    )

    # =====================================
    # Write GOLD table
    # =====================================

    gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(target_path)

    print("✅ GOLD.Condition created (enterprise-ready)")

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
