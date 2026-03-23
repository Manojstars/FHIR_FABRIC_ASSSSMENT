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

import requests
import json
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_URL = "https://hapi.fhir.org/baseR4"

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

# Pipeline run date (today)
RUN_DATE = datetime.today().strftime("%Y-%m-%d")

print("Run date:", RUN_DATE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_raw_json(resource, run_date, page, data):

    path = f"Files/RAW/{resource}/date={run_date}/page_{page}.json"

    json_str = json.dumps(data)

    df = spark.createDataFrame([(json_str,)], ["json"])

    df.coalesce(1).write.mode("overwrite").text(path)

    print(f"Saved {path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_fhir_resource(resource, run_date):

    start_date = run_date
    end_date = (
        datetime.strptime(run_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    url = (
        f"{BASE_URL}/{resource}"
        f"?_count=50"
        f"&_lastUpdated=ge{start_date}"
        f"&_lastUpdated=lt{end_date}"
    )

    page = 1

    while url:

        print(f"Fetching {resource} page {page}")

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        save_raw_json(resource, run_date, page, data)

        # Pagination via bundle links
        next_link = None
        for link in data.get("link", []):
            if link["relation"] == "next":
                next_link = link["url"]

        url = next_link
        page += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n===== RAW Ingestion Started =====")

for resource in RESOURCES:
    print(f"\nProcessing: {resource}")
    fetch_fhir_resource(resource, RUN_DATE)

print("\n✅ RAW ingestion completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
