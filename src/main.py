from pathlib import Path
from src.ingestion.read_local_json import read_local_json
from src.bronze.bronze_pipeline import write_bronze_data
from src.silver.silver_pipeline import read_bronze_data, normalize_issues, write_silver_data
from src.silver.silver_pipeline import (read_bronze_data, normalize_issues, transform_datetime_columns, write_silver_data)
from src.gold.gold_pipeline import build_gold_layer, write_gold_data

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_PATH = BASE_DIR / "data" / "source" / "jira_issues_raw.json"
BRONZE_OUTPUT_PATH = BASE_DIR / "data" / "bronze" / "jira_issues_bronze.json"
SILVER_OUTPUT_PATH = BASE_DIR / "data" / "silver" / "jira_issues_silver.json"
GOLD_PATH = BASE_DIR / "data" / "gold" / "jira_issues_gold.csv"

# Check if the json file exists:
# print("JSON_PATH:", JSON_PATH)
# print("The file exists?", JSON_PATH.exists())

# Bronze layer:
raw_data = read_local_json(JSON_PATH)
write_bronze_data(raw_data, BRONZE_OUTPUT_PATH)

# Silver layer:
bronze_data = read_bronze_data(BRONZE_OUTPUT_PATH)
silver_df = normalize_issues(bronze_data)
silver_df = transform_datetime_columns(silver_df)
write_silver_data(silver_df, SILVER_OUTPUT_PATH)

print("Layers Silver and Bronze created successfully!")

# Testing the silver step:
# print(silver_df.head())

gold_df = build_gold_layer(silver_df)
write_gold_data(gold_df, GOLD_PATH)

print("Pipeline completed: Bronze -> Silver -> Gold")

# Testing the gold step:
print(gold_df.head())
