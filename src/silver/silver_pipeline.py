import json
from pathlib import Path
import pandas as pd

def read_bronze_data(bronze_path: Path) -> dict:
    
    # Leitura do arquivo .json (bronze)
    with open(bronze_path, mode = "r", encoding = "utf-8") as file:
        return json.load(file)
    
def normalize_issues(data: dict) -> pd.DataFrame:
    
    # Normalização do .json para Dataframe
    records = []

    for issue in data.get("issues", []):
        assignee = issue.get("assignee", [])
        timestamps = issue.get("timestamps", [])

        assignee_data = assignee[0] if assignee else {}
        timestamp_data = timestamps[0] if timestamps else {}

        record = {
            "issue_id": issue.get("id"),
            "issue_type": issue.get("issue_type"),
            "status": issue.get("status"),
            "priority": issue.get("priority"),
            "assignee_id": assignee_data.get("id"),
            "assignee_name": assignee_data.get("name"),
            "assignee_email": assignee_data.get("email"),
            "created_at": timestamp_data.get("created_at"),
            "resolved_at": timestamp_data.get("resolved_at")
        }

        records.append(record)

    return pd.DataFrame(records)

def write_silver_data(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

def transform_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
        Transform columns date to datetime format.
    """
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce", utc=True)
    return df