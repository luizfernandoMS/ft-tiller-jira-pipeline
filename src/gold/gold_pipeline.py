import pandas as pd
from pathlib import Path
from src.gold.sla_calculation import (expected_sla_hours, resolution_time_hours, sla_met, get_national_holidays)


def build_gold_layer(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the gold layer by calculating SLA metrics based on the silver DataFrame.
    """
    gold_df = silver_df.copy()

    years = set(gold_df["created_at"].dt.year.dropna().astype(int).unique())

    if "resolved_at" in gold_df.columns:
        years.update(
            gold_df["resolved_at"]
            .dropna()
            .dt.year
            .astype(int)
            .unique()
)

    for year in years:
        get_national_holidays(year)

    # SLA attendance calculation
    gold_df["resolution_time_hours"] = gold_df.apply(
        lambda row: resolution_time_hours(
            row["created_at"],
            row["resolved_at"]
        ),
        axis=1
    )

    return gold_df

def write_gold_data(df: pd.DataFrame, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
