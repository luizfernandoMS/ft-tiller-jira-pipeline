import json
from pathlib import Path


def write_bronze_data(data: dict, output_path: Path) -> None:

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode="w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
