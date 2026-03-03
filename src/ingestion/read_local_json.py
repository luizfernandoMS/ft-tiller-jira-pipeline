import json
from pathlib import Path

# Função para leitura do arquivo .json local

# Argumentos: file_path = caminho arquivo .json
# Saída: Dicionário pyhton, conetúdo json carregado em memória *

def read_local_json(file_path) -> dict:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    if path.suffix.lower() != ".json":
        raise ValueError("O arquivo informado não é .json")

    with open(path, mode="r", encoding="utf-8") as file:
        return json.load(file)
