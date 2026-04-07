import json


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)