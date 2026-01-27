import json
from pathlib import Path

RULES_FILE = Path(__file__).parent / "rules_store.json"

def load_rules():
    with open(RULES_FILE, "r") as f:
        return json.load(f)


def save_rules(rules: dict):
    with open(RULES_FILE, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2, ensure_ascii=False)

# Loaded at startup
RULES_CONFIG = load_rules()
