import os
import json
from pathlib import Path
from loguru import logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RULES_FILE = os.path.join(BASE_DIR, "rules_store.json")

def load_rules():
    if not os.path.exists(RULES_FILE):
        return {}  # or default rules structure

    with open(RULES_FILE, "r") as f:
        return json.load(f)

def save_rules(rules: dict):
    try:
        with open(RULES_FILE, "w", encoding="utf-8") as f:
            json.dump(rules, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to save rules: {e}")

# Loaded at startup
RULES_CONFIG = load_rules()
