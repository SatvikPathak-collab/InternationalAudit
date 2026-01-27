import pandas as pd
from loguru import logger

from src.orchestrator.audit_orchestrator import AuditOrchestrator

def main(input_df, data_type: str, insurer_name: str) -> pd.DataFrame:
    orchestrator = AuditOrchestrator(data_type=data_type, insurer=insurer_name)
    result_df = orchestrator.execute(input_df)

    return result_df
