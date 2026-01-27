import os
import pandas as pd
from loguru import logger
from typing import Any

from src.common.preprocess import PreprocessClass
from src.common.postprocess import PostProcessClass
from src.common.mapper import (
    excluded_conditions_preauth,
    excluded_conditions_claims,
)
from src.rules.rules import ComputeRule


class AuditOrchestrator:
    def __init__(self, data_type: str, insurer: str):
        self.data_type = data_type
        self.insurer = insurer
        self.excluded_conditions = self.load_exclusions(data_type)  

        self.preprocess_client = PreprocessClass()
        self.postprocess_client = PostProcessClass()

        self.RulesEnigne = ComputeRule(
            excluded_conditions=self.excluded_conditions,
            data_type=self.data_type
        )

    def load_exclusions(self, data_type: str):
        if data_type == "PreAuth":
            return excluded_conditions_preauth
        if data_type == "Claim":
            return excluded_conditions_claims
        raise ValueError(f"Invalid data type: {data_type}")

    def fetch_data(self, input_data: Any) -> pd.DataFrame:
        if isinstance(input_data, pd.DataFrame):
            return input_data.copy(deep=True)

        if isinstance(input_data, str):
            ext = os.path.splitext(input_data)[1].lower()
            if ext == ".csv":
                return pd.read_csv(input_data)
            if ext in [".xls", ".xlsx"]:
                return pd.read_excel(input_data)

        if hasattr(input_data, "name"):
            ext = os.path.splitext(input_data.name)[1].lower()
            if ext == ".csv":
                return pd.read_csv(input_data)
            if ext in [".xls", ".xlsx"]:
                return pd.read_excel(input_data)

        raise ValueError("Unsupported input type")

    def preprocess_input(self, df):
        return self.preprocess_client.run_preprocess(
            df=df,
            excluded_conditions=self.excluded_conditions,
        )

    def apply_rules(self, df):
        return self.RulesEnigne.apply_all_rules(df)

    def postprocess_result(self, df):
        return self.postprocess_client.postprocess_df(df)

    def execute(self, input_data) -> pd.DataFrame:
        logger.info("Audit execution started")

        df = self.fetch_data(input_data)
        df = self.preprocess_input(df)
        df = self.apply_rules(df)
        df = self.postprocess_result(df)

        df.reset_index(drop=True, inplace=True)
        logger.info("Audit execution completed")
        return df
