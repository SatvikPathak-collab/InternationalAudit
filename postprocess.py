import pandas as pd
from loguru import logger

class PostProcessClass:
    def convert_list(self, df: pd.DataFrame):
        df["Filter Applied(Including special providers)"] = df["Filter Applied(Including special providers)"].apply(list)
        df["Filter Applied"] = df["Filter Applied"].apply(list)
        df["Filter Applied(Manual Verification)"] = df["Filter Applied(Manual Verification)"].apply(list)

        return df

    def add_exclusions(self, df: pd.DataFrame):
        trigger_mask = df["Filter Applied(Including special providers)"] != {}
        mask = trigger_mask & ~df["exclusion_mask"]
        df.loc[mask, "Filter Applied"] = df.loc[mask, "Filter Applied(Including special providers)"]

        return df

    def postprocess_df(self, df: pd.DataFrame):
        df = self.add_exclusions(df)
        df = self.convert_list(df)

        df = df.drop(columns = ['exclusion_mask'])

        return df