import pandas as pd
from loguru import logger

class PostProcessClass:
    def convert_list(self, df: pd.DataFrame):
        df["Filter Applied(Exclusions not Applied)"] = df["Filter Applied(Exclusions not Applied)"].apply(list)
        df["Filter Applied"] = df["Filter Applied"].apply(list)
        df["Filter Applied(Manual Verification Required)"] = df["Filter Applied(Manual Verification Required)"].apply(list)

        return df

    def add_exclusions(self, df: pd.DataFrame):
        trigger_mask = df["Filter Applied(Exclusions not Applied)"] != set()
        mask = trigger_mask & ~df["exclusion_mask"]
        df.loc[mask, "Filter Applied"] = df.loc[mask, "Filter Applied(Exclusions not Applied)"]

        return df

    def __drop_extra_columns(self, df: pd.DataFrame):
        drop_columns = [
            "exclusion_mask",
            "__approved"
        ]

        df = df.drop(columns = drop_columns)

        return df

    def postprocess_df(self, df: pd.DataFrame):
        steps = [
            self.add_exclusions,
            self.convert_list,
            self.__drop_extra_columns
        ]

        for step in steps:
            df = step(df)

        return df