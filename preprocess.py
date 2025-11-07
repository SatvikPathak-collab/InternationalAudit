import pandas as pd
from loguru import logger


class PreprocessClass:
    def __fix_datetime_cols(self, df):
        date_columns = [
            "MEMBER_INCEPTION_DATE",
            "POLICY_START_DATE",
            "POLICY_END_DATE",
            "RECEIVED_DATE",
            "ADDED_DATE",
            "COMPLETED_DATE",
            "ADMISSION_DATE",
            "DISCHARGE_DATE",
            "DOB",
            "CLAIM_COMPLETED_DATE_TIME",
            "AUDITED DATE",
            "DATE OF LMP(FOR MATERNITY ONLY)",
        ]

        date_format = "mixed"
        missing_columns: list[str] = []
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", format=date_format)
            else:
                missing_columns.append(col)

        if len(missing_columns) != 0:
            logger.warning(f"Missing datetime columns: {missing_columns}")
        return df

    def __fix_numerical_cols(self, df):
        numeric_columns: list[str] = [
            "MEMBER_AGE",
            "ACTIVITY_QUANTITY_APPROVED",
            "QUANTITY",
        ]
        missing_columns: list[str] = []
        for col in numeric_columns:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .round(decimals=0)
                    .astype("Int64")
                )
            else:
                missing_columns.append(col)

        if len(missing_columns) != 0:
            logger.warning(f"Missing numerical columns: {missing_columns}")

        return df

    def __fix_nan_columns(self, df):
        cols = [
            "Activity status-Rejected/Approve"
        ]

        for col in cols:
            df[col] = df[col].fillna("")

        return df

    def run_preprocess(self, df):
        df["Filter Applied(Including special providers)"] = df.apply(lambda _: [], axis=1)
        df["Filter Applied"] = df.apply(lambda _: [], axis=1)
        df = self.__fix_datetime_cols(df=df)
        df = self.__fix_numerical_cols(df=df)
        df = self.__fix_nan_columns(df=df)
        return df
