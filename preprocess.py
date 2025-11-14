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
            "Activity status-Rejected/Approve",
            "SERVICE_NAME"
        ]

        for col in cols:
            df[col] = df[col].fillna("")

        return df

    def __add_exclusion_mask(self, df, excluded_conditions):
        condition_mask = pd.Series(False, index = df.index)
        for condition_type, conditions in excluded_conditions.items():
            if condition_type == 'eq_dict':
                for column, condition_dict in conditions.items():
                    for column_val, service_type in condition_dict.items():
                        condition_mask |= (df[column].eq(column_val)) & (df['SERVICE_NAME'].astype(str).str.lower().isin(service_type))
                        print("CONDITION MATCH COUNT when applied till eq_dict =", condition_mask.sum())
            elif condition_type == 'eq':
                for column, column_val in conditions.items():
                    condition_mask |= df[column].astype(str).str.lower().isin(column_val)
                    print("CONDITION MATCH COUNT when applied till eq =", condition_mask.sum())
            elif condition_type == 'not_eq':
                for column, column_val in conditions.items():
                    condition_mask |= ~df[column].astype(str).str.lower().isin(column_val)
                    print("CONDITION MATCH COUNT when applied till not_eq =", condition_mask.sum())
            elif condition_type == 'not_na':
                for column in conditions:
                    condition_mask |= df[column].notna()
                    print("CONDITION MATCH COUNT when applied till not_na =", condition_mask.sum())

        df['exclusion_mask'] = condition_mask
        return df

    def run_preprocess(self, df, excluded_conditions):
        df["Filter Applied(Including special providers)"] = [set() for _ in range(len(df))]
        df["Filter Applied"] = [set() for _ in range(len(df))]
        df["Filter Applied(Manual Verification)"] = [set() for _ in range(len(df))]
        df = self.__fix_datetime_cols(df=df)
        df = self.__fix_numerical_cols(df=df)
        df = self.__fix_nan_columns(df=df)

        #Calculate mask for all the claims to not be included in 2nd column so that no trigger gets applied in 2nd and 3rd column
        df = self.__add_exclusion_mask(df=df, excluded_conditions=excluded_conditions)
        return df
