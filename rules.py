import functools

import pandas as pd
from loguru import logger
import re

def rule_method(active: bool = True):
    """
    Decorator factory.
    Use as: @rule_method(active=True)  # included
            @rule_method(active=False) # excluded
    Marks a function as a rule method and safely wraps it.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # logger.info(f"Running: {func.__name__}")
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                # Return DF unchanged on rule failure
                df = None
                if len(args) >= 2:  # method
                    df = args[1]
                elif len(args) >= 1:  # plain function
                    df = args[0]
                return df

        wrapper._is_rule_method = True
        wrapper._rule_active = active

        # Preserve metadata added by rule_details
        if hasattr(func, "case_type") and hasattr(func, "rule_scope"):
            wrapper.case_type = func.case_type
            wrapper.rule_scope = func.rule_scope

        return wrapper

    return decorator

def rule_details(case_type: str, rule_scope: str, review_req: str = "none"):
    """
    Attaches metadata to a rule: case type, scope, review requirement.
    """
    def decorator(func):
        func.case_type = case_type
        func.rule_scope = rule_scope
        func.review_req = review_req
        return func
    return decorator

class ComputeRule:
    """
    Base rule engine: evaluates inclusion, exclusion and extra conditions.
    """
    def __init__(self, data_type: str, excluded_conditions: dict = None):
        self.excluded_conditions = excluded_conditions
        self.data_type = data_type

    def _check_extra_condition(
        self, df: pd.DataFrame, extra_condition: list[dict]
    ) -> pd.Series:
        """
        Build mask for extra conditions (gt, lt, eq, isin, notna etc.).
        """
        mask = pd.Series([True] * len(df))
        approved_mask = df["__approved"]

        for condition in extra_condition:
            col: str = condition.get("column", "")
            conds: dict = condition.get("condition", {})
            for op, val in conds.items():
                # Numeric operators
                if op == "gte" and isinstance(val, (int, float)):
                    mask &= df[col] >= val
                elif op == "lte" and isinstance(val, (int, float)):
                    mask &= df[col] <= val
                elif op == "gt" and isinstance(val, (int, float)):
                    mask &= df[col] > val
                elif op == "lt" and isinstance(val, (int, float)):
                    mask &= df[col] < val
                    
                # Text matching operators
                elif op == "eq":
                    mask &= df[col].astype(str).str.lower() == str(val).lower()
                elif op == "neq":
                    mask &= df[col].astype(str).str.lower() != str(val).lower()

                # List membership
                elif op == "isin" and isinstance(val, list):
                    mask &= df[col].isin(val)
                elif op == "notin" and isinstance(val, list):
                    mask &= ~df[col].isin(val)

                # Null check
                elif op == "notna":
                    mask &= df[col].notna()
                
                # Invalid operator fallback
                else:
                    logger.warning(f"Invalid operation detected: {op}")
                    mask &= False

        mask &= approved_mask
        return mask

    def _compute_inclusion_exclusion(
        self,
        df: pd.DataFrame,
        trigger_name: str,
        inclusion: list[str] | list[dict] | None = None,
        exclusion: list[str] | list[dict] | None = None,
        inclusion_column: str | None = None,
        exclusion_column: str | None = None,
        extra_condition: list[dict] | None = None,
    ):
        """
        Final mask = inclusion AND NOT exclusion AND extra_conditions AND approved.
        Adds trigger_name to the set column when matched.
        """

        # Default masks
        is_inclusion_present = pd.Series([True] * len(df))
        is_exclusion_absent = pd.Series([True] * len(df))
        is_extra_conditions_present = pd.Series([True] * len(df))
        is_approved = df["__approved"]

        # At least one category must be used
        if inclusion is None and exclusion is None and extra_condition is None:
            raise RuntimeError(
                "Inclusion, Exclusion and Extra Condition can not be None at the same time."
            )

        # ---------------- Inclusion ----------------
        if inclusion:
            inclusion_masks = []

            # Old style: ["CODE1", "CODE2"] + single inclusion column
            if all(isinstance(i, str) for i in inclusion) and inclusion_column:
                if inclusion_column not in df.columns:
                    logger.warning(f"Inclusion column {inclusion_column} not present.")
                else:
                    lower_inclusion = [c.lower() for c in inclusion]
                    mask = df[inclusion_column].map(lambda x: str(x).lower() in lower_inclusion)
                    inclusion_masks.append(mask)

            # New style: [{"column":..., "codes":[...]}]
            elif all(isinstance(i, dict) for i in inclusion):
                for inc in inclusion:
                    col = inc.get("column")
                    codes = inc.get("codes", [])
                    if col not in df.columns:
                        logger.warning(f"Inclusion column {col} not present.")
                        continue
                    lower_codes = {c.lower() for c in codes}
                    mask = df[col].astype(str).str.lower().isin(lower_codes)
                    inclusion_masks.append(mask)

            # OR logic across all inclusion masks
            if inclusion_masks:
                is_inclusion_present = pd.concat(inclusion_masks, axis=1).any(axis=1)

        # ---------------- Extra Conditions ----------------
        if extra_condition:
            is_extra_conditions_present = self._check_extra_condition(
                df=df,
                extra_condition=extra_condition,
            )

        # ---------------- Exclusion ----------------
        if exclusion:
            exclusion_masks = []

            # Old style: ["CODE1", "CODE2"] + single exclusion column
            if all(isinstance(e, str) for e in exclusion) and exclusion_column:
                if exclusion_column not in df.columns:
                    logger.warning(f"Exclusion column {exclusion_column} not present.")
                else:
                    lower_exclusion = [c.lower() for c in exclusion]
                    mask = df[exclusion_column].map(lambda x: str(x).lower() not in lower_exclusion)
                    exclusion_masks.append(mask)

            # New style: [{"column":..., "codes":[...]}]
            elif all(isinstance(e, dict) for e in exclusion):
                for exc in exclusion:
                    col = exc.get("column")
                    codes = exc.get("codes", [])
                    if col not in df.columns:
                        logger.warning(f"Exclusion column {col} not present.")
                        continue
                    lower_codes = [c.lower() for c in codes]
                    mask = df[col].map(lambda x: str(x).lower() not in lower_codes)
                    exclusion_masks.append(mask)

            # AND logic across all exclusion masks
            if exclusion_masks:
                is_exclusion_absent = pd.concat(exclusion_masks, axis=1).all(axis=1)

        # ---------------- Final apply ----------------
        is_trigger_present = (
            is_inclusion_present & is_exclusion_absent & is_extra_conditions_present & is_approved
        )
        # Apply trigger by updating set column
        df.loc[is_trigger_present, "Filter Applied(Exclusions not Applied)"] = df.loc[
            is_trigger_present, "Filter Applied(Exclusions not Applied)"
        ].apply(lambda x: x.union({trigger_name}))

        logger.success(f"Successfull Run: {trigger_name}")
        return df

    def _apply_group_pair_rule(
        self,
        df: pd.DataFrame,
        trigger_name: str,
        pair_list: list[tuple[list[str], list[str]]],
        code_column: str = "ACTIVITY_CODE",
    ):
        """
        Generic engine to detect code combinations inside the same claim/pre-auth group.

        pair_list format:
            [
                ( ["84402"], ["84403"] ),                     # A → B (1-to-1)
                ( ["31231", "31505"], consultation_codes ),   # multiple A → multiple B
                ( ["84702", "84703"], ["81025"] )             # many A → single B
            ]

        Behavior:
            - Group rows by PRE_AUTH_NUMBER or CLAIM_NUMBER
            - Only considers APPROVED rows (__approved)
            - For each group: if ANY A exists AND ANY B exists → trigger
            - Only marks rows whose code is in A or B (not entire group)
        """

        # 1. Determine grouping column
        pre_auth_col = (
            "PRE_AUTH_NUMBER" if "PRE_AUTH_NUMBER" in df.columns else "PREAUTH_NUMBER"
        )
        df["_GROUP_KEY"] = df[pre_auth_col].where(
            df[pre_auth_col].notna(), df["CLAIM_NUMBER"]
        )

        # 2. Iterate per claim/preauth group
        for claim_id, group in df.groupby("_GROUP_KEY"):
            approved_codes = set(group.loc[group["__approved"], code_column].astype(str))

            for A_list, B_list in pair_list:
                A_set = set(map(str, A_list))
                B_set = set(map(str, B_list))

                has_A = not approved_codes.isdisjoint(A_set)
                has_B = not approved_codes.isdisjoint(B_set)

                if has_A and has_B:
                    # mark only rows belonging to A_set or B_set
                    mask = (
                        (df["_GROUP_KEY"] == claim_id)
                        & df[code_column].astype(str).isin(A_set | B_set)
                        & df["__approved"]
                    )

                    df.loc[mask, "Filter Applied(Exclusions not Applied)"] = df.loc[
                        mask, "Filter Applied(Exclusions not Applied)"
                    ].apply(lambda x: x.union({trigger_name}))

                    break  # stop checking other pairs in this claim

        df.drop(columns=["_GROUP_KEY"], inplace=True)
        return df

    # def apply_all_rules(self, df):
    #     df = df.copy(deep=True)
    #     df = self.general_exclusion_hiv(df=df)
    #     df = self.general_exclusion_zirconium_crown(df=df)
    #     df = self.covid(df=df)
    #     df = self.hpv_screening(df=df)
    #     df = self.alopecia(df=df)
    #     df = self.more_than_one_quantity(df=df)
    #     df = self.sick_leave(df=df)
    #     df = self.pap_smear_age_restriction(df=df)
    #     df = self.zinc_general_exclusion(df=df)
    #     df = self.betadine_mouth_wash(df=df)
    #     df = self.cough_syrup_high_quantity(df=df)
    #     df = self.nasal_syrup_high_quantity(df=df)
    #     df = self.nebulizer_high_quantity(df=df)
    #     df = self.hpyrol_antibody(df=df)
    #     df = self.gardenia_large_dressing(df=df)
    #     df = self.sidra_medical_male(df=df)
    #     return df
    def apply_all_rules_preauth(self, df: pd.DataFrame):
        for name in dir(self):
            method = getattr(self, name)
            if callable(method) and getattr(method, "_is_rule_method", False):
                if getattr(method, "_rule_active", True):
                    if hasattr(method, "case_type") and method.case_type not in ["preauth", "both"]:
                        continue
                    df = method(df)
        return df

    def apply_all_rules_claim(self, df: pd.DataFrame):
        for name in dir(self):
            method = getattr(self, name)
            if callable(method) and getattr(method, "_is_rule_method", False):
                if getattr(method, "_rule_active", True):
                    if hasattr(method, "case_type") and method.case_type not in ["claim", "both"]:
                        continue
                    df = method(df)
        return df

    def apply_all_rules(self, df: pd.DataFrame):
        if self.data_type == "PreAuth":
            return self.apply_all_rules_preauth(df)
        elif self.data_type == "Claim":
            return self.apply_all_rules_claim(df)
        else:
            logger.error(f"Invalid case_type input: {self.data_type}. Returning original df")
            return df

    def apply_manual_trigger(self, df: pd.DataFrame, trigger_name: str):
        trigger_mask = df['Filter Applied(Exclusions not Applied)'].map(lambda x : len(x) > 0 and trigger_name in x)
        mask = trigger_mask & ~df['exclusion_mask']
        df.loc[mask, 'Filter Applied(Manual Verification Required)'] = df.loc[mask, 'Filter Applied(Manual Verification Required)'].apply(lambda x : x.union({trigger_name}))

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def general_exclusion_hiv(self, df):
        inclusion = ["86689", "86701", "86702"]
        exclusion = ["OUT-PATIENT MATERNITY"]
        trigger_name = "General exclusion - HIV"
        df = self._compute_inclusion_exclusion(
            inclusion=inclusion,
            df=df,
            trigger_name=trigger_name,
            exclusion=exclusion,
            inclusion_column="ACTIVITY_CODE",
            exclusion_column="BENEFIT_TYPE",
        )

        return df

    @rule_details("both", "account specific", "manual")
    @rule_method(active=True)
    def general_exclusion_zirconium_crown(self, df):
        trigger_name = "General exclusion-Zirconium Crown"
        inclusion = ["D2720", "D2750"]
        exclusion = [
            "AK/HC/00093/5/1",
            "AK/HC/00093/5/2",
            "AK/HC/00093/5/3",
            "AK/HC/00093/5/4",
            "AK/HC/00093/5/5",
            "AK/HC/00093/5/6",
            "AK/HC/00093/5/7",
            "AK/HC/00143/1/1",
            "AK/HC/00143/0/1",
            "AK/HC/00143/2/1",
            "AK/HC/00153/0/1",
            "AK/HC/00153/1/1",
            "AK/HC/00093/6/1",
            "AK/HC/00093/6/2",
            "AK/HC/00093/6/3",
            "AK/HC/00093/6/4",
            "AK/HC/00093/6/5",
            "AK/HC/00093/6/6",
            "AK/HC/00093/6/8"
        ]
        df = self._compute_inclusion_exclusion(
            inclusion=inclusion,
            exclusion=exclusion,
            df=df,
            trigger_name=trigger_name,
            inclusion_column="ACTIVITY_CODE",
            exclusion_column="POLICY_NUMBER",
        )

        df = self.apply_manual_trigger(df, trigger_name)
        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def covid(self, df):
        icd_code = [
            "U07.1",
            "U09.9",
            "Z11.52",
            "Z20.822",
            "Z28.310",
            "Z28.311",
            "Z86.16",
        ]
        exclusion = [
            "AK/HC/00093/5/1",
            "AK/HC/00093/5/2",
            "AK/HC/00093/5/3",
            "AK/HC/00093/5/4",
            "AK/HC/00093/5/5",
            "AK/HC/00093/5/6",
            "AK/HC/00093/5/7",
        ]
        trigger_name = "General exclusion-COVID"
        df = self._compute_inclusion_exclusion(
            inclusion=icd_code,
            exclusion=exclusion,
            df=df,
            trigger_name=trigger_name,
            inclusion_column="PRIMARY_ICD_CODE",
            exclusion_column="POLICY_NUMBER",
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def hpv_screening(self, df):
        inclusion = ["0096U", "0500T", "0429U", "87623", "87624", "87625", "0354U"]
        trigger_name = "General exclusion-HPV SCREENING"
        inclusion_column = "ACTIVITY_CODE"
        df = self._compute_inclusion_exclusion(
            inclusion=inclusion,
            df=df,
            trigger_name=trigger_name,
            inclusion_column=inclusion_column,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def alopecia(self, df):
        icd_inclusion = [
            "A51.32", "L63.0", "L63.1", "L64.0", "L64.8", "L64.9",
            "L65.2", "L66.8", "L66.9", "Q84.0", "L66.12", "L66.81",
            "L66.89", "L64"
        ]
        trigger_name = "General exclusion-ALOPECIA"
        df = self._compute_inclusion_exclusion(
            inclusion=icd_inclusion,
            df=df,
            trigger_name=trigger_name,
            inclusion_column="PRIMARY_ICD_CODE",
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def more_than_one_quantity(self, df):
        inclusion = [
            "99202", "99203", "99204", "99205", "99211", "99212", "99213", "99214", "99215",
            "99221", "99222", "99223", "99231", "99232", "99233", "99234", "99235", "99236",
            "99238", "99239", "99242", "99243", "99244", "99245", "99252", "99253", "99254",
            "99255", "99281", "99282", "99283", "99284", "99285", "99288", "99291", "99292",
            "99304", "99305", "99306", "99307", "99308", "99309", "99310", "99315", "99316",
            "99341", "99342", "99344", "99345", "99347", "99348", "99349", "99350", "99358",
            "99359", "99360", "99366", "99367", "99368", "99374", "99375", "99377", "99378",
            "99379", "99380", "99381", "99382", "99383", "99384", "99385", "99386", "99387",
            "99391", "99392", "99393", "99394", "99395", "99396", "99397", "99401", "99402",
            "99403", "99404", "99406", "99407", "99408", "99409", "99411", "99412", "99429",
            "99441", "99442", "99443", "99450", "99455", "99456", "99460", "99461", "99462",
            "99463", "99464", "99465", "99466", "99467", "99468", "99469", "99471", "99472",
            "99475", "99476", "99477", "99478", "99479", "99480", "99499", "99500", "99501",
            "99502", "99503", "99504", "99505", "99506", "99507", "99509", "99510", "99511",
            "99512", "99600", "99601", "99602", "99605", "99606", "99607", "10", "61.08",
            "D9310", "10.01", "9", "63", "11.01", "11", "99242", "99241", "61.03", "99253",
            "99243", "10.02", "22", "D0160", "88321", "21", "61.04", "61.01", "61.06", "61.02",
            "61.07", "61.09", "61.12", "63.01", "63.02", "63.03", "63.04", "63.05", "23",
            "61.05", "9.01", "9.02", "11.02", "13",
            "70450", "70460", "70470", "70480", "70481", "70482", "70486", "70487", "70488",
            "70490", "70491", "70492", "71250", "71260", "71270", "72125", "72126", "72127",
            "72128", "72129", "72130", "72131", "72132", "72133", "74150", "74160", "74170",
            "74176", "74177", "74178", "72191", "72192", "72193", "70496", "70498", "71275",
            "73706", "74174", "70551", "70552", "70553", "70540", "70542", "70543", "72141",
            "72142", "72156", "72146", "72147", "72157", "72148", "72149", "72158", "73218",
            "73219", "73220", "73721", "73722", "73723", "74181", "74182", "74183", "72195",
            "72196", "72197", "75557", "75561", "77046", "77047", "77048", "77049", "71271",
            "74712", "74713", "75580", "76391", "70544", "70545", "70546", "70547", "70548",
            "70549", "70554", "72194", "72198", "73700", "73701", "73702", "73718", "73719",
            "74185", "75559", "75563", "77011", "77012", "77013", "77014", "77021", "77022",
        ]
        extra_conditions: list[dict] = [
            {"column": "ACTIVITY_QUANTITY_APPROVED", "condition": {"gt": 1}}
        ]
        trigger_name = "Quantity More Than 1"
        df = self._compute_inclusion_exclusion(
            inclusion=inclusion,
            df=df,
            trigger_name=trigger_name,
            inclusion_column="ACTIVITY_CODE",
            extra_condition=extra_conditions,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def sick_leave(self, df):
        if "PRESENTING_COMPLAINTS" not in df.columns:
            logger.error("Presenting Complaints not in data.")
            return df

        trigger_name = "General exclusion - Sick Leave"

        # Normalize keyword search: prepare a single OR-regex string
        keywords = ["Sick leave", "Sick note", "Medical note"]
        pattern = "|".join(k.lower() for k in keywords)

        # Build temporary boolean flag column for matching text
        temp_col = "_tmp_sick_leave_flag"

        # Flag entries where presenting complaint contains any keyword (case-insensitive)
        df[temp_col] = df["PRESENTING_COMPLAINTS"].astype(str).str.lower().str.contains(
            pattern, na=False
        )

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}}
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns=[temp_col])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def pap_smear_age_restriction(self, df):
        trigger_name: str = "PAP Smear Age Restriction"
        inclusion = [
            "88141", "88142", "88143", "88147", "88148", "88150",
            "88152", "88153", "88155", "88164", "88165", "88166",
            "88167", "88174", "88175", "88177",
        ]
        exclusion: list[str] = [
            "AL EMADI HOSPITAL",
            "AL EMADI HOSPITAL CLINICS - NORTH",
        ]
        inclusion_column: str = "ACTIVITY_CODE"
        exclusion_column: str = "PROVIDER_NAME"

        # Temporary flag: True if age < 24 or > 65
        df = df.copy()
        df[inclusion_column] = df[inclusion_column].astype(str)
        df["AGE_OUTSIDE_24_65"] = (df["MEMBER_AGE"] < 24) | (df["MEMBER_AGE"] > 65)

        extra_conditions: list[dict] = [
            {"column": "AGE_OUTSIDE_24_65", "condition": {"eq": True}}
        ]

        df = self._compute_inclusion_exclusion(
            inclusion=inclusion,
            exclusion=exclusion,
            df=df,
            trigger_name=trigger_name,
            inclusion_column=inclusion_column,
            exclusion_column=exclusion_column,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns = ["AGE_OUTSIDE_24_65"])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def zinc_general_exclusion(self, df):
        trigger_name: str = "Zinc-General Exclusion"
        inclusion: list[str] = ["84630"]
        inclusion_column = "ACTIVITY_CODE"

        extra_conditions: list[dict] = [
            {"column": "BENEFIT_TYPE", "condition": {"eq": "OUT-PATIENT"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_conditions,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def mouth_wash(self, df):
        trigger_name: str = "Mouth wash"
        inclusion: list[str] = ["0000-000000-001427"]
        inclusion_column: str = "ACTIVITY_CODE"

        # Temporary column for a special extra exclusion scenario
        temp_col = "_mouth_wash_extra_exclusion"
        df[temp_col] = "false"

        # Special override: if BOTH policy & corporate match → exclude
        mask = (
            (df["POLICY_NUMBER"] == "AK/HC/00232/0/1") &
            (df["CORPORATE_NAME"] == "QATARENERGY LNG")
        )
        df.loc[mask, temp_col] = "true"

        exclusion : list[dict] = [
            {"column": "POLICY_NUMBER", "codes": ["AK/HC/00156/0/1"]},
            {"column": "CORPORATE_NAME", "codes": ["QAFCO", "QATARENERGY"]},
            {"column": temp_col, "codes": ["true"]}
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            exclusion=exclusion,
            inclusion_column=inclusion_column,
        )

        df = df.drop(columns = [temp_col])
        return df

    @rule_details("both", "generic", "manual")
    @rule_method(active=True)
    def cough_syrup_high_quantity(self, df):
        trigger_name: str = "Cough Syrup-Quantity 2"

        # Regex pattern covering brand names and common variants of cough syrups
        cough_pattern = r"(cough syrup|cough syp|koflet|propolsaft|zecuf|toplexil|dextrokuf)"

        # Temporary boolean column marking rows that match cough syrup products
        df["_syrup_flag"] = (
            df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains(cough_pattern, case = False, na = False) |
            df["ACTIVITY_DESCRIPTION"].astype(str).str.contains(cough_pattern, case = False, na = False)
        )

        extra_conditions: list[dict] = [
            {"column": "_syrup_flag", "condition": {"eq": True}},
            {"column": "ACTIVITY_QUANTITY_APPROVED", "condition": {"gt": 2}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df, trigger_name=trigger_name, extra_condition=extra_conditions
        )

        df = self.apply_manual_trigger(df, trigger_name)
        df = df.drop(columns=["_syrup_flag"])
        return df

    @rule_details("both", "universal", "manual")
    @rule_method(active=True)
    def nasal_syrup_high_quantity(self, df):
        trigger_name: str = "Nasal Spray-Quantity 2"

        df["_nasal_spray_flag"] = (
            (
                df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains(str("nasal"), case = False, na = False) & 
                df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains(str("spray"), case = False, na = False)
            ) |
            (
                df["ACTIVITY_DESCRIPTION"].astype(str).str.contains(str("nasal"), case = False, na = False) & 
                df["ACTIVITY_DESCRIPTION"].astype(str).str.contains(str("spray"), case = False, na = False)
            )
        )

        extra_conditions: list[dict] = [
            {"column": "_nasal_spray_flag", "condition": {"eq": True}},
            {"column": "ACTIVITY_QUANTITY_APPROVED", "condition": {"gt": 2}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df, trigger_name=trigger_name, extra_condition=extra_conditions
        )

        df = self.apply_manual_trigger(df, trigger_name)
        df = df.drop(columns=["_nasal_spray_flag"])
        return df

    @rule_details("both", "generic", "manual")
    @rule_method(active=True)
    def nebulizer_high_quantity(self, df):
        trigger_name: str = "Nebulizer- Quantity 1"
        inclusion: list[str] = ["94640"]
        inclusion_column: str = "ACTIVITY_CODE"

        # Age-based threshold logic
        age_greater_18_mask = df["MEMBER_AGE"] >= 18
        # Temporary boolean condition:
        #   - Adults: quantity > 1
        #   - Children: quantity > 2
        df["_nebulizer"] = (
            (age_greater_18_mask & (df["ACTIVITY_QUANTITY_APPROVED"] > 1)) | 
            (~age_greater_18_mask & (df["ACTIVITY_QUANTITY_APPROVED"] > 2))
        )

        extra_conditions: list[dict] = [
            {"column": "_nebulizer", "condition": {"eq": True}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_conditions,
        )

        df = self.apply_manual_trigger(df, trigger_name)
        df = df.drop(columns = "_nebulizer")
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def hpyrol_antibody(self, df):
        trigger_name: str = "H-Pylori Antibody not covered"
        inclusion: list[str] = ["86677"]
        inclusion_column: str = "ACTIVITY_CODE"
        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def gardenia_large_dressing(self, df):
        trigger_name: str = "Gardenia-Large Dressing not covered"

        df["_large_dressing_flag"] = df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains(str("dressing large"), case = False, na = False)

        extra_conditions: list[dict] = [
            {"column": "_large_dressing_flag", "condition": {"eq": True}},
            {
                "column": "PROVIDER_NAME",
                "condition": {"eq": "GARDENIA MEDICAL CENTER"},
            },
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns = ["_large_dressing_flag"])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def sidra_medical_male(self, df):
        trigger_name: str = "Sidra Medical Male Above 17 Years"

        # Create temporary flag for identifying Sidra Medical provider
        df["_sidra_medical_flag"] = df["PROVIDER_NAME"].astype(str).str.contains(str("sidra medical"), case = False, na = False)

        extra_conditions: list[dict] = [
            {"column": "_sidra_medical_flag", "condition": {"eq": True}},
            {"column": "MEMBER_AGE", "condition": {"gt": 17}},
            {"column": "GENDER", "condition": {"eq": "Male"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns = ["_sidra_medical_flag"])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def glucosamine_quantity(self, df):
        trigger_name: str = "Quantity more than 2"
        glucosamine_codes: list[str] = [
            "0000-000000-003857",
            "0000-000000-001538",
            "0000-000000-000937",
            "0000-000000-001516",
            "0000-000000-002250",
            "1000-475401-0391",
            "1553-529901-0061",
            "0000-000000-003700",
            "0000-000000-001528",
            "0000-000000-002628",
            "0000-000000-003843",
        ]
        code_mask = df["ACTIVITY_CODE"].isin(glucosamine_codes)

        glucosamine_keywords: list[str] = [
            "JOINT PLUS",
            "JOINTPLAN",
            "JOINT PLAN",
            "GLUCOSAMINE",
            "HEALTH WISE JOINT PLUS",
            "HEALTHWISE JOINT PLUS",
        ]
        keyword_mask = df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains("|".join(glucosamine_keywords), case = False, na = False)

        # Temporary flag identifying glucosamine products
        df["_glucosamine_flag"] = code_mask | keyword_mask

        extra_conditions: list[dict] = [
            {"column": "_glucosamine_flag", "condition": {"eq": True}},
            {"column": "ACTIVITY_QUANTITY_APPROVED", "condition": {"gt": 2}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns = ["_glucosamine_flag"])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def apply_crp_esr_rule(self, df):
        """
        CRP & ESR must both be present in the same claim/pre-auth.
        Uses the generic group-pair template.
        """
        trigger_name = "CRP & ESR in Same claim / pre-auth"

        # Convert old pair list → template format
        pair_list = [
            (["85651", "85652"], ["86140", "86141"])
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=trigger_name,
            pair_list=pair_list,
            code_column="ACTIVITY_CODE",
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def general_exclusion_probiotic(self, df):
        trigger_name: str = "General Exclusion-Probiotics"
        code: list[str] = [
            "0000-000000-000683",
            "0000-000000-001315",
            "2845-133702-2401-B",
            "0170-502203-4021",
            "0000-000000-000682",
        ]
        code_mask = df["ACTIVITY_CODE"].isin(code)

        probiotic_pattern = r"(PROBIOTIC|ENTEROGERMINA)"
        keyword_mask = df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains(probiotic_pattern, case = False, na = False)

        # Temporary flag identifying probiotic rows
        df["_probiotic"] = code_mask | keyword_mask

        extra_conditions: list[dict] = [
            {"column": "_probiotic", "condition": {"eq": True}}
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions
        )

        df = df.drop(columns = ["_probiotic"])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def not_payable_ondansetron(self, df):
        trigger_name: str = "Ondansetron - Payable only in Cancer ICDs."
        code: list[str] = [
            "0000-000000-003766",
            "0000-000000-002029",
            "0000-000000-003721",
            "0000-000000-002030",
            "0000-000000-003394",
            "0000-000000-003395",
            "0000-000000-003209",
            "0000-000000-003211",
            "0000-000000-003210",
            "0000-000000-003212",
            "6639-627604-1161",
            "0000-000000-001584",
            "0000-000000-001586",
            "0006-238802-1172-1",
            "0006-238802-1172-2",
            "0006-238803-1171",
            "0006-238803-1171-A",
            "0063-238801-0511",
            "0006-238804-2481",
            "0006-238802-1173",
            "0050-238802-1171",
            "0063-238801-0511-A",
        ]
        code_mask = df["ACTIVITY_CODE"].isin(code)

        keyword = [
            "Ondansetron",
            "zofran",
            "Vomiran",
            "Vominor",
            "Vomet",
            "Ondavell",
            "Ondan",
            "Kromafina",
            "Zoron",
            "Emeset"
        ]
        keyword_mask = df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains("|".join(keyword), case = False, na = False)

        # Temporary flag for ondansetron detection
        df["_ondansetron"] = code_mask | keyword_mask

        extra_conditions: list[dict] = [
            {"column": "_ondansetron", "condition": {"eq": True}}
        ]

        exclusion: list[dict] = [
            {"codes": ["0205-229501-1171"], "column": "ACTIVITY_CODE"},
            {"codes": ["VOMINORE"], "column": "ACTIVITY_INTERNAL_DESCRIPTION"}
        ]
        
        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions,
            exclusion=exclusion
        )

        df = df.drop(columns = ["_ondansetron"])
        return df

    @rule_details("both", "account specific", "manual")
    @rule_method(active=True)
    def not_payable_semaglutide(self, df):
        trigger_name: str = "WEGOVY - Not Payable"
        inclusion: list[str] = [
            "0000-000000-003378",
            "0000-000000-003379",
            "0000-000000-003380",
            "0000-000000-003423",
            "0000-000000-003381",
        ]
        inclusion_column: str = "ACTIVITY_CODE"
        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
        )

        df = self.apply_manual_trigger(df, trigger_name)
        return df

    @rule_details("both", "account specific", "manual")
    @rule_method(active=True)
    def diabetic_semaglutide(self, df):
        trigger_name: str = "OZEMPIC - To verify DM history and approve"
        inclusion: list[str] = [
            "4788-782701-1021",
            "4788-782701-1023",
            "4788-782701-1025",
        ]
        inclusion_column: str = "ACTIVITY_CODE"
        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
        )

        df = self.apply_manual_trigger(df, trigger_name)
        return df

    @rule_details("claim", "account specific")
    @rule_method(active=True)
    def biopsy_pa_available(self, df):
        trigger_name: str = "Service not payable without Preauth"
        inclusion = [
            "11101","11102","11103","11104","11105","11106","11107",
            "19081","19082","19083","19084","19085","19086",
            "19100","19101","19102","19103",
            "47000","47001","47100",
            "32400","32402","32405","32408",
            "32607","32608","32609",
            "32096","32097","32098",
            "55700","55705","55706",
            "50200","50205",
            "43239","45380","44389",
            "20220","20225","20240","20245","20250","20251",
            "38220","38221","38222",
            "38500","38505","38510","38520","38525","38530","38531",
        ]
        inclusion_column: str = "ACTIVITY_CODE"

        pre_auth_col = "PRE_AUTH_NUMBER" if "PRE_AUTH_NUMBER" in df.columns else "PREAUTH_NUMBER"

        # Preauth missing condition (no PA in column & no PA mentioned in text)
        df["_pre_auth_missing"] = (
            df[pre_auth_col].isna()
            | df[pre_auth_col].astype(str).str.strip().eq("")
        ) & (
            ~df["PRESENTING_COMPLAINTS"]
                .astype(str)
                .str.contains(r'PA:?\s*\d+', regex=True, na=False)
        )
        
        extra_conditions: list[dict] = [
            {"column": "_pre_auth_missing", "condition": {"eq": True}},
        ]
        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns = ["_pre_auth_missing"])
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def beta_hcg_urine_pregnancy(self, df):
        """
        Beta HCG + Urine Pregnancy must both be present
        in the same claim/pre-auth number.
        """

        trigger_name = "Beta HCG + Urine Pregnancy Test"

        # Old pairs → template structure
        pair_list = [
            (["84702", "84703", "84704"], ["81025"])
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=trigger_name,
            pair_list=pair_list,
            code_column="ACTIVITY_CODE",
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def capsaicin_belladona_non_payable(self, df):
        trigger_name: str = "Capsaicin / Belladona - Not Payable"

        codes = ["C9268", "J7335", "J7336"]
        code_mask = df["ACTIVITY_CODE"].isin(codes)

        description = ["Capidol patch", "Capsicum Plaster"]
        description_mask = df["ACTIVITY_INTERNAL_DESCRIPTION"].astype(str).str.contains("|".join(description), case = False, na = False)

        # Temporary flag capturing both conditions
        df["_capsaicin_belladona"] = code_mask | description_mask

        extra_conditions: list[dict] = [
            {"column": "_capsaicin_belladona", "condition": {"eq": True}},
        ]
        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_conditions,
        )

        df = df.drop(columns = ["_capsaicin_belladona"])
        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def heatpad_non_payable(self, df):
        trigger_name: str = "Heat Pad - Not Payable"

        inclusion_codes: list[str] = [
            "A9273",
            "E0210",
            "E0215",
            "E0217",
            "E0221",
            "E0238",
            "E0490",
            "A4639"
        ]

        inclusion: list[dict] = [
            {"column": "ACTIVITY_CODE", "codes": inclusion_codes},
            {"column": "ACTIVITY_INTERNAL_DESCRIPTION", "codes": ["Heat pad"]},
            {"column": "ACTIVITY_DESCRIPTION", "codes": ["Heat pad"]},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=False)
    def steam_inhaler_non_payable(self, df):
        trigger_name: str = "Steam Inhaler - Not Payable"

        inclusion_codes: list[str] = [
            "94644",
            "J3535",
            "S8100",
            "S8101",
            "A4627"
        ]

        inclusion : list[dict] = [
            {"column": "ACTIVITY_CODE", "codes": inclusion_codes},
            {"column": "ACTIVITY_INTERNAL_DESCRIPTION", "codes": ["Steam Inhaler"]},
            {"column": "ACTIVITY_DESCRIPTION", "codes": ["Steam Inhaler"]},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def hot_water_bag_non_payable(self, df):
        trigger_name: str = "Hot Water Bag - Not Payable"

        inclusion_codes: list[str] = [
            "E0220",
            "A9273"
        ]

        inclusion : list[dict] = [
            {"column": "ACTIVITY_CODE", "codes": inclusion_codes},
            {"column": "ACTIVITY_INTERNAL_DESCRIPTION", "codes": ["Hot Water Bag"]},
            {"column": "ACTIVITY_DESCRIPTION", "codes": ["Hot Water Bag"]},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def pillows_non_payable(self, df):
        trigger_name: str = "Pillows - Not Payable"

        inclusion_codes: list[str] = [
            "E0190"
        ]

        inclusion : list[dict] = [
            {"column": "ACTIVITY_CODE", "codes": inclusion_codes},
            {"column": "ACTIVITY_INTERNAL_DESCRIPTION", "codes": ["Pillows"]},
            {"column": "ACTIVITY_DESCRIPTION", "codes": ["Pillows"]},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def antiseptics_non_payable(self, df):
        trigger_name: str = "Antispetics - Not Payable"

        inclusion: list[str] = [
            "A4674",
            "0000-000000-000127",
            "A4248",
            "6030F"
        ]
        inclusion_column: str = "ACTIVITY_CODE"

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def diapers_non_payable(self, df):
        trigger_name: str = "Diapers - Not Payable"

        inclusion_codes: list[str] = [
            "T4543",
            "T4521",
            "T4522",
            "T4523",
            "T4524",
            "T4529",
            "T4530",
            "T4538",
            "T4539",
            "A4520"
        ]

        inclusion : list[dict] = [
            {"column": "ACTIVITY_CODE", "codes": inclusion_codes},
            {"column": "ACTIVITY_INTERNAL_DESCRIPTION", "codes": ["Diapers"]},
            {"column": "ACTIVITY_DESCRIPTION", "codes": ["Diapers"]},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def moisturizer_non_payable(self, df):
        trigger_name: str = "Moisturizer - Not Payable"

        inclusion_codes: list[str] = [
            "A6250",
        ]

        inclusion : list[dict] = [
            {"column": "ACTIVITY_CODE", "codes": inclusion_codes},
            {"column": "ACTIVITY_INTERNAL_DESCRIPTION", "codes": ["Moisturizer"]},
            {"column": "ACTIVITY_DESCRIPTION", "codes": ["Moisturizer"]},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def al_abdulghani_motors(self, df):
        trigger_name: str = "AL ABDULGHANI MOTORS - Dental Procedures & Consultation"

        policy_numbers: list[str] = [
            "AK/HC/00142/3/3A",
            "AK/HC/00142/3/3A",
            "AK/HC/00142/3/3"
        ]

        extra_condition : list[dict] = [
            {"column": "POLICY_NUMBER", "condition": {"isin": policy_numbers}},
            {"column": "CORPORATE_NAME", "condition": {"eq": "AL ABDULGHANI MOTORS"}},
            {"column": "BENEFIT_TYPE", "condition": {"eq": "DENTAL"}},
        ]

        exclusion : list[str] = [
            "KINGS DENTAL CENTER - AL WAKRA",
            "KINGS DENTAL CENTER - AL HILAL",
            "KINGS DENTAL CENTER - AL KHOR",
            "DIVINE DENTAL CLINIC",
            "FAIRVIEW DENTAL CARE",
            "CRAFT DENTAL CENTER",
            "GREEN HEALTH CLINIC",
            "ELEGANT SMILE DENTAL CLINIC",
            "THE PEARL DERMATOLOGY AND LASER CENTER- AL WAAB",
            "THE PEARL DERMATOLOGY AND LASER CENTER- AL WAKRA",
            "THE PEARL DERMATOLOGY AND LASER CENTER- UMM SALAL",
            "AL ABEER DENTAL CENTRE - MUATHER",
            "SHAM DENTAL",
            "DENTAL ASIAN TOWN",
            # These may be added later
            # "KINGS DENTAL CENTER - AL HILAL",
            # "KINGS DENTAL CENTER - AL KHOR",
            # "DIVINE DENTAL CLINIC",
            # "THE PEARL DERMATOLOGY AND LASER CENTER- AL WAAB",
            # "THE PEARL DERMATOLOGY AND LASER CENTER- AL WAKRA",
            # "THE PEARL DERMATOLOGY AND LASER CENTER- UMM SALAL",
            # "KINGS DENTAL CENTER - AL WAKRA",
            # "SHAM DENTAL CENTER",
            # "ABEER DENTAL CENTER - MUATHER",
        ]

        exclusion_column = "PROVIDER_NAME"

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            extra_condition=extra_condition,
            exclusion=exclusion,
            exclusion_column=exclusion_column
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_maternity_cmv(self, df):
        trigger_name: str = "OP Maternity - CMV"

        inclusion: list[str] = [
            "86644",
            "86645",
            "87495",
            "87496",
            "87497",
        ]

        inclusion_column = "ACTIVITY_CODE"

        extra_condition : list[dict] = [
            {"column": "BENEFIT_TYPE", "condition": {"eq": "OUT-PATIENT MATERNITY"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_condition,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_maternity_toxoplasma(self, df):
        trigger_name: str = "OP Maternity – TOXOPLASMA"

        inclusion: list[str] = [
            "86777",
            "86778",
        ]

        inclusion_column = "ACTIVITY_CODE"

        extra_condition : list[dict] = [
            {"column": "BENEFIT_TYPE", "condition": {"eq": "OUT-PATIENT MATERNITY"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_condition,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_maternity_qatar_charity(self, df):
        trigger_name: str = "OP Maternity – Qatar Charity"

        inclusion: list[str] = [
            "AK/HC/00191/0/3",
            "AK/HC/00191/1/3"
        ]

        inclusion_column = "POLICY_NUMBER"

        extra_condition : list[dict] = [
            {"column": "CORPORATE_NAME", "condition": {"eq": "Qatar Charity"}},
            {"column": "BENEFIT_TYPE", "condition": {"eq": "OUT-PATIENT MATERNITY"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_condition,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_optical_qatar_charity(self, df):
        trigger_name: str = "OP Optical – Qatar Charity"

        inclusion: list[str] = [
            "AK/HC/00191/0/3",
            "AK/HC/00191/1/3"
        ]

        inclusion_column = "POLICY_NUMBER"

        exclusion: list[str] = [
            "Yateem Optician",
            "YATEEM OPTICIAN - MUSHEIREB",
            "YATEEM OPTICIAN - RAMADA",
            "YATEEM OPTICIAN- DOHA MALL",
            "YATEEM OPTICIAN- RAYYAN",
            "YATEEM OPTICIAN-AL NASSER",
            "YATEEM OPTICIAN-CITY CENTER",
            "YATEEM OPTICIAN-DOHA FESTIVAL CITY",
            "YATEEM OPTICIAN-KHOR",
            "YATEEM OPTICIAN-LAGOONA",
            "YATEEM OPTICIAN-MALL OF QATAR",
            "YATEEM OPTICIAN-VENDOME",
            "YATEEM OPTICIAN-WAKRA",
            "QATAR OPTICS - AL KHOR",
            "QATAR OPTICS - CITY CENTER",
            "QATAR OPTICS - HYATT PLAZA",
            "QATAR OPTICS - LANDMARK",
            "QATAR OPTICS - MALL OF QATAR",
            "QATAR OPTICS - SALWA ROAD",
            "QATAR OPTICS- PLACE VENDOME",
        ]

        exclusion_column = "PROVIDER_NAME"

        extra_condition : list[dict] = [
            {"column": "CORPORATE_NAME", "condition": {"eq": "Qatar Charity"}},
            {"column": "BENEFIT_TYPE", "condition": {"eq": "OPTICAL"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_condition,
            exclusion=exclusion,
            exclusion_column=exclusion_column
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def al_jazeera_media_network(self, df):
        trigger_name: str = "Health Check-Up – Al Jazeera Media Network"

        inclusion: list[str] = [
            "82306",
            "82607",
            "82728",
        ]

        inclusion_column = "ACTIVITY_CODE"

        extra_condition : list[dict] = [
            {"column": "CORPORATE_NAME", "condition": {"eq": "AL JAZEERA MEDIA NETWORK"}},
            {"column": "BENEFIT_TYPE", "condition": {"eq": "HEALTH CHECK-UP"}},
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_condition,
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def dental_mofa(self, df):
        trigger_name: str = "Dental – MOFA"

        inclusion: list[str] = [
            "D2999",
        ]

        inclusion_column = "ACTIVITY_CODE"

        extra_condition : list[dict] = [
            {"column": "CORPORATE_NAME", "condition": {"eq": "MINISTRY OF FOREIGN AFFAIRS"}},
            {"column": "BENEFIT_TYPE", "condition": {"eq": "DENTAL"}},
        ]   

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=trigger_name,
            inclusion=inclusion,
            inclusion_column=inclusion_column,
            extra_condition=extra_condition,
        )

        return df
        
    @rule_details("both", "account specific")
    @rule_method(active=True)
    def free_total_testosterone_rule(self, df):
        trigger_name = "Free Testosterone & Total Testosterone in same preauth/claim"

        pair_list = [
            (["84402"], ["84403"])   # Free & Total Testosterone
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=trigger_name,
            pair_list=pair_list,
            code_column="ACTIVITY_CODE",
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def laryngoscopy_nasoendoscopy_rule(self, df):
        trigger_name = "Laryngoscopy and Naso-endoscopy- Consultation in Same claim / pre-auth"

        # All acceptable consultation codes (very large list)
        match_codes = {
            '9','9.01','9.02','10','10.01','10.02','11','11.01','11.02','13',
            '21','22','23',
            '61.01','61.02','61.03','61.04','61.05','61.06','61.07','61.08','61.09','61.11','61.12',
            '63','63.01','63.02','63.03','63.04','63.05',
            '99202','99203','99204','99205','99211','99212','99213','99214','99215',
            '99221','99222','99223','99231','99232','99233','99234','99235','99236',
            '99238','99239','99241','99242','99243','99244','99245',
            '99252','99253','99254','99255',
            '99281','99282','99283','99284','99285','99288',
            '99291','99292',
            '99304','99305','99306','99307','99308','99309','99310',
            '99315','99316',
            '99341','99342','99344','99345','99347','99348','99349','99350',
            '99358','99359','99360',
            '99366','99367','99368',
            '99374','99375','99377','99378','99379','99380','99381','99382','99383','99384','99385','99386','99387',
            '99391','99392','99393','99394','99395','99396','99397',
            '99401','99402','99403','99404',
            '99406','99407','99408','99409',
            '99411','99412','99429',
            '99441','99442','99443',
            '99450','99455','99456',
            '99460','99461','99462','99463','99464','99465',
            '99466','99467','99468','99469',
            '99471','99472','99475','99476','99477','99478','99479','99480',
            '99499',
            '9500',
            '99501','99502','99503','99504','99505','99506','99507','99509','99510','99511','99512',
            '99600','99601','99602','99605','99606','99607'
        }

        # Pairs: procedure codes → consultation codes
        pair_list = [
            (["31231", "31505"], list(match_codes))
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=trigger_name,
            pair_list=pair_list,
            code_column="ACTIVITY_CODE",
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def troponin_cpkmb_rule(self, df):
        trigger_name = "Troponin and CPK-MB in Same claim / pre-auth"

        # Define the pair: A → B
        pair_list = [
            (["84484"], ["82553", "82550"])
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=trigger_name,
            pair_list=pair_list,
            code_column="ACTIVITY_CODE",
        )
