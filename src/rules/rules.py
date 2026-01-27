import functools

from numpy import True_
import pandas as pd
from loguru import logger
import re
from src.config.config import RULES_CONFIG

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
        config = RULES_CONFIG["general_exclusion_hiv"]
        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            df=df,
            trigger_name=config["name"],
            exclusion=config["excl_codes"],
            inclusion_column=config["incl_col"],
            exclusion_column=config["excl_col"],
        )

        return df

    @rule_details("both", "account specific", "manual")
    @rule_method(active=True)
    def general_exclusion_zirconium_crown(self, df):
        config = RULES_CONFIG["general_exclusion_zirconium_crown"]
        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            exclusion=config["excl_codes"],
            df=df,
            trigger_name=config["name"],
            inclusion_column=config["incl_col"],
            exclusion_column=config["excl_col"],
        )

        df = self.apply_manual_trigger(df, config["name"])
        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def covid(self, df):
        config = RULES_CONFIG["covid"]

        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            exclusion=config["excl_codes"],
            df=df,
            trigger_name=config["name"],
            inclusion_column=config["incl_col"],
            exclusion_column=config["excl_col"],
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def hpv_screening(self, df):
        config = RULES_CONFIG["hpv_screening"]

        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            df=df,
            trigger_name=config["name"],
            inclusion_column=config["incl_col"],
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def alopecia(self, df):
        config = RULES_CONFIG["alopecia"]

        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            df=df,
            trigger_name=config["name"],
            inclusion_column=config["incl_col"],
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def more_than_one_quantity(self, df):
        config = RULES_CONFIG["more_than_one_quantity"]

        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            df=df,
            trigger_name=config["name"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def sick_leave(self, df):
        config = RULES_CONFIG["sick_leave"]

        text_col = config["text_column"]
        if text_col not in df.columns:
            logger.error(f"{text_col} not present in data.")
            return df

        trigger_name = config["name"]
        temp_col = "_tmp_sick_leave_flag"

        # Build regex from config keywords
        pattern = "|".join(re.escape(k.lower()) for k in config["keywords"])

        df[temp_col] = (
            df[text_col]
            .astype(str)
            .str.lower()
            .str.contains(pattern, na=False)
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
        config = RULES_CONFIG["pap_smear_age_restriction"]

        age_cfg = config["age_rule"]
        temp_col = "_tmp_age_restriction"

        age_series = df[age_cfg["column"]]
        if age_cfg.get("outside", True):
            df[temp_col] = (age_series < age_cfg["min"]) | (age_series > age_cfg["max"])
        else:
            df[temp_col] = (age_series >= age_cfg["min"]) & (age_series <= age_cfg["max"])

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}}
        ]

        df = self._compute_inclusion_exclusion(
            inclusion=config["incl_codes"],
            exclusion=config["excl_codes"],
            df=df,
            trigger_name=config["name"],
            inclusion_column=config["incl_col"],
            exclusion_column=config["excl_col"],
            extra_condition=extra_conditions,
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def zinc_general_exclusion(self, df):
        config = RULES_CONFIG["zinc_general_exclusion"]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def mouth_wash(self, df):
        config = RULES_CONFIG["mouth_wash"]
        temp_col = "_tmp_mouth_wash_exclusion"

        df[temp_col] = False

        # compound exclusion (AND logic)
        mask = pd.Series(True, index=df.index)
        for cond in config["compound_exclusion"]["conditions"]:
            mask &= df[cond["column"]] == cond["eq"]

        df.loc[mask, temp_col] = True

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            exclusion=config["exclusions"],
            extra_condition=[
                {"column": temp_col, "condition": {"eq": False}}
            ],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "generic", "manual")
    @rule_method(active=True)
    def cough_syrup_high_quantity(self, df):
        config = RULES_CONFIG["cough_syrup_high_quantity"]
        temp_col = "_tmp_cough_syrup"

        df[temp_col] = False
        for col in config["text_match"]["columns"]:
            df[temp_col] |= df[col].astype(str).str.contains(
                config["text_match"]["pattern"], case=False, na=False
            )

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}},
            {
                "column": config["quantity_rule"]["column"],
                "condition": {"gt": config["quantity_rule"]["gt"]},
            },
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=extra_conditions,
        )

        df = self.apply_manual_trigger(df, config["name"])
        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "universal", "manual")
    @rule_method(active=True)
    def nasal_syrup_high_quantity(self, df):
        config = RULES_CONFIG["nasal_syrup_high_quantity"]
        temp_col = "_tmp_nasal_spray"

        df[temp_col] = False
        for col in config["text_match"]["columns"]:
            series = df[col].astype(str).str.lower()
            df[temp_col] |= (
                series.str.contains(config["text_match"]["contains_all"][0], na=False) &
                series.str.contains(config["text_match"]["contains_all"][1], na=False)
            )

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}},
            {
                "column": config["quantity_rule"]["column"],
                "condition": {"gt": config["quantity_rule"]["gt"]},
            },
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=extra_conditions,
        )

        df = self.apply_manual_trigger(df, config["name"])
        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "generic", "manual")
    @rule_method(active=True)
    def nebulizer_high_quantity(self, df):
        config = RULES_CONFIG["nebulizer_high_quantity"]
        temp_col = "_tmp_nebulizer"

        age_cfg = config["age_quantity_rule"]
        adult_mask = df[age_cfg["age_column"]] >= age_cfg["adult_age"]

        df[temp_col] = (
            (adult_mask & (df["ACTIVITY_QUANTITY_APPROVED"] > age_cfg["adult_qty_gt"])) |
            (~adult_mask & (df["ACTIVITY_QUANTITY_APPROVED"] > age_cfg["child_qty_gt"]))
        )

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}}
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=extra_conditions,
        )

        df = self.apply_manual_trigger(df, config["name"])
        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def hpyrol_antibody(self, df):
        config = RULES_CONFIG["hpyrol_antibody"]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
        )

        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def gardenia_large_dressing(self, df):
        config = RULES_CONFIG["gardenia_large_dressing"]
        temp_col = "_tmp_large_dressing"

        df[temp_col] = (
            df[config["text_match"]["column"]]
            .astype(str)
            .str.contains(config["text_match"]["pattern"], case=False, na=False)
        )

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}},
            {
                "column": config["provider_condition"]["column"],
                "condition": {"eq": config["provider_condition"]["eq"]},
            },
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=extra_conditions,
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def sidra_medical_male(self, df):
        config = RULES_CONFIG["sidra_medical_male"]
        temp_col = "_tmp_sidra_medical"

        df[temp_col] = (
            df[config["provider_match"]["column"]]
            .astype(str)
            .str.contains(config["provider_match"]["pattern"], case=False, na=False)
        )

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}},
            {
                "column": config["age_rule"]["column"],
                "condition": {"gt": config["age_rule"]["gt"]},
            },
            {
                "column": config["gender_rule"]["column"],
                "condition": {"eq": config["gender_rule"]["eq"]},
            },
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=extra_conditions,
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def glucosamine_quantity(self, df):
        config = RULES_CONFIG["glucosamine_quantity"]
        temp_col = "_tmp_glucosamine_flag"

        code_cfg = config["code_match"]
        keyword_cfg = config["keyword_match"]

        code_mask = df[code_cfg["column"]].isin(code_cfg["codes"])

        keyword_pattern = "|".join(
            k.lower() for k in keyword_cfg["keywords"]
        )
        keyword_mask = (
            df[keyword_cfg["column"]]
            .astype(str)
            .str.lower()
            .str.contains(keyword_pattern, na=False)
        )

        df[temp_col] = code_mask | keyword_mask

        extra_conditions = [
            {"column": temp_col, "condition": {"eq": True}},
            {
                "column": config["quantity_rule"]["column"],
                "condition": {"gt": config["quantity_rule"]["gt"]},
            },
        ]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=extra_conditions,
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def apply_crp_esr_rule(self, df):
        config = RULES_CONFIG["crp_esr_pair_rule"]

        pair_list = [
            (pair["A"], pair["B"])
            for pair in config["pair_rule"]["pairs"]
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=config["name"],
            pair_list=pair_list,
            code_column=config["pair_rule"]["code_column"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def general_exclusion_probiotic(self, df):
        config = RULES_CONFIG["general_exclusion_probiotic"]
        temp_col = "_tmp_probiotic"

        code_cfg = config["code_match"]
        keyword_cfg = config["keyword_match"]

        code_mask = df[code_cfg["column"]].isin(code_cfg["codes"])
        keyword_mask = (
            df[keyword_cfg["column"]]
            .astype(str)
            .str.contains(keyword_cfg["pattern"], case=False, na=False)
        )

        df[temp_col] = code_mask | keyword_mask

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def not_payable_ondansetron(self, df):
        config = RULES_CONFIG["not_payable_ondansetron"]
        temp_col = "_tmp_ondansetron"

        code_cfg = config["code_match"]
        keyword_cfg = config["keyword_match"]

        code_mask = df[code_cfg["column"]].isin(code_cfg["codes"])

        keyword_pattern = "|".join(k.lower() for k in keyword_cfg["keywords"])
        keyword_mask = (
            df[keyword_cfg["column"]]
            .astype(str)
            .str.lower()
            .str.contains(keyword_pattern, na=False)
        )

        df[temp_col] = code_mask | keyword_mask

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
            exclusion=config["exclusions"],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific", "manual")
    @rule_method(active=True)
    def not_payable_semaglutide(self, df):
        config = RULES_CONFIG["not_payable_semaglutide"]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
        )

        df = self.apply_manual_trigger(df, config["name"])
        return df

    @rule_details("both", "account specific", "manual")
    @rule_method(active=True)
    def diabetic_semaglutide(self, df):
        config = RULES_CONFIG["diabetic_semaglutide"]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
        )

        df = self.apply_manual_trigger(df, config["name"])
        return df

    @rule_details("claim", "account specific")
    @rule_method(active=True)
    def biopsy_pa_available(self, df):
        config = RULES_CONFIG["biopsy_pa_available"]
        temp_col = "_tmp_pre_auth_missing"

        # determine available preauth column
        preauth_cols = config["preauth_rule"]["preauth_columns"]
        pre_auth_col = next((c for c in preauth_cols if c in df.columns), None)

        df[temp_col] = (
            df[pre_auth_col].isna()
            | df[pre_auth_col].astype(str).str.strip().eq("")
        ) & (
            ~df[config["preauth_rule"]["complaint_column"]]
                .astype(str)
                .str.contains(
                    config["preauth_rule"]["regex"],
                    regex=True,
                    na=False,
                )
        )

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def beta_hcg_urine_pregnancy(self, df):
        config = RULES_CONFIG["beta_hcg_urine_pregnancy"]

        pair_list = [
            (pair["A"], pair["B"])
            for pair in config["pair_rule"]["pairs"]
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=config["name"],
            pair_list=pair_list,
            code_column=config["pair_rule"]["code_column"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def capsaicin_belladona_non_payable(self, df):
        config = RULES_CONFIG["capsaicin_belladona_non_payable"]
        temp_col = "_tmp_capsaicin_belladona"

        code_cfg = config["code_match"]
        keyword_cfg = config["keyword_match"]

        code_mask = df[code_cfg["column"]].isin(code_cfg["codes"])

        keyword_pattern = "|".join(k.lower() for k in keyword_cfg["keywords"])
        keyword_mask = (
            df[keyword_cfg["column"]]
            .astype(str)
            .str.lower()
            .str.contains(keyword_pattern, na=False)
        )

        df[temp_col] = code_mask | keyword_mask

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "generic")
    @rule_method(active=True)
    def heatpad_non_payable(self, df):
        config = RULES_CONFIG["heatpad_non_payable"]

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["inclusion"],
        )

        return df

    @rule_details("both", "generic")
    @rule_method(active=False)
    def steam_inhaler_non_payable(self, df):
        config = RULES_CONFIG["steam_inhaler_non_payable"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["inclusion"],
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def hot_water_bag_non_payable(self, df):
        config = RULES_CONFIG["hot_water_bag_non_payable"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["inclusion"],
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def pillows_non_payable(self, df):
        config = RULES_CONFIG["pillows_non_payable"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["inclusion"],
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def antiseptics_non_payable(self, df):
        config = RULES_CONFIG["antiseptics_non_payable"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def diapers_non_payable(self, df):
        config = RULES_CONFIG["diapers_non_payable"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["inclusion"],
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def moisturizer_non_payable(self, df):
        config = RULES_CONFIG["moisturizer_non_payable"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["inclusion"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def al_abdulghani_motors(self, df):
        config = RULES_CONFIG["al_abdulghani_motors"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=config["extra_conditions"],
            exclusion=config["excl_providers"],
            exclusion_column="PROVIDER_NAME",
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_maternity_cmv(self, df):
        config = RULES_CONFIG["op_maternity_cmv"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_maternity_toxoplasma(self, df):
        config = RULES_CONFIG["op_maternity_toxoplasma"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_maternity_qatar_charity(self, df):
        config = RULES_CONFIG["op_maternity_qatar_charity"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def op_optical_qatar_charity(self, df):
        config = RULES_CONFIG["op_optical_qatar_charity"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            exclusion=config["excl_codes"],
            exclusion_column=config["excl_col"],
            extra_condition=config["extra_conditions"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def al_jazeera_media_network(self, df):
        config = RULES_CONFIG["al_jazeera_media_network"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def dental_mofa(self, df):
        config = RULES_CONFIG["dental_mofa"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
            extra_condition=config["extra_conditions"],
        )
        
    @rule_details("both", "account specific")
    @rule_method(active=True)
    def free_total_testosterone_rule(self, df):
        config = RULES_CONFIG["free_total_testosterone_rule"]

        pair_list = [
            (pair["A"], pair["B"])
            for pair in config["pair_rule"]["pairs"]
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=config["name"],
            pair_list=pair_list,
            code_column=config["pair_rule"]["code_column"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def laryngoscopy_nasoendoscopy_rule(self, df):
        config = RULES_CONFIG["laryngoscopy_nasoendoscopy_rule"]

        pair_list = [
            (pair["A"], pair["B"])
            for pair in config["pair_rule"]["pairs"]
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=config["name"],
            pair_list=pair_list,
            code_column=config["pair_rule"]["code_column"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def troponin_cpkmb_rule(self, df):
        config = RULES_CONFIG["troponin_cpkmb_rule"]

        pair_list = [
            (pair["A"], pair["B"])
            for pair in config["pair_rule"]["pairs"]
        ]

        return self._apply_group_pair_rule(
            df=df,
            trigger_name=config["name"],
            pair_list=pair_list,
            code_column=config["pair_rule"]["code_column"],
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def general_exclusion_gonococcal(self, df):
        config = RULES_CONFIG["general_exclusion_gonococcal"]

        icd_codes = config["icd_codes"]
        inclusion = [
            {"column": inc["column"], "codes": icd_codes}
            for inc in config["inclusion"]
        ]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=inclusion,
        )

    @rule_details("both", "generic")
    @rule_method(active=True)
    def general_exclusion_syphilis(self, df):
        config = RULES_CONFIG["general_exclusion_syphilis"]

        icd_codes = config["icd_codes"]
        inclusion = [
            {"column": inc["column"], "codes": icd_codes}
            for inc in config["inclusion"]
        ]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=inclusion,
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def general_exclusion_mounjaro(self, df):
        config = RULES_CONFIG["general_exclusion_mounjaro"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
        )

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def weight_loss_glp1_exclusion(self, df):
        config = RULES_CONFIG["weight_loss_glp1_exclusion"]
        temp_col = "_tmp_weight_loss"

        pattern = "|".join(k.lower() for k in config["keywords"])
        df[temp_col] = (
            df[config["text_column"]]
            .astype(str)
            .str.lower()
            .str.contains(pattern, na=False)
        )

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def cosmetic_exclusion(self, df):
        config = RULES_CONFIG["cosmetic_exclusion"]
        temp_col = "_tmp_cosmetic"

        pattern = "|".join(k.lower() for k in config["keywords"])
        df[temp_col] = (
            df[config["text_column"]]
            .astype(str)
            .str.lower()
            .str.contains(pattern, na=False)
        )

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def infertility_exclusion(self, df):
        config = RULES_CONFIG["infertility_exclusion"]
        temp_col = "_tmp_infertility"

        pattern = "|".join(k.lower() for k in config["keywords"])
        df[temp_col] = (
            df[config["text_column"]]
            .astype(str)
            .str.lower()
            .str.contains(pattern, na=False)
        )

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def physiotherapy_pa_mandatory(self, df):
        config = RULES_CONFIG["physiotherapy_pa_mandatory"]
        temp_col = "_tmp_physio"

        preauth_col = next(
            (c for c in config["preauth"]["preauth_columns"] if c in df.columns),
            None,
        )

        df[temp_col] = (
            df["ACTIVITY_CODE"].isin(config["physio_codes"]) &
            (
                df[preauth_col].isna() |
                ~df[config["preauth"]["complaint_column"]]
                    .astype(str)
                    .str.contains(config["preauth"]["regex"], regex=True, na=False)
            )
        )

        df = self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            extra_condition=[{"column": temp_col, "condition": {"eq": True}}],
        )

        df.drop(columns=[temp_col], inplace=True)
        return df

    @rule_details("both", "account specific")
    @rule_method(active=True)
    def silymarin_exclusion(self, df):
        config = RULES_CONFIG["silymarin_exclusion"]

        return self._compute_inclusion_exclusion(
            df=df,
            trigger_name=config["name"],
            inclusion=config["incl_codes"],
            inclusion_column=config["incl_col"],
        )
