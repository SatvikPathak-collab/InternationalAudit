RULE_UI_SCHEMA = {
    # ---------- Simple ----------
    "incl_codes": {"label": "Included Codes", "type": "list"},
    "excl_codes": {"label": "Excluded Codes", "type": "list"},
    "icd_codes": {"label": "ICD Codes", "type": "list"},
    "keywords": {"label": "Keywords", "type": "list"},
    "policy_numbers": {"label": "Policy Numbers", "type": "list"},
    "excl_providers": {"label": "Excluded Providers", "type": "list"},
    "physio_codes": {"label": "Physio Codes", "type": "list"},

    "incl_col": {"label": "Inclusion Column", "type": "text"},
    "excl_col": {"label": "Exclusion Column", "type": "text"},
    "text_column": {"label": "Text Column", "type": "text"},

    # ---------- Composite ----------
    "exclusions": {"label": "Exclusions", "type": "multi_column_list"},
    "inclusion": {"label": "Inclusion Conditions", "type": "multi_column_list"},
    "extra_conditions": {"label": "Extra Conditions", "type": "conditions"},
    "compound_exclusion": {"label": "Combined Exclusion", "type": "compound_condition"},

    # ---------- Rules ----------
    "quantity_rule": {"label": "Quantity Rule", "type": "quantity_rule"},
    "age_rule": {"label": "Age Rule", "type": "age_range"},
    "age_quantity_rule": {"label": "Age + Quantity Rule", "type": "age_quantity_rule"},
    "pair_rule": {"label": "Paired Codes Rule", "type": "pair_rule"},
    "code_match": {"label": "Code Match", "type": "code_match"},
    "keyword_match": {"label": "Keyword Match", "type": "keyword_match"},
    "text_match": {"label": "Text Match", "type": "text_match"},
    "provider_match": {"label": "Provider Match", "type": "provider_match"},
    "provider_condition": {"label": "Provider Condition", "type": "provider_condition"},
    "gender_rule": {"label": "Gender Rule", "type": "gender_rule"},
    "preauth_rule": {"label": "Preauth Rule", "type": "preauth_rule"},
}
