import streamlit as st
from loguru import logger
from copy import deepcopy

from src.config.config import RULES_CONFIG, save_rules
from rule_ui_schema import RULE_UI_SCHEMA


# ---------- Helpers ----------
def is_reference(val):
    return isinstance(val, str) and val.startswith("__") and val.endswith("__")


# ---------- Field Renderer ----------
def render_field(rule_key, field_key, value):
    schema = RULE_UI_SCHEMA[field_key]
    ftype = schema["type"]
    label = schema["label"]
    base = f"{rule_key}__{field_key}"

    # ---------- LIST ----------
    if ftype == "list":
        text = "\n".join(value or [])
        updated = st.text_area(label, text, key=base)
        return [v.strip() for v in updated.splitlines() if v.strip()]

    # ---------- TEXT ----------
    if ftype == "text":
        return st.text_input(label, value or "", key=base)

    # ---------- MULTI COLUMN LIST (REFERENCE SAFE) ----------
    if ftype == "multi_column_list":
        st.markdown(f"**{label}**")
        rows = []

        for i, row in enumerate(value or []):
            c1, c2 = st.columns(2)

            column = c1.text_input(
                "Column",
                row.get("column", ""),
                key=f"{base}_{i}_col",
            )

            codes = row.get("codes")

            # Reference token → read-only
            if is_reference(codes):
                c2.text_input(
                    "Codes",
                    f"(linked to {codes.strip('_')})",
                    disabled=True,
                    key=f"{base}_{i}_ref",
                )
                rows.append({"column": column, "codes": codes})

            # Editable list
            else:
                text = "\n".join(codes or [])
                updated = c2.text_area(
                    "Codes (one per line)",
                    text,
                    key=f"{base}_{i}_codes",
                )
                rows.append({
                    "column": column,
                    "codes": [v.strip() for v in updated.splitlines() if v.strip()],
                })

        return rows

    # ---------- CONDITIONS ----------
    if ftype == "conditions":
        st.markdown(f"**{label}**")
        out = []
        for i, c in enumerate(value or []):
            col, cond = st.columns(2)
            out.append({
                "column": col.text_input(
                    "Column",
                    c["column"],
                    key=f"{base}_{i}_col",
                ),
                "condition": st.text_input(
                    "Condition (JSON)",
                    str(c["condition"]),
                    key=f"{base}_{i}_cond",
                ),
            })
        return out

    # ---------- COMPOUND ----------
    if ftype == "compound_condition":
        st.markdown(f"**{label}**")
        conds = []
        for i, c in enumerate(value.get("conditions", [])):
            col, eq = st.columns(2)
            conds.append({
                "column": col.text_input("Column", c["column"], key=f"{base}_{i}_col"),
                "eq": eq.text_input("Equals", c["eq"], key=f"{base}_{i}_eq"),
            })
        return {"conditions": conds}

    # ---------- QUANTITY ----------
    if ftype == "quantity_rule":
        c1, c2 = st.columns(2)
        return {
            "column": c1.text_input("Quantity Column", value.get("column"), key=f"{base}_col"),
            "gt": c2.number_input("Greater Than", value=value.get("gt", 1), min_value=0, key=f"{base}_gt"),
        }

    # ---------- AGE ----------
    if ftype == "age_range":
        c1, c2 = st.columns(2)
        return {
            "column": value.get("column", "MEMBER_AGE"),
            "min": c1.number_input("Min Age", value.get("min", 0)),
            "max": c2.number_input("Max Age", value.get("max", 100)),
            "outside": st.checkbox("Outside Range", value.get("outside", True)),
        }

    # ---------- PAIR ----------
    if ftype == "pair_rule":
        code_col = st.text_input("Code Column", value.get("code_column"), key=f"{base}_cc")
        pairs = []
        for i, p in enumerate(value.get("pairs", [])):
            a, b = st.columns(2)
            pairs.append({
                "A": a.text_area("Group A", "\n".join(p["A"]), key=f"{base}_{i}_A").splitlines(),
                "B": b.text_area("Group B", "\n".join(p["B"]), key=f"{base}_{i}_B").splitlines(),
            })
        return {"code_column": code_col, "pairs": pairs}

    if ftype == "code_match":
        st.markdown(f"**{label}**")
        col = st.text_input(
            "Column",
            value.get("column", ""),
            key=f"{base}_col",
        )
        codes = st.text_area(
            "Codes (one per line)",
            "\n".join(value.get("codes", [])),
            key=f"{base}_codes",
        )
        return {
            "column": col,
            "codes": [v.strip() for v in codes.splitlines() if v.strip()],
        }

    if ftype == "keyword_match":
        st.markdown(f"**{label}**")
        col = st.text_input(
            "Column",
            value.get("column", ""),
            key=f"{base}_col",
        )

        # supports BOTH keywords list and regex/pattern
        if "keywords" in value:
            kws = st.text_area(
                "Keywords (one per line)",
                "\n".join(value.get("keywords", [])),
                key=f"{base}_kw",
            )
            return {
                "column": col,
                "keywords": [v.strip() for v in kws.splitlines() if v.strip()],
            }

        if "pattern" in value:
            pattern = st.text_input(
                "Regex Pattern",
                value.get("pattern", ""),
                key=f"{base}_pattern",
            )
            return {
                "column": col,
                "pattern": pattern,
            }

    if ftype == "text_match":
        st.markdown(f"**{label}**")
        col = st.text_input(
            "Column",
            value.get("column", ""),
            key=f"{base}_col",
        )
        pattern = st.text_input(
            "Regex / Pattern",
            value.get("pattern", ""),
            key=f"{base}_pattern",
        )
        return {
            "column": col,
            "pattern": pattern,
        }

    if ftype == "provider_match":
        st.markdown(f"**{label}**")
        return {
            "column": st.text_input("Column", value.get("column"), key=f"{base}_col"),
            "pattern": st.text_input("Pattern", value.get("pattern"), key=f"{base}_pat"),
        }

    if ftype == "provider_condition":
        st.markdown(f"**{label}**")
        return {
            "column": st.text_input("Column", value.get("column"), key=f"{base}_col"),
            "eq": st.text_input("Equals", value.get("eq"), key=f"{base}_eq"),
        }

    if ftype == "gender_rule":
        st.markdown(f"**{label}**")
        return {
            "column": st.text_input("Column", value.get("column"), key=f"{base}_col"),
            "eq": st.selectbox(
                "Gender",
                ["Male", "Female"],
                index=["Male", "Female"].index(value.get("eq", "Male")),
                key=f"{base}_eq",
            ),
        }

    if ftype == "preauth_rule":
        st.markdown(f"**{label}**")
        cols = st.text_area(
            "Preauth Columns (one per line)",
            "\n".join(value.get("preauth_columns", [])),
            key=f"{base}_cols",
        )
        return {
            "preauth_columns": [v.strip() for v in cols.splitlines() if v.strip()],
            "complaint_column": st.text_input(
                "Complaint Column",
                value.get("complaint_column"),
                key=f"{base}_complaint",
            ),
            "regex": st.text_input(
                "Regex",
                value.get("regex", ""),
                key=f"{base}_regex",
            ),
        }

    if ftype == "age_quantity_rule":
        st.markdown(f"**{label}**")
        return {
            "age_column": st.text_input(
                "Age Column",
                value.get("age_column", "MEMBER_AGE"),
                key=f"{base}_age_col",
            ),
            "adult_age": st.number_input(
                "Adult Age",
                value.get("adult_age", 18),
                key=f"{base}_adult_age",
            ),
            "adult_qty_gt": st.number_input(
                "Adult Qty >",
                value.get("adult_qty_gt", 1),
                key=f"{base}_adult_qty",
            ),
            "child_qty_gt": st.number_input(
                "Child Qty >",
                value.get("child_qty_gt", 2),
                key=f"{base}_child_qty",
            ),
        }

    return value


# ---------- Page ----------
def rules_config_editor():
    st.title("⚙️ Rules Configuration Editor")

    if st.button("⬅️ Back"):
        st.session_state.show_config_page = False
        st.rerun()

    rule_key = st.selectbox(
        "Select Rule",
        sorted(RULES_CONFIG.keys()),
        format_func=lambda k: RULES_CONFIG[k]["name"],
    )

    rule = deepcopy(RULES_CONFIG[rule_key])

    # ---------- Metadata ----------
    st.subheader("Metadata")
    rule["name"] = st.text_input("Rule Name", rule["name"])
    rule["active"] = st.checkbox("Active", rule["active"])
    rule["case_type"] = st.selectbox("Case Type", ["both", "claim", "preauth"],
                                     index=["both", "claim", "preauth"].index(rule["case_type"]))
    rule["scope"] = st.selectbox("Scope", ["generic", "account specific", "universal"],
                                 index=["generic", "account specific", "universal"].index(rule["scope"]))
    rule["review_req"] = st.selectbox("Review", ["none", "manual"],
                                      index=["none", "manual"].index(rule["review_req"]))

    # ---------- Config ----------
    st.divider()
    st.subheader("Configuration")

    for key in rule:
        if key in {"trigger_key", "name", "active", "case_type", "scope", "review_req"}:
            continue
        if key in RULE_UI_SCHEMA:
            rule[key] = render_field(rule_key, key, rule.get(key))

    # ---------- Save ----------
    if st.button("💾 Save"):
        RULES_CONFIG[rule_key] = rule

        # THIS persists to JSON
        save_rules(RULES_CONFIG)

        logger.info(f"Saved rule to disk: {rule_key}")
        st.success("Saved successfully")

        st.rerun()
