import os
import sys
import warnings

import pandas as pd
import streamlit as st
from loguru import logger

from preprocess import PreprocessClass
from rules import ComputeRule
from mapper import excluded_conditions

warnings.filterwarnings("ignore")
logger.remove()
logger.add("log.log", level="DEBUG")
logger.add(sys.stderr, level="DEBUG", colorize=True)


st.set_page_config(page_title="CSV/Excel Preprocessor", layout="wide")


def preprocess_and_run_rules(df: pd.DataFrame) -> pd.DataFrame:
    preprocess_client = PreprocessClass()
    rules_client = ComputeRule(excluded_conditions=excluded_conditions)
    preprocessed_data = preprocess_client.run_preprocess(df=df)
    rules_applied_data = rules_client.apply_all_rules(preprocessed_data)
    rules_applied_data.reset_index(drop=True, inplace=True)
    return rules_applied_data


# ---- Streamlit UI ----
st.title("CSV Preprocessor & Rule Runner")

uploaded_file = st.file_uploader(
    "Upload a CSV or Excel file", type=["csv", "xls", "xlsx"], help="Supported formats: CSV, XLS, XLSX"
)

if uploaded_file is not None:
    # Check the file extension
    filename = uploaded_file.name
    ext = os.path.splitext(filename)[1].lower()

    try:
        if ext == ".csv":
            df = pd.read_csv(uploaded_file)
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(uploaded_file)
        else:
            st.error("❌ Unsupported file type. Please upload CSV or Excel files.")
            df = None

        if df is not None:
            st.success(f"✅ Successfully uploaded: {filename}")

            # Call your processing function
            with st.spinner("Processing..."):
                result_df = preprocess_and_run_rules(df)

            # Show result
            st.subheader("📄 Processed Data")
            st.dataframe(result_df, width='stretch')

            # Prepare for download
            result_csv = result_df.to_csv(index=False).encode("utf-8")
            result_name = f"result_{os.path.splitext(filename)[0]}.csv"

            st.download_button(
                label="⬇️ Download Processed CSV",
                data=result_csv,
                file_name=result_name,
                mime="text/csv",
            )

    except Exception as e:
        st.error(f"Error reading file: {e}")