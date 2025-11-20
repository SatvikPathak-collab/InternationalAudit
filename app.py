import os
import sys
import warnings

import pandas as pd
import streamlit as st
from loguru import logger

from postprocess import PostProcessClass
from preprocess import PreprocessClass
from rules import ComputeRule
from mapper import excluded_conditions_preauth, excluded_conditions_claims

warnings.filterwarnings("ignore")
logger.remove()
logger.add("log.log", level="DEBUG")
logger.add(sys.stderr, level="DEBUG", colorize=True)


st.set_page_config(page_title="CSV/Excel Preprocessor", layout="wide")


def preprocess_and_run_rules(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    preprocess_client = PreprocessClass()
    post_process_client = PostProcessClass()

    # Selecting excluded conditions mapper based on file type
    if data_type == "PreAuth":
        excluded_conditions = excluded_conditions_preauth
    elif data_type == "Claim":
        excluded_conditions = excluded_conditions_claims
    else:
        logger.info("Invalid file type. Returning original data")
        return df

    rules_client = ComputeRule(excluded_conditions=excluded_conditions, data_type=data_type)
    preprocessed_data = preprocess_client.run_preprocess(df=df, excluded_conditions=excluded_conditions)
    rules_applied_data = rules_client.apply_all_rules(preprocessed_data)
    processed_df = post_process_client.postprocess_df(df=rules_applied_data)
    processed_df.reset_index(drop=True, inplace=True)
    return processed_df

def show_processing_summary(processed_df: pd.DataFrame):
    """Display summary statistics of processed dataframe in Streamlit."""

    st.subheader("📊 Processing Summary")

    total_rows = len(processed_df)
    triggered_rows = processed_df["Filter Applied(Exclusions not Applied)"].apply(lambda x: len(x) > 0).sum()
    trigger_rows_excl_applied = processed_df["Filter Applied"].apply(lambda x: len(x) > 0).sum()
    manual_verification_rows = processed_df["Filter Applied(Manual Verification Required)"].apply(lambda x: len(x) > 0).sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", total_rows)
    col2.metric("Rows with Triggers", triggered_rows)
    col3.metric("Rows with Triggers(Exclusions Applied)", trigger_rows_excl_applied)
    col4.metric("Rows requiring Manual Verification", manual_verification_rows)


# ---- Streamlit UI ----
logger.info("International Audit app running")
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
            logger.info("✅ Successfully uploaded: {filename}")

            # Preview the uploaded data
            st.subheader("📄 Preview of Uploaded Data")
            st.dataframe(df.head(), width='stretch')

            data_type = st.selectbox(
                "Enter file data type: ",
                ["PreAuth", "Claim"],
                index=None,
                placeholder="Select data type"
            )
            if data_type:
                # Call your processing function
                with st.spinner("Processing..."):
                    result_df = preprocess_and_run_rules(df, data_type)

                # Show result
                st.subheader("📄 Processed Data")
                st.dataframe(result_df.head(), width='stretch')

                # --- Summary statistics ---
                show_processing_summary(result_df)

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
        logger.error(f"{e}. Execution stopped")
        st.error(f"Error reading file: {e}")