import os
import sys
import streamlit as st
import pandas as pd
from loguru import logger

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.main import main
from rules_config_editor import rules_config_editor

st.set_page_config(page_title="Audit Engine", layout="wide")
st.title("CSV / Excel Audit Rule Runner")

if "show_config_page" not in st.session_state:
    st.session_state.show_config_page = False

col1, col2 = st.columns([4, 1])
with col2:
    if st.button("⚙️ Change Rules Config"):
        st.session_state.show_config_page = True

# ---------------- Processing Summary ---------------- #
def show_processing_summary(df: pd.DataFrame):
    st.subheader("📊 Processing Summary")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", len(df))
    col2.metric(
        "Rows with Triggers",
        df["Filter Applied(Exclusions not Applied)"].apply(lambda x: bool(x)).sum(),
    )
    col3.metric(
        "Rows with Triggers (Exclusions Applied)",
        df["Filter Applied"].apply(lambda x: bool(x)).sum(),
    )
    col4.metric(
        "Manual Verification",
        df["Filter Applied(Manual Verification Required)"].apply(lambda x: bool(x)).sum(),
    )

# ---------------- Router ---------------- #
if st.session_state.show_config_page:
    rules_config_editor()
    st.stop()

# ---------------- Audit Runner ---------------- #
uploaded_file = st.file_uploader(
    "Upload CSV / Excel",
    type=["csv", "xls", "xlsx"],
)

if uploaded_file:
    uploaded_file.seek(0)

    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.dataframe(df.head())

    data_type = st.selectbox("Data Type", ["PreAuth", "Claim"])
    insurer = st.selectbox("Insurer", ["ALKOOT INSURANCE COMPANY"])

    if st.button("▶️ Run Audit"):
        with st.spinner("Running rules..."):
            try:
                result = main(df, data_type, insurer)
            except Exception as e:
                logger.exception(e)
                st.error(str(e))
                st.stop()

        st.dataframe(result.head())
        show_processing_summary(result)

        st.download_button(
            "⬇️ Download CSV",
            result.to_csv(index=False).encode(),
            file_name="audit_result.csv",
            mime="text/csv",
        )
