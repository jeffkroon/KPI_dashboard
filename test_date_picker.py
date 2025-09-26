import streamlit as st
import pandas as pd
from datetime import date, timedelta

st.set_page_config(
    page_title="Date Picker Test",
    page_icon="📅",
    layout="wide"
)

st.title("📅 Date Picker Test")

# EXACT zoals werkverdeling.py en app.py
filter_col1, filter_col2 = st.columns([1, 2])

with filter_col1:
    max_date = date.today()
    min_date_default = max_date - timedelta(days=30)
    date_range = st.date_input(
        "📅 Analyseperiode",
        (min_date_default, max_date),
        min_value=date(2020, 1, 1),
        max_value=max_date,
        help="Selecteer de periode die u wilt analyseren."
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date_default, max_date

with filter_col2:
    st.write("")
    if st.button("📅 Alles", help="Selecteer alle beschikbare data", use_container_width=True):
        # Set to earliest possible date to today
        st.session_state["test_start_date"] = date(2020, 1, 1)
        st.session_state["test_end_date"] = max_date
        st.rerun()

# Convert to datetime objects only for pandas filtering
start_date_dt = pd.to_datetime(start_date)
end_date_dt = pd.to_datetime(end_date)

# Validate date range
if start_date > end_date:
    st.error("⚠️ Start datum moet voor eind datum liggen!")
    st.stop()

# Display selected period
months = [
    "Januari", "Februari", "Maart", "April", "Mei", "Juni",
    "Juli", "Augustus", "September", "Oktober", "November", "December"
]
start_month_name = months[start_date.month - 1]
end_month_name = months[end_date.month - 1]
st.info(f"📊 Geselecteerde periode: {start_month_name} {start_date.year} tot {end_month_name} {end_date.year}")

# Debug info
st.markdown("### Debug Info:")
st.write(f"**Date Range:** {date_range}")
st.write(f"**Start Date:** {start_date} (type: {type(start_date)})")
st.write(f"**End Date:** {end_date} (type: {type(end_date)})")
st.write(f"**Start Date DT:** {start_date_dt}")
st.write(f"**End Date DT:** {end_date_dt}")

# Test session state
if "test_start_date" in st.session_state:
    st.write(f"**Session State Start:** {st.session_state['test_start_date']}")
if "test_end_date" in st.session_state:
    st.write(f"**Session State End:** {st.session_state['test_end_date']}")
