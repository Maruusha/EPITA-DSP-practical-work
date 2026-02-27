import streamlit as st
from datetime import datetime
from services.api_client import make_prediction

st.title("Renewable Energy Production Prediction")

# Select Datetime Section
selected_date = st.date_input("Select date")

col1, col2 = st.columns(2)

with col1:
    start_hour = st.selectbox(
        "Start hour",
        options=list(range(24)),
        format_func=lambda x: f"{x:02d}:00"
    )

with col2:
    end_hour = st.selectbox(
        "End hour",
        options=list(range(24)),
        format_func=lambda x: f"{x:02d}:00"
    )

# Energy Source Section
energy_source = st.selectbox(
    "Energy Source",
    options=["Wind", "Solar", "Mixed"]
)

# Click on Predict button
if st.button("Predict"):

    # Validation 
    if start_hour >= end_hour:
       st.error("End hour must be greater than start hour.")
       st.stop()  # Stop execution immediately

    # Build payload
    payload = {
        "date": selected_date.strftime("%Y-%m-%d"),
        "start_hour": start_hour,
        "end_hour": end_hour,
        "energy_source": energy_source
    }

    # Only runs if validation passes
    with st.spinner("Calling prediction service..."):
        result = make_prediction(payload)

    if "error" in result:
        st.error(f"API Error: {result['error']}")
    else:
        production_value = result.get("production")

        st.success("Prediction successful!")
        st.metric(label="Predicted Production Value in MWh", value=production_value)