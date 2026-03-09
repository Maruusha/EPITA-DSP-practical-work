import streamlit as st
from datetime import datetime
from services.api_client import make_prediction
import pandas as pd

# helper function to run predict
def run_prediction(payload):
    """Calls API and returns a dataframe of predictions."""
    
    with st.spinner("Calling prediction service..."):
        result = make_prediction(payload)

    if "error" in result:
        st.error(f"API Error: {result['error']}")
        return None

    predictions = result.get("predictions", [])

    if not predictions:
        st.error("No predictions returned from API.")
        return None

    result_df = pd.DataFrame(predictions)

    if result_df is None:
        clear_batch_results()
    else:
        st.session_state["batch_result_df"] = result_df

        # flatten received_input
        input_df = pd.json_normalize(result_df["received_input"])

        # Rename columns for UI display
        input_df = input_df.rename(columns={
            "date": "Date",
            "start_hour": "Start hour",
            "end_hour": "End hour",
            "energy_source": "Energy Source"
        })

        # Add prediction and model columns
        input_df["Predicted Production (MWh)"] = result_df["production"]
        input_df["Model Version"] = result_df["model_version"]

        # Arrange columns in nice order
        result_df = input_df[
            ["Date", "Start hour", "End hour", "Energy Source",
            "Predicted Production (MWh)", "Model Version"]
        ]

    return result_df

# Handle session state
if "batch_result_df" not in st.session_state:
    st.session_state["batch_result_df"] = None

if "last_uploaded_file" not in st.session_state:
    st.session_state["last_uploaded_file"] = None

def clear_batch_results():
    """Safely clear batch prediction results."""
    if "batch_result_df" in st.session_state:
        st.session_state["batch_result_df"] = None

# Start the page UI
st.title("Renewable Energy Production Prediction")

tab_single, tab_batch = st.tabs(["Single Prediction", "Batch Prediction"])

with tab_single:
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
    if st.button("Predict", type="primary"):

        # Validation 
        if start_hour >= end_hour:
            st.error("End hour must be greater than start hour.")
        else:
            # Build payload
            payload = [{
                "date": selected_date.strftime("%Y-%m-%d"),
                "start_hour": start_hour,
                "end_hour": end_hour,
                "energy_source": energy_source
            }]

            # Only runs if validation passes
            result_df = run_prediction(payload)

            if result_df is not None:
                st.success("Prediction successful!")
                st.dataframe(result_df, use_container_width=True)

with tab_batch:

    uploaded_file = st.file_uploader(
        "Upload CSV file for batch prediction",
        type=["csv"]
    )

    # Reset results if a new file is uploaded
    if uploaded_file is not None:
        if st.session_state.get("last_uploaded_file") != uploaded_file.name:
            st.session_state["batch_result_df"] = None
            st.session_state["last_uploaded_file"] = uploaded_file.name

    # Show preview only
    if uploaded_file is not None:
        try:
            batch_df = pd.read_csv(uploaded_file)

            st.write("Preview of uploaded data:")
            st.dataframe(batch_df.head(), use_container_width=True)

        except Exception as e:
            clear_batch_results()
            st.error(f"Invalid CSV file: {e}")
            st.stop()

    # Predict button (only triggers prediction)
    if st.button("Predict Batch", type="primary"):

        if uploaded_file is None:
            clear_batch_results()
            st.error("Please upload a CSV file first.")
            st.stop()

        uploaded_file.seek(0)
        # Re-read file safely
        batch_df = pd.read_csv(uploaded_file)

        required_columns = [
            "date",
            "start_hour",
            "end_hour",
            "energy_source"
        ]

        if not all(col in batch_df.columns for col in required_columns):
            st.error("This CSV file does not contain required columns.")
            st.stop()

        payload = batch_df.to_dict(orient="records")

        result_df = run_prediction(payload)

        if result_df is not None:
            st.session_state["batch_result_df"] = result_df

    if "batch_result_df" in st.session_state and st.session_state["batch_result_df"] is not None:

        result_df = st.session_state["batch_result_df"]

        st.success("Batch prediction successful!")
        st.dataframe(result_df, use_container_width=True)

        csv = result_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download predictions as CSV",
            data=csv,
            file_name="prediction_results.csv",
            mime="text/csv",
            type="secondary"
        )