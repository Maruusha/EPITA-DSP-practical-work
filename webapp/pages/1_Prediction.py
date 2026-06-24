import streamlit as st
from services.api_client import make_prediction
import pandas as pd

DEFAULT_SOURCE = "Webapp"
BATCH_CHUNK_SIZE = 100

# -----Session state Initialization-----
if "batch_input_df" not in st.session_state:
    st.session_state["batch_input_df"] = None

if "batch_result_df" not in st.session_state:
    st.session_state["batch_result_df"] = None

if "last_uploaded_file" not in st.session_state:
    st.session_state["last_uploaded_file"] = None


# -----Helper Functions-----
def clear_batch_results():
    if "batch_result_df" in st.session_state:
        st.session_state["batch_result_df"] = None


def show_api_error(error_message, exception=None):
    st.error("Something went wrong while calling the prediction service. Please try again.")

    with st.expander("Show error details"):
        if exception:
            st.exception(exception)
        else:
            st.write(error_message)


def _parse_predictions(predictions):
    """Converts a list of prediction dicts to a display DataFrame."""
    result_df = pd.DataFrame(predictions)

    if "received_input" not in result_df.columns:
        return None

    input_df = pd.json_normalize(result_df["received_input"])
    input_df = input_df.rename(columns={
        "date": "Date",
        "start_hour": "Start hour",
        "end_hour": "End hour",
        "energy_source": "Energy Source",
    })
    input_df["Predicted Production (MWh)"] = result_df["production"]
    input_df["Model Version"] = result_df["model_version"]

    return input_df[
        ["Date", "Start hour", "End hour", "Energy Source",
         "Predicted Production (MWh)", "Model Version"]
    ]


def run_prediction(payload):
    """Calls API in chunks and returns a combined dataframe of predictions."""
    all_predictions = []
    chunks = [payload[i:i + BATCH_CHUNK_SIZE] for i in range(0, len(payload), BATCH_CHUNK_SIZE)]

    with st.spinner(f"Calling prediction service ({len(payload)} rows in {len(chunks)} batch(es))..."):
        for chunk in chunks:
            results = make_prediction(chunk)
            if "error" in results:
                show_api_error(results["error"])
                return None
            all_predictions.extend(results.get("predictions", []))

    if not all_predictions:
        st.error("No predictions returned from API.")
        return None

    result_df = _parse_predictions(all_predictions)
    if result_df is None:
        st.error("Invalid API response format.")
        return None

    return result_df


# -----Page UI-----
st.title("Renewable Energy Production Prediction")

tab_single, tab_batch = st.tabs(["Single Prediction", "Batch Prediction"])

# ----------Single Prediction----------
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
            try:
                # Build payload
                payload = [{
                    "date": selected_date.strftime("%Y-%m-%d"),
                    "start_hour": start_hour,
                    "end_hour": end_hour,
                    "energy_source": energy_source,
                    "prediction_source": DEFAULT_SOURCE
                }]

                # Only runs if validation passes
                result_df = run_prediction(payload)

                if result_df is not None:
                    st.success("Prediction successful!")
                    st.dataframe(result_df, use_container_width=True)
            except Exception as e:
                st.error("Something went wrong during prediction.")

                with st.expander("Show error details"):
                    st.exception(e)

# ----------Batch Prediction----------
with tab_batch:

    uploaded_file = st.file_uploader(
        "Upload CSV file for batch prediction",
        type=["csv"]
    )

    # Handle file upload + store input
    if uploaded_file is not None:
        if st.session_state["last_uploaded_file"] != uploaded_file.name:
            try:
                batch_df = pd.read_csv(uploaded_file)

                st.session_state["batch_input_df"] = batch_df
                st.session_state["last_uploaded_file"] = uploaded_file.name

                clear_batch_results()

            except Exception as e:
                clear_batch_results()
                st.error(f"Invalid CSV file: {e}")

    # Preview
    if st.session_state["batch_input_df"] is not None:
        st.write("Preview of uploaded data:")
        st.dataframe(
            st.session_state["batch_input_df"].head(),
            use_container_width=True
        )

    # Predict button (only triggers prediction)
    if st.button("Predict Batch", type="primary"):

        batch_df = st.session_state.get("batch_input_df")

        if batch_df is None:
            clear_batch_results()
            st.error("Please upload a CSV file first.")
        else:
            try:
                # Normalize column names: lowercase and map dataset aliases
                batch_df = batch_df.copy()
                batch_df.columns = batch_df.columns.str.lower()
                if "source" in batch_df.columns and "energy_source" not in batch_df.columns:
                    batch_df = batch_df.rename(columns={"source": "energy_source"})

                required_columns = [
                    "date",
                    "start_hour",
                    "end_hour",
                    "energy_source"
                ]

                missing = [
                    col for col in required_columns if col not in batch_df.columns
                ]

                if missing:
                    clear_batch_results()
                    st.error(f"This CSV file does not contain required columns. Missing columns: {missing}")
                else:
                    # Normalize date to ISO format (YYYY-MM-DD)
                    batch_df["date"] = pd.to_datetime(batch_df["date"]).dt.strftime("%Y-%m-%d")

                    # Drop rows the API would reject (start_hour >= end_hour or out of 0-23)
                    invalid_mask = (
                        (batch_df["start_hour"] >= batch_df["end_hour"]) |
                        (~batch_df["start_hour"].between(0, 23)) |
                        (~batch_df["end_hour"].between(0, 23))
                    )
                    if invalid_mask.any():
                        st.warning(
                            f"Skipped {invalid_mask.sum()} row(s) with invalid hours "
                            "(start_hour must be less than end_hour, both in range 0–23)."
                        )
                        batch_df = batch_df[~invalid_mask]

                    if batch_df.empty:
                        clear_batch_results()
                        st.error("No valid rows remaining after filtering invalid hours.")
                    else:
                        # Only send the fields the API expects
                        api_columns = ["date", "start_hour", "end_hour", "energy_source"]
                        payload_df = batch_df[api_columns].copy()
                        payload_df["prediction_source"] = DEFAULT_SOURCE

                        payload = payload_df.to_dict(orient="records")

                        result_df = run_prediction(payload)

                        if result_df is not None:
                            st.session_state["batch_result_df"] = result_df
            except Exception as e:
                clear_batch_results()
                st.error("Something went wrong during batch prediction.")

                with st.expander("Show error details"):
                    st.exception(e)

    # Display results
    if "batch_result_df" in st.session_state and st.session_state["batch_result_df"] is not None:

        result_df = st.session_state["batch_result_df"]

        st.success("Batch prediction successful!")
        st.dataframe(result_df, use_container_width=True)

        # Allow Download
        try:
            csv = result_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                label="Download predictions as CSV",
                data=csv,
                file_name="prediction_results.csv",
                mime="text/csv",
                type="secondary"
            )
        except Exception as e:
            st.error("Failed to prepare download file.")

            with st.expander("Show error details"):
                st.exception(e)
