import streamlit as st
import pandas as pd
from datetime import date
from services.api_client import get_past_predictions

# Handle session state
if "past_result_df" not in st.session_state:
    st.session_state["past_result_df"] = None

if "last_query_params" not in st.session_state:
    st.session_state["last_query_params"] = None

# clear session results
def clear_past_results():
    st.session_state["past_result_df"] = None

# Start the page UI
st.title("Past Predictions")

col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input(
        "Start Date",
        value=date.today()
    )

with col2:
    end_date = st.date_input(
        "End Date",
        value=date.today()
    )

energy_source = st.selectbox(
    "Energy Source",
    options=["Wind", "Solar", "Mixed", "All"]
)

prediction_source = st.selectbox(
    "Prediction Source",
    options=["Webapp", "Scheduled", "All"]
)

# For session
query_params = {
    "start_date": start_date,
    "end_date": end_date,
    "energy_source": energy_source,
    "prediction_source": prediction_source
}

if st.session_state["last_query_params"] != query_params:
    st.session_state["past_result_df"] = None
    st.session_state["last_query_params"] = query_params

# Retrieve Predictions button
if st.button("Retrieve Predictions", type="primary"):

    if start_date > end_date:
        clear_past_results()
        st.error("Start date must be before end date.")
    else:

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "energy_source": energy_source,
            "prediction_source": prediction_source
        }

        with st.spinner("Fetching predictions..."):
            result = get_past_predictions(params)

        if "error" in result:
            clear_past_results()
            st.error(f"API Error: {result["error"]}")
        else:
            past_predictions = result.get("predictions", [])

            if not past_predictions:
                clear_past_results()
                st.warning("No predictions found for the selected filters.")
            else:
                result_df = pd.DataFrame(past_predictions)
                input_df = pd.json_normalize(result_df["received_input"])
                # Rename columns for UI
                input_df = input_df.rename(columns={
                    "date": "Date",
                    "start_hour": "Start hour",
                    "end_hour": "End hour",
                    "energy_source": "Energy Source",
                    "prediction_source": "Prediction Source"
                })

                # Add prediction and metadata
                input_df["Predicted Production (MWh)"] = result_df["production"]
                input_df["Model Version"] = result_df["model_version"]

                st.session_state["past_result_df"] = input_df
            
if st.session_state["past_result_df"] is not None:

    df = st.session_state["past_result_df"]

    st.success(f"{len(df)} predictions retrieved.")
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download results as CSV",
        data=csv,
        file_name="past_predictions.csv",
        mime="text/csv",
        type="secondary"
    )

