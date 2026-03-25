import streamlit as st
import requests
import os

# 1. Setup
st.set_page_config(
    page_title="ML Prediction System",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Grab the API URL your teammate set in the environment variables
API_URL = os.getenv("REP_PREDICT_API_URL", "http://fastapi:80")

# --- NEW: Auto-Refreshing System Status Sidebar ---
# Notice we removed "st.sidebar." from inside the function
# and replaced it with standard "st." calls.
@st.fragment(run_every="5s")
def live_health_check():
    st.markdown("---")
    st.markdown("### System Status")
    
    try:
        # Pinging the API container
        api_res = requests.get(f"{API_URL}/health", timeout=2)
        if api_res.status_code == 200:
            st.success("🟢 API: Online")
        else:
            st.error(f"🔴 API: Error {api_res.status_code}")
    except requests.exceptions.RequestException:
        st.error("🔴 API: Offline")
        
    st.caption("Status updates automatically every 20s.")

# We call the fragment INSIDE a sidebar context manager to put it in the right place!
with st.sidebar:
    live_health_check()
# ----------------------------------

# 2. Define the "Main" logic in a function
def show_main_content():
    st.title("Welcome to the ML Prediction Platform")
    st.markdown("""
    This application allows you to:

    - Make on-demand the Renewable Energy Production predictions
    - View historical predictions

    Use the sidebar to navigate between pages.
    """)

    st.info("Select a page from the navigation menu on the left.")

# 3. Define Page objects
# Instead of a filename, pass the function name to st.Page
main_page = st.Page(show_main_content, title="Main", default=True)
prediction_page = st.Page("pages/1_Prediction.py", title="Prediction")
past_prediction = st.Page("pages/2_Past_Predictions.py", title="Past Prediction")

# 4. Navigation
pg = st.navigation([main_page, prediction_page, past_prediction])
pg.run()
