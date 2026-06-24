import streamlit as st
from services.api_client import check_health

# 1. Setup
st.set_page_config(
    page_title="ML Prediction System",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


# --- Auto-Refreshing System Status Sidebar ---
@st.fragment(run_every="20s")
def live_health_check():
    st.markdown("---")
    st.markdown("### System Status")

    # Call the helper function from client.py
    health = check_health()

    if health["status"] == "online":
        st.success("🟢 API: Online")
    elif health["status"] == "error":
        st.error(f"🔴 API: Error {health['code']}")
    else:
        st.error("🔴 API: Offline")

    st.caption("Status updates automatically every 20s.")


# We call the fragment INSIDE a sidebar context manager
with st.sidebar:
    live_health_check()


# 2. Define the "Main" logic in a function
def show_main_content():
    st.title("Welcome to the ML Prediction Platform")
    st.markdown("""
    This application allows you to:

    - Make on-demand Renewable Energy Production predictions
    - View historical predictions

    Use the sidebar to navigate between pages.
    """)

    st.info("Select a page from the navigation menu on the left.")


# 3. Define Page objects
main_page = st.Page(show_main_content, title="Main", default=True)
prediction_page = st.Page("pages/1_Prediction.py", title="Prediction")
past_prediction = st.Page("pages/2_Past_Predictions.py", title="Past Prediction")

# 4. Navigation
pg = st.navigation([main_page, prediction_page, past_prediction])
pg.run()
