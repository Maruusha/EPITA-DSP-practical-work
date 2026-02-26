import streamlit as st

# 1. Setup
st.set_page_config(
    page_title="ML Prediction System",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

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