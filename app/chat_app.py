import streamlit as st
import os
import sys

# Get the absolute path of the src directory
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

# Add src directory to sys.path
sys.path.insert(0, src_path)

from packages.gdelt_data_scraper import GDELTDataScraper
from packages.pyspark_agent import PysparkAgent

st.title("GDELT Data Scraper and Chatbot")

# Section 1: Data Scraper
st.header("Get your base data")

# Get start and end date from user input
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

# Function to fetch and store data in session state
def fetch_data():
    # Clear previous fetched data if present
    if 'fetched_data' in st.session_state:
        del st.session_state['fetched_data']
    
    scraper = GDELTDataScraper()
    with st.spinner("Fetching data..."):
        st.session_state.fetched_data = scraper.scrape_data(start_date, end_date)
    st.success("Data fetched successfully!")

# Trigger function with start and end date
if st.button("Fetch Data"):
    fetch_data()

# Section 2: Chatbot
st.header("GDELT Chatbot")

def generate_response(input_text, agent_executor):
    response = agent_executor.invoke(input_text)['output']
    st.info(response)

# Initialize the PysparkAgent only once
if 'agent_executor' not in st.session_state:
    st.warning("Loading data and preparing agent...")
    pyspark_agent = PysparkAgent()
    st.session_state.agent_executor = pyspark_agent.agent_executor

with st.form("chat_form"):
    text = st.text_area(
        "Enter question:",
        "Which country is most affected by sanctions?",
    )
    submitted = st.form_submit_button("Submit")
    if submitted:
        generate_response(text, st.session_state.agent_executor)
