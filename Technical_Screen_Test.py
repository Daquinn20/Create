"""
Minimal Technical Analysis Screen - Test Version
"""
import streamlit as st

# Page config MUST be first
st.set_page_config(
    page_title="Technical Screen Test",
    page_icon="📈",
    layout="wide"
)

st.title("Technical Analysis Screen - Test")
st.write("If you can see this, the basic app works!")

# Simple sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select", ["Page 1", "Page 2"])

if page == "Page 1":
    st.header("Page 1")
    st.write("This is page 1")
elif page == "Page 2":
    st.header("Page 2")
    st.write("This is page 2")
