import streamlit as st
import pandas as pd
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from mongo_cnx import connect_to_mongo

def render_sidebar():
    db = connect_to_mongo()
    collection = db["top_genre_all_time"]  
    df_genres = pd.DataFrame(list(collection.find({}, {"_id": 0})))
    countries = sorted(df_genres["country"].dropna().unique())
    countries.insert(0, "All Countries")  

    with st.sidebar:
        st.title("🎧 Spotify Dashboard")

        selected_country = st.selectbox("🌍 Choose a Country", countries)
        selected_view = st.radio(
            "📊 Choose a View",
            ("Top Songs", "Top Artists", "Top Genre", "Energy Map", "Trending Now")
        )


    return selected_country, selected_view
