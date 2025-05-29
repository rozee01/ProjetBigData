import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from mongo_cnx import connect_to_mongo

def render_top_genres(country: str):
    st.subheader(f"🎵 Top Genre{'s' if country == 'All' else f' in {country}'} (All-Time)")
    
    # Load data from MongoDB
    db = connect_to_mongo()   
    collection = db["top_genre_all_time"]
    df = pd.DataFrame(list(collection.find({}, {"_id": 0})))

    if country == "All Countries":
        if df.empty:
            st.warning("No data available.")
            return
        
        # Use Plotly to create a choropleth map
        fig = px.choropleth(
            df,
            locations="country",                # This should match ISO-3 country codes
            locationmode="country names",       # You can also use "ISO-3" if your codes are like "USA"
            color="total_streams",
            hover_name="genre",
            title="Top Genre by Country",
            color_continuous_scale="Viridis"
        )
        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)

    else:
        top_genre = df[df["country"] == country]
        if not top_genre.empty:
            genre = top_genre.iloc[0]["genre"]
            streams = top_genre.iloc[0]["total_streams"]
            st.success(f"**{genre}** with **{streams:,} streams**")
        else:
            st.warning("No data available for this country.")
