import streamlit as st
import pandas as pd
import plotly.express as px
from mongo_cnx import connect_to_mongo

def render_top_songs(country: str):
    st.subheader(f"🎵 Top Song{'s' if country == 'All' else f' in {country}'} (All-Time)")
    
    # Load data from MongoDB
    db = connect_to_mongo()   
    collection = db["top_song_all_time"]
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
            hover_name="track_name",
            title="Top Song by Country",
            color_continuous_scale="Viridis"
        )
        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)

    else:
        top_song = df[df["country"] == country]
        if not top_song.empty:
            track = top_song.iloc[0]["track_name"]
            streams = top_song.iloc[0]["total_streams"]
            st.success(f"**{track}** with **{streams:,} streams**")
        else:
            st.warning("No data available for this country.")
