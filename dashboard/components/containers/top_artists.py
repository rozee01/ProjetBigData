import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from mongo_cnx import connect_to_mongo

def render_top_artists_per_genre():
    st.subheader("🌞 Top Artists per Genre (Sunburst View)")

    # Connect to MongoDB
    db = connect_to_mongo()
    collection = db["top_artist_per_genre"]

    # Load data
    df = pd.DataFrame(list(collection.find({}, {"_id": 0})))

    if df.empty:
        st.warning("No data available.")
        return

    # Prepare sunburst
    fig = px.sunburst(
        df,
        path=["genre", "artist"],  # hierarchy
        values="total_streams",
        color="genre",
        title="Top Artists per Genre by Total Streams"
    )

    fig.update_layout(margin=dict(t=40, l=10, r=10, b=10))
    st.plotly_chart(fig, use_container_width=True)
