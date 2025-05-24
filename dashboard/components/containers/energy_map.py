import streamlit as st
import pandas as pd
import plotly.express as px
from mongo_cnx import connect_to_mongo

def render_energy_map(country: str):
    st.subheader(f"⚡ Average Energy & Danceability {'(All Countries)' if country == 'All Countries' else f'in {country}'}")

    # Load data from MongoDB
    db = connect_to_mongo()
    collection = db["energy_per_country"]
    df = pd.DataFrame(list(collection.find({}, {"_id": 0})))

    if country == "All Countries":
        if df.empty:
            st.warning("No data available.")
            return

        metric = st.selectbox("Select metric to display", ["avg_energy", "avg_danceability"],
                              format_func=lambda x: "Energy" if x == "avg_energy" else "Danceability")

        fig = px.choropleth(
            df,
            locations="country",
            locationmode="country names",
            color=metric,
            hover_name="country",
            title=f"Global {metric.replace('_', ' ').capitalize()} by Country",
            color_continuous_scale="Plasma"
        )
        fig.update_layout(margin={"r":0, "t":40, "l":0, "b":0})
        st.plotly_chart(fig, use_container_width=True)

    else:
        # Optional: Show metrics for a single country
        row = df[df["country"] == country]
        if not row.empty:
            st.metric("Average Energy", f"{row.iloc[0]['avg_energy']:.2f}")
            st.metric("Average Danceability", f"{row.iloc[0]['avg_danceability']:.2f}")
        else:
            st.warning("No data available for this country.")
