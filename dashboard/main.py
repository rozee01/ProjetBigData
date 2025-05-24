import streamlit as st
from components.sidebar import render_sidebar

# Import view components
from components.containers.top_songs import render_top_songs
from components.containers.top_artists import render_top_artists_per_genre
from components.containers.top_genres import render_top_genres
from components.containers.energy_map import render_energy_map
# from components.containers.trending_now import render_trending_now

# Get selections from sidebar
country, view = render_sidebar()

# Page header
st.title("📈 Spotify Insights Dashboard")

# Conditional rendering
if view == "Top Songs":
    render_top_songs(country)
elif view == "Top Artists":
    render_top_artists_per_genre()
elif view == "Top Genre":
    render_top_genres(country)
elif view == "Energy Map":
    render_energy_map(country)
# elif view == "Trending Now":
#     render_trending_now(country)
else:
    st.info("Choose a view from the sidebar.")
