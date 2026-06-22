"""Analytics — catalog-wide leaderboards and distributions."""

import streamlit as st
import altair as alt
from lib import data, tmdb, theme

st.set_page_config(page_title="Analytics · Cinema Atlas", page_icon="📊", layout="wide")
theme.inject()

theme.eyebrow("Analytics")
st.markdown("# Explore the catalog")

# --- leaderboard controls ---
METRICS = {
    "Highest rated": "vote_average",
    "Most voted": "vote_count",
    "Highest revenue": "revenue",
    "Most popular": "popularity",
}
c1, c2 = st.columns([2, 1])
with c1:
    label = st.selectbox("Rank films by", list(METRICS.keys()), label_visibility="collapsed")
with c2:
    min_votes = st.slider("Min votes", 0, 5000, 500, step=100)

metric = METRICS[label]
films = data.top_films(metric=metric, min_votes=min_votes, limit=24)

st.caption(f"Top {len(films)} films · {label.lower()} · ≥ {min_votes} votes")

# poster grid leaderboard
if films.empty:
    st.info("No films match. Lower the minimum votes.")
else:
    cols = st.columns(8)
    for i, row in films.iterrows():
        with cols[i % 8]:
            poster = tmdb.img_url(row["poster_path"], "w185")
            if poster:
                st.image(poster, use_container_width=True)
            val = row[metric]
            if metric == "revenue":
                shown = f"${val/1e6:.0f}M"
            elif metric == "vote_average":
                shown = f"★ {val:.1f}"
            else:
                shown = f"{int(val):,}"
            st.markdown(
                f"<div style='font-size:0.8rem'><b>{row['title'][:28]}</b></div>"
                f"<div style='color:{theme.AMBER};font-size:0.8rem'>{shown}</div>",
                unsafe_allow_html=True,
            )

st.divider()

# --- rating distribution + revenue vs rating scatter ---
left, right = st.columns(2)

with left:
    theme.eyebrow("Rating vs revenue")
    scatter_src = data.top_films(metric="revenue", min_votes=200, limit=300)
    scatter_src = scatter_src[scatter_src["revenue"] > 0]
    if not scatter_src.empty:
        sc = (
            alt.Chart(scatter_src)
            .mark_circle(opacity=0.55, color=theme.AMBER)
            .encode(
                x=alt.X("vote_average:Q", title="Rating", scale=alt.Scale(domain=[4, 9]),
                        axis=alt.Axis(labelColor=theme.MUTED)),
                y=alt.Y("revenue:Q", title="Revenue", axis=alt.Axis(labelColor=theme.MUTED)),
                size=alt.Size("vote_count:Q", legend=None),
                tooltip=["title", "vote_average", "revenue"],
            )
            .properties(height=300)
            .configure_view(strokeWidth=0)
            .configure(background="#0E0E12")
        )
        st.altair_chart(sc, use_container_width=True)

with right:
    theme.eyebrow("Best-rated genres")
    rbg = data.rating_by_genre()
    if not rbg.empty:
        bar = (
            alt.Chart(rbg)
            .mark_bar(color=theme.CREAM, cornerRadiusEnd=3)
            .encode(
                x=alt.X("avg_rating:Q", title=None, scale=alt.Scale(domain=[5, 8.5]),
                        axis=alt.Axis(labelColor=theme.MUTED)),
                y=alt.Y("genre:N", sort="-x", title=None, axis=alt.Axis(labelColor=theme.CREAM)),
                tooltip=["genre", "avg_rating", "films"],
            )
            .properties(height=300)
            .configure_view(strokeWidth=0)
            .configure(background="#0E0E12")
        )
        st.altair_chart(bar, use_container_width=True)