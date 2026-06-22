"""
Cinema Atlas — main entry / catalog overview.

Run:  streamlit run Home.py
Pages live in pages/ and appear in the sidebar automatically.
"""

import streamlit as st
import altair as alt
from lib import data, theme

st.set_page_config(page_title="Cinema Atlas", page_icon="🎬", layout="wide")
theme.inject()

# --- header ---
theme.eyebrow("Cinema Atlas")
st.markdown("# A living map of cinema")
st.markdown(
    "<p style='color:#8A8779; max-width:60ch; margin-top:-0.4rem;'>"
    "Built on the TMDB silver layer — films, people, genres, releases, reviews, "
    "and a box-office time series. Search a title for its full profile, or explore "
    "the catalog below.</p>",
    unsafe_allow_html=True,
)

# --- connection guard ---
try:
    summ = data.catalog_summary()
except Exception as e:
    st.error(
        "Couldn't reach the Databricks warehouse. Check your `.env` "
        "(DATABRICKS_HOST / HTTP_PATH / TOKEN) and that the SQL Warehouse is running."
    )
    st.caption(f"Details: {e}")
    st.stop()

# --- headline metrics ---
st.write("")
c1, c2, c3, c4, c5 = st.columns(5)
with c1: theme.metric_card(f"{int(summ['films']):,}", "Films")
with c2: theme.metric_card(f"{int(summ['people']):,}", "People")
with c3: theme.metric_card(f"{int(summ['reviews']):,}", "Reviews")
with c4: theme.metric_card(int(summ["genres"]), "Genres")
with c5:
    span = f"{str(summ['earliest'])[:4]}–{str(summ['latest'])[:4]}"
    theme.metric_card(span, "Release span")

st.write("")
st.write("")

# --- films per year ---
theme.eyebrow("Releases over time")
fpy = data.films_per_year()
if not fpy.empty:
    chart = (
        alt.Chart(fpy)
        .mark_area(
            line={"color": theme.AMBER},
            color=alt.Gradient(
                gradient="linear",
                stops=[
                    alt.GradientStop(color="#1c1a12", offset=0),
                    alt.GradientStop(color=theme.AMBER, offset=1),
                ],
                x1=1, x2=1, y1=1, y2=0,
            ),
        )
        .encode(
            x=alt.X("year:O", title=None, axis=alt.Axis(labelColor=theme.MUTED, grid=False)),
            y=alt.Y("films:Q", title=None, axis=alt.Axis(labelColor=theme.MUTED, grid=False)),
            tooltip=["year", "films"],
        )
        .properties(height=240)
        .configure_view(strokeWidth=0)
        .configure(background="#0E0E12")
    )
    st.altair_chart(chart, use_container_width=True)

st.write("")

# --- two columns: top genres + rating by genre ---
left, right = st.columns(2)

with left:
    theme.eyebrow("Most common genres")
    tg = data.top_genres()
    if not tg.empty:
        bar = (
            alt.Chart(tg.head(12))
            .mark_bar(color=theme.AMBER, cornerRadiusEnd=3)
            .encode(
                x=alt.X("films:Q", title=None, axis=alt.Axis(labelColor=theme.MUTED, grid=False)),
                y=alt.Y("genre:N", sort="-x", title=None, axis=alt.Axis(labelColor=theme.CREAM)),
                tooltip=["genre", "films"],
            )
            .properties(height=320)
            .configure_view(strokeWidth=0)
            .configure(background="#0E0E12")
        )
        st.altair_chart(bar, use_container_width=True)

with right:
    theme.eyebrow("Best-rated genres")
    rbg = data.rating_by_genre()
    if not rbg.empty:
        bar = (
            alt.Chart(rbg.head(12))
            .mark_bar(color=theme.CREAM, cornerRadiusEnd=3)
            .encode(
                x=alt.X("avg_rating:Q", title=None, scale=alt.Scale(domain=[5, 8.5]),
                        axis=alt.Axis(labelColor=theme.MUTED, grid=False)),
                y=alt.Y("genre:N", sort="-x", title=None, axis=alt.Axis(labelColor=theme.CREAM)),
                tooltip=["genre", "avg_rating", "films"],
            )
            .properties(height=320)
            .configure_view(strokeWidth=0)
            .configure(background="#0E0E12")
        )
        st.altair_chart(bar, use_container_width=True)

st.write("")
st.caption("Use the sidebar → **Film profile** to search a title, or **Analytics** for deeper charts.")