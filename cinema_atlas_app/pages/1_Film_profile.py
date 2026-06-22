"""Film profile — search a title, then a rich detail view."""

import streamlit as st
import altair as alt
from lib import data, tmdb, theme

st.set_page_config(page_title="Film profile · Cinema Atlas", page_icon="🎬", layout="wide")
theme.inject()

theme.eyebrow("Film profile")
st.markdown("# Search the catalog")

term = st.text_input("Search by title", placeholder="e.g. Dune, Parasite, Sinners…", label_visibility="collapsed")

# Keep the selected film across reruns
if "film_id" not in st.session_state:
    st.session_state.film_id = None

# --- search results ---
if term:
    results = data.search_films(term)
    if results.empty:
        st.info("No films match that title.")
    else:
        st.caption(f"{len(results)} result(s)")
        cols = st.columns(6)
        for i, row in results.iterrows():
            with cols[i % 6]:
                poster = tmdb.img_url(row["poster_path"], "w342")
                if poster:
                    st.image(poster, use_container_width=True)
                yr = str(row["release_date"])[:4] if row["release_date"] else "—"
                if st.button(f"{row['title']} ({yr})", key=f"pick_{row['film_id']}", use_container_width=True):
                    st.session_state.film_id = int(row["film_id"])
                    st.rerun()

st.divider()

# --- profile view ---
fid = st.session_state.film_id
if not fid:
    st.caption("Search above and pick a film to see its profile.")
    st.stop()

m = data.film_detail(fid)
if not m:
    st.warning("Film not found.")
    st.stop()

# layout: poster | details
poster_col, info_col = st.columns([1, 2.4], gap="large")

with poster_col:
    poster = tmdb.img_url(m.get("poster_path"), "w500")
    if poster:
        st.image(poster, use_container_width=True)
    else:
        st.markdown(
            "<div style='aspect-ratio:2/3;background:#16161C;border-radius:8px;"
            "display:flex;align-items:center;justify-content:center;color:#8A8779;'>No poster</div>",
            unsafe_allow_html=True,
        )

with info_col:
    yr = str(m.get("release_date"))[:4] if m.get("release_date") else ""
    st.markdown(f"# {m.get('title')} <span style='color:#8A8779;font-weight:400;'>({yr})</span>",
                unsafe_allow_html=True)
    if m.get("tagline"):
        st.markdown(f"<p style='color:{theme.AMBER};font-style:italic;margin-top:-0.6rem;'>“{m['tagline']}”</p>",
                    unsafe_allow_html=True)

    # genre chips
    g = data.film_genres(fid)
    if not g.empty:
        theme.chips(g["genre_name"].tolist())

    # quick stats row
    st.write("")
    s1, s2, s3, s4 = st.columns(4)
    with s1: theme.metric_card(f"{m.get('vote_average', 0):.1f}", "Rating")
    with s2: theme.metric_card(f"{int(m.get('vote_count') or 0):,}", "Votes")
    rev = m.get("revenue") or 0
    with s3: theme.metric_card(f"${rev/1e6:.0f}M" if rev else "—", "Revenue")
    with s4: theme.metric_card(f"{int(m.get('runtime') or 0)}m", "Runtime")

    if m.get("overview"):
        st.write("")
        st.markdown(f"<p style='color:#C9C3B6;line-height:1.6;'>{m['overview']}</p>", unsafe_allow_html=True)

    # key crew inline
    crew = data.film_crew(fid)
    if not crew.empty:
        st.write("")
        bits = []
        for _, c in crew.iterrows():
            bits.append(f"<b style='color:{theme.CREAM}'>{c['name']}</b> "
                        f"<span style='color:#8A8779'>{c['job']}</span>")
        st.markdown(" &nbsp;·&nbsp; ".join(bits[:6]), unsafe_allow_html=True)

# --- trailer (live TMDB call) ---
st.write("")
theme.eyebrow("Trailer")
key = tmdb.get_trailer_key(fid)
if key:
    st.video(f"https://www.youtube.com/watch?v={key}")
else:
    st.caption("No trailer available for this title.")

# --- cast ---
st.write("")
theme.eyebrow("Top billed cast")
cast = data.film_cast(fid)
if cast.empty:
    st.caption("No cast records.")
else:
    cols = st.columns(6)
    for i, row in cast.iterrows():
        with cols[i % 6]:
            pic = tmdb.img_url(row["profile_path"], "w185")
            if pic:
                st.image(pic, use_container_width=True)
            st.markdown(f"<b style='font-size:0.85rem'>{row['name']}</b>", unsafe_allow_html=True)
            if row["character"]:
                st.markdown(f"<span style='color:#8A8779;font-size:0.78rem'>{row['character']}</span>",
                            unsafe_allow_html=True)

# --- box office / popularity time series (SCD2) ---
hist = data.film_metric_history(fid)
if not hist.empty and len(hist) > 1:
    st.write("")
    theme.eyebrow("Metrics over time")
    metric = st.radio("Metric", ["revenue", "popularity", "vote_count"],
                      horizontal=True, label_visibility="collapsed")
    line = (
        alt.Chart(hist)
        .mark_line(color=theme.AMBER, point=True)
        .encode(
            x=alt.X("snapshot_ts:T", title=None, axis=alt.Axis(labelColor=theme.MUTED)),
            y=alt.Y(f"{metric}:Q", title=None, axis=alt.Axis(labelColor=theme.MUTED)),
            tooltip=["snapshot_ts:T", f"{metric}:Q"],
        )
        .properties(height=220)
        .configure_view(strokeWidth=0)
        .configure(background="#0E0E12")
    )
    st.altair_chart(line, use_container_width=True)

# --- reviews ---
rev = data.film_reviews(fid)
if not rev.empty:
    st.write("")
    theme.eyebrow("Reviews")
    for _, r in rev.iterrows():
        rating = f" · ★ {r['author_rating']:.0f}" if r["author_rating"] else ""
        body = (r["content"] or "")[:600]
        st.markdown(
            f"<div class='review'><div class='who'>{r['author']}{rating}</div>"
            f"<div class='txt'>{body}…</div></div>",
            unsafe_allow_html=True,
        )