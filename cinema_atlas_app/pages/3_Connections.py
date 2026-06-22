"""
Film connections graph — one hop from a film to its people, genres,
and other films that share those genres.
Uses streamlit-agraph (react-force-graph wrapper).
"""

import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from lib import data, tmdb, theme

st.set_page_config(page_title="Connections · Cinema Atlas", page_icon="🕸️", layout="wide")
theme.inject()

theme.eyebrow("Connections")
st.markdown("# Film network")
st.markdown(
    "<p style='color:#8A8779;margin-top:-0.4rem;'>Pick a film to see how it connects "
    "to its cast, crew, genres, and related films.</p>",
    unsafe_allow_html=True,
)

# --- film search ---
term = st.text_input("Search a film", placeholder="e.g. Parasite, Dune, Sinners…",
                     label_visibility="collapsed")

if "graph_film_id" not in st.session_state:
    st.session_state.graph_film_id = None
if "graph_film_title" not in st.session_state:
    st.session_state.graph_film_title = ""

if term:
    results = data.search_films(term, limit=10)
    if results.empty:
        st.info("No films found.")
    else:
        cols = st.columns(5)
        for i, row in results.iterrows():
            with cols[i % 5]:
                poster = tmdb.img_url(row["poster_path"], "w185")
                if poster:
                    st.image(poster, use_container_width=True)
                yr = str(row["release_date"])[:4] if row["release_date"] else "—"
                if st.button(f"{row['title']} ({yr})", key=f"gp_{row['film_id']}",
                             use_container_width=True):
                    st.session_state.graph_film_id = int(row["film_id"])
                    st.session_state.graph_film_title = row["title"]
                    st.rerun()

fid = st.session_state.graph_film_id
if not fid:
    st.caption("Search above and pick a film to build its connection graph.")
    st.stop()

# --- controls ---
st.divider()
c1, c2 = st.columns([3, 1])
with c1:
    st.markdown(f"### {st.session_state.graph_film_title}")
with c2:
    related_limit = st.slider("Related films per genre", 2, 10, 5)

# --- build graph data ---
with st.spinner("Building graph…"):
    film      = data.film_detail(fid)
    genres    = data.film_genres(fid)
    cast      = data.film_cast(fid, limit=10)
    crew      = data.film_crew(fid)

    # related films: for each genre, get top N by vote_count (excluding this film)
    related_rows = []
    for _, g in genres.iterrows():
        genre_name = g["genre_name"]
        df = data.run_query(f"""
            SELECT m.film_id, m.title, m.vote_count
            FROM milkmoo.silver.film_genres fg
            JOIN milkmoo.silver.movies m ON fg.film_id = m.film_id
            JOIN milkmoo.silver.genres g ON fg.genre_id = g.genre_id
            WHERE g.genre_name = ?
              AND m.film_id != ?
              AND m.vote_count >= 200
            ORDER BY m.vote_count DESC
            LIMIT ?
        """, (genre_name, fid, related_limit))
        for _, r in df.iterrows():
            related_rows.append({
                "film_id": int(r["film_id"]),
                "title": r["title"],
                "genre": genre_name,
            })

# --- build nodes + edges ---
nodes = []
edges = []
seen_nodes = set()

# colour palette per node type
COLOR_FILM    = theme.AMBER          # center film
COLOR_RELATED = "#8a6e2f"            # related films (muted amber)
COLOR_PERSON  = "#4a7c9e"            # people (steel blue)
COLOR_GENRE   = "#5a9e6f"            # genres (muted green)

# center film node
yr = str(film.get("release_date", ""))[:4]
nodes.append(Node(
    id=f"film_{fid}",
    label=f"{film.get('title', '')} ({yr})",
    size=35,
    color=COLOR_FILM,
    font={"color": theme.CREAM, "size": 14},
))
seen_nodes.add(f"film_{fid}")

# genre nodes + edges
for _, g in genres.iterrows():
    gid = f"genre_{g['genre_name']}"
    if gid not in seen_nodes:
        nodes.append(Node(
            id=gid,
            label=g["genre_name"],
            size=22,
            color=COLOR_GENRE,
            font={"color": theme.CREAM, "size": 12},
        ))
        seen_nodes.add(gid)
    edges.append(Edge(source=f"film_{fid}", target=gid, color="#3a5c43", width=1.5))

# related film nodes + edges (via genre)
for r in related_rows:
    rid = f"film_{r['film_id']}"
    gid = f"genre_{r['genre']}"
    if rid not in seen_nodes:
        nodes.append(Node(
            id=rid,
            label=r["title"][:25],
            size=16,
            color=COLOR_RELATED,
            font={"color": "#c9b882", "size": 11},
        ))
        seen_nodes.add(rid)
    edges.append(Edge(source=gid, target=rid, color="#3a3526", width=1))

# cast nodes + edges
for _, c in cast.iterrows():
    pid = f"person_{c['person_id']}"
    if pid not in seen_nodes:
        nodes.append(Node(
            id=pid,
            label=c["name"],
            size=18,
            color=COLOR_PERSON,
            font={"color": theme.CREAM, "size": 11},
        ))
        seen_nodes.add(pid)
    label = c["character"][:20] if c["character"] else "Cast"
    edges.append(Edge(source=f"film_{fid}", target=pid,
                      label=label, color="#2a4a5e", width=1.5))

# crew nodes + edges
for _, c in crew.iterrows():
    pid = f"person_{c['person_id']}"
    if pid not in seen_nodes:
        nodes.append(Node(
            id=pid,
            label=c["name"],
            size=20,
            color=COLOR_PERSON,
            font={"color": theme.CREAM, "size": 12},
        ))
        seen_nodes.add(pid)
    edges.append(Edge(source=f"film_{fid}", target=pid,
                      label=c["job"], color="#2a4a5e", width=2))

# --- graph config ---
config = Config(
    width="100%",
    height=620,
    directed=False,
    physics=True,
    hierarchical=False,
    nodeHighlightBehavior=True,
    highlightColor=theme.AMBER,
    collapsible=False,
    node={"labelProperty": "label"},
    link={"labelProperty": "label", "renderLabel": False},
    backgroundColor=theme.INK,
)

# --- legend ---
leg1, leg2, leg3, leg4 = st.columns(4)
with leg1: st.markdown(f"<span style='color:{COLOR_FILM}'>⬤</span> Selected film", unsafe_allow_html=True)
with leg2: st.markdown(f"<span style='color:{COLOR_PERSON}'>⬤</span> People (cast / crew)", unsafe_allow_html=True)
with leg3: st.markdown(f"<span style='color:{COLOR_GENRE}'>⬤</span> Genres", unsafe_allow_html=True)
with leg4: st.markdown(f"<span style='color:{COLOR_RELATED}'>⬤</span> Related films", unsafe_allow_html=True)

# --- render ---
st.caption(f"{len(nodes)} nodes · {len(edges)} edges")
agraph(nodes=nodes, edges=edges, config=config)