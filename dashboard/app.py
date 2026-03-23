
import streamlit as st
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt

st.set_page_config(page_title="Movie Dashboard", layout="wide")

# ---------------- STYLE ----------------
st.markdown("""
<style>
.block-container { padding-top: 0.5rem; }
.card {
    background-color: #ffffff;
    padding: 12px;
    border-radius: 10px;
    box-shadow: 2px 2px 8px rgba(0,0,0,0.08);
    margin-bottom: 10px;
}
</style>
""", unsafe_allow_html=True)

# ---------------- CONNECTION ----------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user="YOUR USERNAME",
    password="PASSWORD",
    account="ACCOUNT IDENTIFIER",
    warehouse="COMPUTE_WH",
    database="MOVIE_DB",
    schema="MOVIE_SCHEMA"
    )

conn = get_connection()

# ---------------- LOAD MAIN DATA ----------------
@st.cache_data(ttl=600)
def load_data():
    query = """
    SELECT 
        m.title,
        m.genre,
        m.rating,
        m.duration,
        f.release_year,
        p.platform_name,
        l.city
    FROM fact_streaming f
    JOIN dim_movies m ON f.movie_id = m.movie_id
    JOIN dim_platform p ON f.platform_id = p.platform_id
    LEFT JOIN dim_location l ON m.title = l.title
    WHERE m.is_current = TRUE
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    return df

# ---------------- LOAD THEATRE ----------------
@st.cache_data(ttl=600)
def load_theatre():
    query = """
    SELECT theatre_name, city
    FROM dim_theatre
    WHERE is_current = TRUE
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    return df

df = load_data()
theatre_df = load_theatre()

# ---------------- CLEAN ----------------
df = df[df["release_year"] > 1900]

df["rating"] = df["rating"].fillna("Unknown")
df["genre"] = df["genre"].fillna("Unknown")
df["city"] = df["city"].fillna("Unknown")

def map_platform(x):
    x = str(x).lower()
    if "netflix" in x: return "Netflix"
    elif "amazon" in x: return "Amazon"
    elif "disney" in x: return "Disney"
    else: return "Movies"

df["platform"] = df["platform_name"].apply(map_platform)

def convert_duration(x):
    try: return int(str(x).split()[0])
    except: return None

df["duration_min"] = df["duration"].apply(convert_duration)
df = df[~df["duration_min"].isin([1, 20])]

# Theatre clean
theatre_df = theatre_df[theatre_df["city"] != "Unknown"]
theatre_df["label"] = theatre_df["theatre_name"] + " (" + theatre_df["city"] + ")"

# ---------------- SIDEBAR ----------------
st.sidebar.title("🎬 Filters")

page = st.sidebar.radio("Page", ["🎥 Movies", "📺 OTT"])

year_min = int(df["release_year"].min())
year_max = int(df["release_year"].max())

platform = st.sidebar.selectbox("Platform", ["All","Netflix","Amazon","Disney","Movies"])
year = st.sidebar.slider("Year", year_min, year_max, (year_min, year_max))
rating = st.sidebar.selectbox("Rating", ["All"] + sorted(df["rating"].unique()))

# ---------------- FILTER ----------------
df_f = df.copy()

if platform != "All":
    df_f = df_f[df_f["platform"] == platform]

if rating != "All":
    df_f = df_f[df_f["rating"] == rating]

df_f = df_f[
    (df_f["release_year"] >= year[0]) &
    (df_f["release_year"] <= year[1])
]

# ---------------- KPI ----------------
def show_kpi():
    k1, k2, k3, k4 = st.columns(4)

    k1.metric("🎬 Movies", df_f["title"].nunique())
    k2.metric("📺 Platforms", df_f["platform"].nunique())
    k3.metric("🌍 Cities", df_f["city"].nunique())
    k4.metric("📊 Records", len(df_f))

# =====================================================
# 🎥 MOVIES PAGE
# =====================================================
if page == "🎥 Movies":

    st.title("🎥 Movies Dashboard")
    show_kpi()

    trend = df_f.groupby("release_year")["title"].nunique()
    genre = df_f["genre"].value_counts().head(10)
    rating_df = df_f[df_f["rating"] != "Unknown"]["rating"].value_counts().head(10)
    duration = df_f["duration_min"].value_counts().head(10)

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("📈 Movies Trend")
        st.line_chart(trend)
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("🎭 Genre")
        st.bar_chart(genre)
        st.markdown('</div>', unsafe_allow_html=True)

    with c3:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("⭐ Ratings")
        st.bar_chart(rating_df)
        st.markdown('</div>', unsafe_allow_html=True)

    c4, c5 = st.columns(2)

    with c4:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("⏱ Duration Count")
        st.bar_chart(duration)
        st.markdown('</div>', unsafe_allow_html=True)

    with c5:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("📊 Rating Share")
        fig, ax = plt.subplots()
        rating_df.plot.pie(autopct="%1.1f%%")
        ax.set_ylabel("")
        st.pyplot(fig)
        st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# 📺 OTT PAGE
# =====================================================
elif page == "📺 OTT":

    st.title("📺 OTT Dashboard")
    show_kpi()

    platform_count = df_f["platform"].value_counts()
    movies_per_platform = df_f.groupby("platform")["title"].nunique()
    top_theatres = theatre_df["label"].value_counts().head(10)

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("📺 Platform Distribution")
        st.bar_chart(platform_count)
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("🎬 Movies per Platform")
        st.bar_chart(movies_per_platform)
        st.markdown('</div>', unsafe_allow_html=True)

    with c3:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("🎭 Top Theatres")
        st.bar_chart(top_theatres)
        st.markdown('</div>', unsafe_allow_html=True)

    c4, c5 = st.columns(2)

    with c4:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("📊 Platform Share")
        fig, ax = plt.subplots()
        platform_count.plot.pie(autopct="%1.1f%%")
        ax.set_ylabel("")
        st.pyplot(fig)
        st.markdown('</div>', unsafe_allow_html=True)

    with c5:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("📈 Movies Trend")
        st.line_chart(df_f.groupby("release_year")["title"].nunique())
        st.markdown('</div>', unsafe_allow_html=True)
