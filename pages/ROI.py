import streamlit as st
import pandas as pd
import os
import plotly.express as px
import altair as alt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

st.logo("images/dunion-logo-def_donker-06.png")
st.set_page_config(
    page_title="Customer-analysis",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.title("Customer-analysis")
st.markdown(
    "<small style='color: #9CA3AF;'>⚙️ Dit is de interne kostprijs per gewerkt uur (incl. personeel, overhead, etc.). "
    "Wordt gebruikt om winst en rendabiliteit per klant te schatten.</small>",
    unsafe_allow_html=True
)
kostprijs_per_uur = st.slider("Kostprijs per uur (€)", min_value=30, max_value=150, value=75)

st.markdown(
    """
    <style>
    body {
        background-color: #111827;
        color: white;
    }
    .main, .block-container {
        background-color: #111827 !important;
        color: white;
    }
    .stSlider label { color: white !important; }
    </style>
    """,
    unsafe_allow_html=True
)

# ---------------------------
# 📁 Data voorbereiding
# ---------------------------
df = pd.DataFrame({
    "klant": [
        # Klant A-E
        "Klant A", "Klant A", "Klant A",
        "Klant B", "Klant B", "Klant B",
        "Klant C", "Klant C", "Klant C",
        "Klant D", "Klant D", "Klant D",
        "Klant E", "Klant E", "Klant E",
        # Klant F-J
        "Klant F", "Klant F", "Klant F",
        "Klant G", "Klant G", "Klant G",
        "Klant H", "Klant H", "Klant H",
        "Klant I", "Klant I", "Klant I",
        "Klant J", "Klant J", "Klant J",
        # Klant K-O
        "Klant K", "Klant K", "Klant K",
        "Klant L", "Klant L", "Klant L",
        "Klant M", "Klant M", "Klant M",
        "Klant N", "Klant N", "Klant N",
        "Klant O", "Klant O", "Klant O",
        # Klant P-T
        "Klant P", "Klant P", "Klant P",
        "Klant Q", "Klant Q", "Klant Q",
        "Klant R", "Klant R", "Klant R",
        "Klant S", "Klant S", "Klant S",
        "Klant T", "Klant T", "Klant T"
    ],
    "maand": [
        # 3 maanden per klant
        "2024-01", "2024-02", "2024-03",  # A
        "2024-01", "2024-02", "2024-03",  # B
        "2024-01", "2024-02", "2024-03",  # C
        "2024-01", "2024-02", "2024-03",  # D
        "2024-01", "2024-02", "2024-03",  # E
        "2024-01", "2024-02", "2024-03",  # F
        "2024-01", "2024-02", "2024-03",  # G
        "2024-01", "2024-02", "2024-03",  # H
        "2024-01", "2024-02", "2024-03",  # I
        "2024-01", "2024-02", "2024-03",  # J
        "2024-01", "2024-02", "2024-03",  # K
        "2024-01", "2024-02", "2024-03",  # L
        "2024-01", "2024-02", "2024-03",  # M
        "2024-01", "2024-02", "2024-03",  # N
        "2024-01", "2024-02", "2024-03",  # O
        "2024-01", "2024-02", "2024-03",  # P
        "2024-01", "2024-02", "2024-03",  # Q
        "2024-01", "2024-02", "2024-03",  # R
        "2024-01", "2024-02", "2024-03",  # S
        "2024-01", "2024-02", "2024-03"   # T
    ],
    "omzet": [
        1200, 1300, 1100,    # A
        2000, 1900, 2100,    # B
        800, 700, 750,       # C
        1500, 1400, 1550,    # D
        1000, 950, 1025,     # E
        1100, 1200, 1150,    # F
        1800, 1750, 1900,    # G
        950, 900, 1000,      # H
        1400, 1350, 1450,    # I
        300, 240, 899,    # J
        1600, 1580, 1620,    # K
        900, 950, 925,       # L
        1250, 1300, 1275,    # M
        2000, 2050, 2100,    # N
        1150, 1200, 1175,    # O
        1300, 1350, 1325,    # P
        120, 850, 200,       # Q
        1700, 1680, 1750,    # R
        950, 1000, 975,      # S
        1450, 1500, 1475     # T
    ],
    "uren": [
        10, 12, 9,           # A
        25, 23, 26,          # B
        8, 7, 7.5,           # C
        18, 17, 19,          # D
        15, 13, 14,          # E
        12, 13, 12.5,        # F
        21, 20, 22,          # G
        10, 9, 10.5,         # H
        16, 15, 17,          # I
        13, 13.5, 14,        # J
        20, 19, 21,          # K
        11, 12, 11.5,        # L
        14, 15, 14.5,        # M
        27, 28, 26,          # N
        13, 14, 13.5,        # O
        15, 16, 15.5,        # P
        7, 8, 7.5,           # Q
        22, 21, 23,          # R
        12, 13, 12.5,        # S
        18, 19, 18.5         # T
    ]
})

df_extra = pd.DataFrame({
    "klant": ["Klant U", "Klant U", "Klant U",  # topklant
              "Klant V", "Klant V", "Klant V",  # verliesgevend
              "Klant W", "Klant W", "Klant W",  # hoog volume, goed renderend
              "Klant X", "Klant X", "Klant X",  # matige klant
              "Klant Y", "Klant Y", "Klant Y",  # gemiddelde klant
              "Klant Z", "Klant Z", "Klant Z",
              "Klant AA", "Klant AA", "Klant AA",
              "Klant AB", "Klant AB", "Klant AB",
              "Klant AC", "Klant AC", "Klant AC",
              "Klant AD", "Klant AD", "Klant AD",
              "Klant AE", "Klant AE", "Klant AE",
              "Klant AF", "Klant AF", "Klant AF",
              "Klant AG", "Klant AG", "Klant AG",
              "Klant AH", "Klant AH", "Klant AH",
              "Klant AI", "Klant AI", "Klant AI"],
    "maand": ["2024-01", "2024-02", "2024-03"] * 15,
    "omzet": [5500, 5700, 5600,   # Klant U (aangepast)
              900, 850, 875,      # Klant V
              6200, 6400, 6300,   # Klant W
              1200, 1100, 1150,   # Klant X
              3200, 3300, 3250,   # Klant Y
              4700, 4600, 4800,     # Z - winstgevend
              5000, 4900, 5100,     # AA - winstgevend
              4500, 4400, 4600,     # AB - winstgevend
              5200, 5300, 5100,     # AC - winstgevend
              1100, 1050, 1075,     # AD - gemiddeld
              800, 850, 825,        # AE - laag
              1500, 1550, 1525,     # AF - gemiddeld
              1000, 950, 975,       # AG - laag
              1200, 1150, 1175,     # AH - gemiddeld
              900, 925, 875         # AI - laag
              ],
    "uren": [48, 50, 49,         # Klant U (aangepast)
             45, 44, 46,         # Klant V
             60, 62, 61,         # Klant W
             20, 22, 21,         # Klant X
             36, 35, 37,         # Klant Y
             35, 36, 34,           # Z
             38, 39, 37,           # AA
             33, 32, 34,           # AB
             40, 41, 39,           # AC
             14, 15, 14.5,         # AD
             10, 9, 10.5,          # AE
             18, 19, 17.5,         # AF
             13, 12, 13.5,         # AG
             16, 15, 16.5,         # AH
             11, 10.5, 12          # AI
             ]
})
df = pd.concat([df, df_extra], ignore_index=True)
df["winst"] = df["omzet"] - (df["uren"] * kostprijs_per_uur)
df["rendabiliteit"] = df["winst"] / df["uren"]

def card(header, value, unit=""):
    return f"""
    <div style="
        background-color: #1F2937;
        padding: 1rem;
        border-radius: 0.75rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        margin-bottom: 1rem;
    ">
        <div style="font-size: 1rem; color: #9CA3AF;">{header}</div>
        <div style="font-size: 1.5rem; font-weight: bold; color: white;">{value} {unit}</div>
    </div>
    """

st.subheader("📌 Kerncijfers per maand (gemiddeld)")

avg_omzet = df.groupby("maand")["omzet"].mean().mean()
avg_uren = df.groupby("maand")["uren"].mean().mean()
avg_rendabiliteit = df["rendabiliteit"].mean()

kpi1, kpi2, kpi3 = st.columns(3)

with kpi1:
    st.markdown(card("💶 Gem. Omzet", f"€ {avg_omzet:.2f}"), unsafe_allow_html=True)

with kpi2:
    st.markdown(card("⏱️ Gem. Uren", f"{avg_uren:.1f}", "u"), unsafe_allow_html=True)

with kpi3:
    st.markdown(card("💰 Rendabiliteit", f"€ {avg_rendabiliteit:.2f}"), unsafe_allow_html=True)

# ---------------------------
# ⚙️ Machine Learning Clustering
# ---------------------------
with st.expander("Bekijk klantclassificatie op basis van winstgevendheid"):
    # Machine learning classificatie op basis van KMeans clustering
    features = df[["winst", "uren", "rendabiliteit"]]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    kmeans = KMeans(n_clusters=3, random_state=0)
    df["cluster"] = kmeans.fit_predict(X_scaled)

    # Nieuwe classificatie op basis van percentielen
    # Bereken percentielen
    rend_q75 = df["rendabiliteit"].quantile(0.75)
    winst_q75 = df["winst"].quantile(0.75)
    rend_q25 = df["rendabiliteit"].quantile(0.25)
    winst_q25 = df["winst"].quantile(0.25)

    # Default label
    df["label"] = "⚖️ Gemiddeld"

    # Slechte klanten: lage rendabiliteit én lage winst
    df.loc[(df["rendabiliteit"] < rend_q25) & (df["winst"] < winst_q25), "label"] = "❌ slecht"

    # Winstgevende klanten: hoge rendabiliteit én hoge winst
    df.loc[(df["rendabiliteit"] > rend_q75) & (df["winst"] > winst_q75), "label"] = "💎 optimaal"

    # Scatterplot met klantnamen in hover
    fig = px.scatter(
        df, x="uren", y="omzet", color="label",
        hover_data=["klant", "maand", "rendabiliteit"],
        title="Klantsegmentatie (KMeans)", template="plotly_dark"
    )
    st.plotly_chart(fig, use_container_width=True)

    # Gesorteerde tabel met clusterinformatie
    st.dataframe(df[["klant", "cluster", "label", "rendabiliteit", "omzet", "uren"]].sort_values("cluster"))


# ---------------------------
# 🔍 Zoek een klant
# ---------------------------
st.subheader("🔎 Zoek een klant")
st.markdown("""
<style>
.stTextInput > label {
    color: white !important;
}
</style>
""", unsafe_allow_html=True)
zoek_klant = st.text_input("Voer klantnaam in (bijv. 'Klant A')", label_visibility="visible").strip()
if zoek_klant:
    klantdata = df[df["klant"].str.lower() == zoek_klant.lower()]
    if not klantdata.empty:
        st.markdown("""
<style>
thead tr th {
    color: white !important;
}
</style>
""", unsafe_allow_html=True)
        st.dataframe(
            klantdata.sort_values("maand")[
                ["maand", "omzet", "uren", "winst", "rendabiliteit", "label"]
            ]
        )
    else:
        st.warning("Klant niet gevonden. Controleer de spelling.")





# ---------------------------
# 🗺️ Visualisaties: Segmentatie & KPI’s
# ---------------------------
# Treemap voor klantsegmentatie
st.subheader("🗺️ Klantsegmentatie-overzicht (Treemap)")

df_treemap = (
    df.groupby(["label", "klant"])
    .agg({
        "omzet": "sum",
        "rendabiliteit": "mean"
    })
    .reset_index()
)

# Force visibility van categorieën, inclusief lage omzetklanten
df_treemap["omzet"] = df_treemap["omzet"].apply(lambda x: x if x > 0 else 1)

# Voeg alsnog placeholder toe als een label ontbreekt
labels_in_df = df_treemap["label"].unique()
for lbl in ["❌ Slecht", "⚖️ Gemiddeld", "💎 optimaal"]:
    if lbl not in labels_in_df:
        df_treemap = pd.concat([
            df_treemap,
            pd.DataFrame([{
                "label": lbl,
                "klant": f"⚠️ Geen actieve klanten ({lbl})",
                "omzet": 1,
                "rendabiliteit": -50 if lbl == "❌ Slecht" else 0
            }])
        ], ignore_index=True)

fig = px.treemap(
    df_treemap,
    path=["label", "klant"],
    values="omzet",
    color="rendabiliteit",
    color_continuous_scale="blues",
    range_color=[df["rendabiliteit"].min(), df["rendabiliteit"].max()],
    hover_data=["omzet", "rendabiliteit"],
    template="plotly_dark"
)
fig.update_layout(
    title="Klantsegmentatie op basis van omzet & rendabiliteit",
    paper_bgcolor='#111827',
    plot_bgcolor='#111827',
    font=dict(color='white'),
    title_font=dict(color='white'),
    legend=dict(font=dict(color='white')),
    coloraxis_colorbar=dict(title_font=dict(color='white'), tickfont=dict(color='white')),
    margin=dict(t=30, l=0, r=0, b=0)
)
fig.update_traces(marker=dict(line=dict(width=0)))  # Verwijder witte kaders
st.plotly_chart(fig, use_container_width=True)

# Bar chart als aanvulling
st.subheader("📊 Klantsegmentatie per klasse")

fig_bar = px.bar(
    df.groupby(["label", "klant"]).sum(numeric_only=True).reset_index(),
    x="klant",
    y="omzet",
    color="label",
    title="Omzet per klant gegroepeerd per klasse",
    template="plotly_dark"
)
fig_bar.update_layout(
    xaxis_title="Klant",
    yaxis_title="Omzet",
    paper_bgcolor='#111827',
    plot_bgcolor='#111827',
    font=dict(color='white'),
    title_font=dict(color='white'),
    legend=dict(font=dict(color='white')),
    xaxis_tickangle=-45
)

st.plotly_chart(fig_bar, use_container_width=True)


# KPI Badge Cards
st.subheader("🏅 KPI Highlights")

top3_winst = df.groupby("klant")["winst"].sum().sort_values(ascending=False).head(3)
bottom3_winst = df.groupby("klant")["winst"].sum().sort_values().head(3)
top3_rendabiliteit = df.groupby("klant")["rendabiliteit"].mean().sort_values(ascending=False).head(3)
bottom3_rendabiliteit = df.groupby("klant")["rendabiliteit"].mean().sort_values().head(3)


# Nieuwe 2x2 KPI-layout
kpi_row1_col1, kpi_row1_col2 = st.columns(2)
kpi_row2_col1, kpi_row2_col2 = st.columns(2)

with kpi_row1_col1:
    st.markdown("### 🥇 Top 3 klanten op winst")
    for klant, winst in top3_winst.items():
        st.markdown(card(klant, f"€ {winst:.2f}"), unsafe_allow_html=True)

with kpi_row1_col2:
    st.markdown("### 🔻 Klanten met laagste winst")
    for klant, winst in bottom3_winst.items():
        st.markdown(card(klant, f"€ {winst:.2f}"), unsafe_allow_html=True)

with kpi_row2_col1:
    st.markdown("### 💹 Top 3 klanten op rendabiliteit")
    for klant, rend in top3_rendabiliteit.items():
        st.markdown(card(klant, f"€ {rend:.2f}"), unsafe_allow_html=True)

with kpi_row2_col2:
    st.markdown("### 📉 Klanten met laagste rendabiliteit")
    for klant, rend in bottom3_rendabiliteit.items():
        st.markdown(card(klant, f"€ {rend:.2f}"), unsafe_allow_html=True)



# ---------------------------
# 📉 Betrouwbaarheid per klant (volatiliteit)
# ---------------------------
st.subheader("📉 Betrouwbaarheid per klant (volatiliteit in rendabiliteit)")

df_volatility = df.groupby("klant")["rendabiliteit"].std().reset_index()
df_volatility.columns = ["klant", "rendabiliteit_std"]
df_volatility = df_volatility.sort_values("rendabiliteit_std")

fig_volatility = px.bar(
    df_volatility,
    x="klant",
    y="rendabiliteit_std",
    title="Standaarddeviatie in rendabiliteit per klant (lager = betrouwbaarder)",
    template="plotly_dark"
)
fig_volatility.update_layout(
    xaxis_title="Klant",
    yaxis_title="Standaarddeviatie rendabiliteit",
    paper_bgcolor='#111827',
    plot_bgcolor='#111827',
    font=dict(color='white'),
    title_font=dict(color='white'),
    xaxis_tickangle=-45
)

st.plotly_chart(fig_volatility, use_container_width=True)


# ROI-kaarten

df_roi = df.groupby("klant").agg({"winst": "sum", "uren": "sum"})
df_roi["roi"] = (df_roi["winst"] / (df_roi["uren"] * kostprijs_per_uur)) * 100
gem_roi = df_roi["roi"].mean()

top2_roi = df_roi.sort_values("roi", ascending=False).head(2)
bottom2_roi = df_roi.sort_values("roi").head(2)

def roi_card(klant, waarde):
    delta = waarde - gem_roi
    kleur = "green" if delta >= 0 else "red"
    symbool = "▲" if delta >= 0 else "▼"
    return f"""
    <div style='background-color:#1F2937; padding:1rem; margin:0.5rem 0; border-radius:8px;'>
        <div style='color:white; font-weight:bold; font-size:1.1rem;'>{klant}</div>
        <div style='color:white; font-size:1.8rem; font-weight:bold;'>{waarde:.1f}%</div>
        <div style='color:{kleur}; font-size:1rem;'>{symbool} {abs(delta):.1f}% t.o.v. gem.</div>
    </div>
    """

col_roi1, col_roi2 = st.columns(2)

with col_roi1:
    st.markdown("### 🟩 Klanten met hoogste ROI")
    for klant, row in top2_roi.iterrows():
        st.markdown(roi_card(klant, row["roi"]), unsafe_allow_html=True)

with col_roi2:
    st.markdown("### 🟥 Klanten met laagste ROI")
    for klant, row in bottom2_roi.iterrows():
        st.markdown(roi_card(klant, row["roi"]), unsafe_allow_html=True)


# ROI per klant plot data (moved up so it is available below)
df_roi_plot = df.groupby("klant").agg({
    "winst": "sum",
    "uren": "sum",
    "omzet": "sum"
}).reset_index()

df_roi_plot["roi"] = (df_roi_plot["winst"] / (df_roi_plot["uren"] * kostprijs_per_uur)) * 100

# Uitklapbare tabel met negatieve ROI-klanten
with st.expander("📉 Klanten met negatieve ROI"):
    negatieve_roi_df = df_roi_plot[df_roi_plot["roi"] < 0].sort_values("roi")
    if negatieve_roi_df.empty:
        st.info("Er zijn op dit moment geen klanten met een negatieve ROI.")
    else:
        st.dataframe(negatieve_roi_df[["klant", "omzet", "uren", "winst", "roi"]])


top3_omzet = df.groupby("klant")["omzet"].sum().sort_values(ascending=False).head(3)

# (optioneel: top3_omzet kan elders getoond worden)



# ---------------------------
# 📐 ROI per klant: inzicht in rendement
# ---------------------------
st.subheader("📐 ROI per klant: inzicht in rendement")

fig_roi = px.scatter(
    df_roi_plot,
    x="omzet",
    y="roi",
    text="klant",
    color="roi",
    color_continuous_scale="Viridis",
    template="plotly_dark"
)
fig_roi.update_traces(
    text=None,
    hovertemplate="<b>%{customdata[0]}</b><br>Omzet: %{x}<br>ROI: %{y:.1f}%<extra></extra>",
    customdata=df_roi_plot[["klant"]].values
)
fig_roi.update_layout(
    xaxis_title="Totale omzet",
    yaxis_title="ROI (%)",
    paper_bgcolor='#111827',
    plot_bgcolor='#111827',
    font=dict(color='white'),
    coloraxis_colorbar=dict(
        title=dict(text="ROI", font=dict(color="white")),
        tickfont=dict(color="white")
    )
)

st.plotly_chart(fig_roi, use_container_width=True)

st.markdown(
    "<hr style='margin-top: 3rem; margin-bottom: 1rem; border-color: #374151;'>"
    "<div style='text-align: center; color: gray;'>© 2025 Dunion Online Marketing – Dashboard door Jeff Kroon</div>",
    unsafe_allow_html=True
)
