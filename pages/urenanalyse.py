import streamlit as st
import pandas as pd
import plotly.express as px

st.logo("images/dunion-logo-def_donker-06.png")
st.set_page_config(
    page_title="Werkanalyse",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)
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
        1050, 1075, 1100,    # J
        1600, 1580, 1620,    # K
        900, 950, 925,       # L
        1250, 1300, 1275,    # M
        2000, 2050, 2100,    # N
        1150, 1200, 1175,    # O
        1300, 1350, 1325,    # P
        800, 850, 825,       # Q
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

# --- Nieuwe kolommen toevoegen aan df en df_extra ---
import numpy as np

# Fictieve werknemers en taken
werknemers = ["Sophie", "Mark", "Fatima", "Jan", "Lisa"]
taken = ["SEO", "SEA", "Content", "Webdesign", "Strategie", "Social Media"]
taak_tarief = {
    "SEO": 90,
    "SEA": 95,
    "Content": 85,
    "Webdesign": 100,
    "Strategie": 120,
    "Social Media": 80
}

def genereer_taakrijen(df_orig):
    # Maak kopie
    df_new = df_orig.copy()
    werknemer_lijst = []
    taak_lijst = []
    taak_uren_lijst = []
    taak_kosten_lijst = []
    rng = np.random.default_rng(seed=42)
    for idx, row in df_new.iterrows():
        # Kies werknemer en taak
        werknemer = rng.choice(werknemers)
        taak = rng.choice(taken)
        # Taakuren tussen 30% en 70% van totaal, niet meer dan totaal
        perc = rng.uniform(0.3, 0.7)
        taak_uren = round(min(row["uren"], row["uren"] * perc), 2)
        tarief = taak_tarief[taak]
        taak_kosten = round(taak_uren * tarief, 2)
        werknemer_lijst.append(werknemer)
        taak_lijst.append(taak)
        taak_uren_lijst.append(taak_uren)
        taak_kosten_lijst.append(taak_kosten)
    df_new["werknemer"] = werknemer_lijst
    df_new["taak"] = taak_lijst
    df_new["taak_uren"] = taak_uren_lijst
    df_new["taak_kosten"] = taak_kosten_lijst
    return df_new

# Pas toe op beide datasets
df = genereer_taakrijen(df)
df_extra = genereer_taakrijen(df_extra)

# ====== UI & Visualisaties ======

# Combineer eventueel beide datasets voor meer data
df_all = pd.concat([df, df_extra], ignore_index=True)

# Titel
st.title("Urenbesteding per Klant – Dunion Online Marketing")

# KPI's
totaal_uren = df_all["taak_uren"].sum()
totaal_kosten = df_all["taak_kosten"].sum()
gemiddeld_uurtarief = round(totaal_kosten / totaal_uren, 2) if totaal_uren > 0 else 0
aantal_taken = df_all.shape[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Totale uren", f"{totaal_uren:.2f}")
col2.metric("Totale kosten", f"€ {totaal_kosten:,.2f}".replace(",", "."))
col3.metric("Gemiddeld uurtarief", f"€ {gemiddeld_uurtarief:.2f}")
col4.metric("Aantal taken", aantal_taken)

# --- Filters ---
st.subheader("Filters")
filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
klanten = sorted(df_all["klant"].unique())
werknemers_lijst = sorted(df_all["werknemer"].unique())
taken_lijst = sorted(df_all["taak"].unique())
maanden = sorted(df_all["maand"].unique())

with filter_col1:
    klant_selectie = st.selectbox("Klant", options=["Alle"] + klanten, index=0)
with filter_col2:
    werknemer_selectie = st.selectbox("Werknemer", options=["Alle"] + werknemers_lijst, index=0)
with filter_col3:
    taak_selectie = st.selectbox("Taak", options=["Alle"] + taken_lijst, index=0)
with filter_col4:
    datum_range = st.select_slider("Maand", options=maanden, value=(maanden[0], maanden[-1]))

# --- Filter data op basis van selectie ---
df_filtered = df_all.copy()
if klant_selectie != "Alle":
    df_filtered = df_filtered[df_filtered["klant"] == klant_selectie]
if werknemer_selectie != "Alle":
    df_filtered = df_filtered[df_filtered["werknemer"] == werknemer_selectie]
if taak_selectie != "Alle":
    df_filtered = df_filtered[df_filtered["taak"] == taak_selectie]
if datum_range:
    start_maand, end_maand = datum_range
    df_filtered = df_filtered[(df_filtered["maand"] >= start_maand) & (df_filtered["maand"] <= end_maand)]

# --- Visualisaties ---
st.subheader("Taakuren per Taak")
fig1 = px.bar(
    df_filtered.groupby("taak", as_index=False)["taak_uren"].sum(),
    x="taak", y="taak_uren",
    labels={"taak_uren": "Uren", "taak": "Taak"},
    title="Uren per Taak"
)
st.plotly_chart(fig1, use_container_width=True)

st.subheader("Kosten per Taak")
fig2 = px.bar(
    df_filtered.groupby("taak", as_index=False)["taak_kosten"].sum(),
    x="taak", y="taak_kosten",
    labels={"taak_kosten": "Kosten (€)", "taak": "Taak"},
    title="Kosten per Taak"
)
st.plotly_chart(fig2, use_container_width=True)

st.subheader("Taakuren per Werknemer (per Taak)")
fig3 = px.bar(
    df_filtered.groupby(["werknemer", "taak"], as_index=False)["taak_uren"].sum(),
    x="werknemer", y="taak_uren", color="taak", barmode="group",
    labels={"taak_uren": "Uren", "werknemer": "Werknemer", "taak": "Taak"},
    title="Uren per Werknemer per Taak"
)
st.plotly_chart(fig3, use_container_width=True)

st.subheader("Relatie Uren vs Kosten per Taak")
df_scatter = df_filtered.groupby("taak", as_index=False).agg({
    "taak_uren": "sum",
    "taak_kosten": "sum"
})
fig4 = px.scatter(
    df_scatter,
    x="taak_uren", y="taak_kosten", text="taak",
    labels={"taak_uren": "Uren", "taak_kosten": "Kosten (€)"},
    title="Uren vs Kosten per Taak"
)
fig4.update_traces(textposition='top center')
st.plotly_chart(fig4, use_container_width=True)

# === Toevoeging: Urenverdeling per Klant per Taak ===
st.subheader("📊 Urenverdeling per Klant per Taak")
df_klant_taak = df_filtered.groupby(["klant", "taak"], as_index=False)["taak_uren"].sum()
fig_klant_taak = px.bar(
    df_klant_taak,
    x="klant",
    y="taak_uren",
    color="taak",
    barmode="stack",
    title="Aantal Uren per Klant (opgedeeld per Taak)",
    labels={"taak_uren": "Uren", "klant": "Klant", "taak": "Taak"}
)
st.plotly_chart(fig_klant_taak, use_container_width=True)

# === Toevoeging: Tabeloverzicht per klant ===
st.subheader("📋 Tabeloverzicht: Uren en Kosten per Klant per Taak")
overzicht_tabel = df_filtered.groupby(["klant", "taak"]).agg({
    "taak_uren": "sum",
    "taak_kosten": "sum"
}).reset_index()
st.dataframe(
    overzicht_tabel.style.format({
        "taak_uren": "{:.2f}",
        "taak_kosten": "€ {:.2f}"
    }),
    use_container_width=True
)

# Heatmap: werknemer vs maand, gevuld met taak_uren
st.subheader("Heatmap: Uren per Werknemer per Maand")
heatmap_data = df_filtered.groupby(["werknemer", "maand"], as_index=False)["taak_uren"].sum()
heatmap_pivot = heatmap_data.pivot(index="werknemer", columns="maand", values="taak_uren").fillna(0)
fig5 = px.imshow(
    heatmap_pivot,
    labels=dict(x="Maand", y="Werknemer", color="Uren"),
    x=heatmap_pivot.columns,
    y=heatmap_pivot.index,
    color_continuous_scale="Blues",
    aspect="auto"
)
fig5.update_layout(title="Uren per Werknemer per Maand", xaxis_title="Maand", yaxis_title="Werknemer")
st.plotly_chart(fig5, use_container_width=True)

st.markdown(
    "<div style='font-size: 0.8em; color: #888; text-align: right; margin-top: 1em;'>"
    "© 2025 Dunion Online Marketing – Analyse: Jeff Kroon"
    "</div>",
    unsafe_allow_html=True
)
