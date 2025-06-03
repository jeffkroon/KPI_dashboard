import streamlit as st
import pandas as pd

try:
    from prophet import Prophet
except ImportError:
    st.warning("Install `prophet` met `pip install prophet` om AI-forecasting mogelijk te maken.")

st.logo("images/dunion-logo-def_donker-06.png")
st.set_page_config(
    page_title="Dunion KPI-dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Forecasting")
st.markdown("Deze pagina bevat een forecasting prototype gebaseerd op gesimuleerde data. Gebruik de dropdown om de voorspelde omzet en ROI per klant te bekijken.")

import numpy as np
import plotly.express as px

np.random.seed(42)

maanden = pd.date_range(start="2024-01-01", periods=24, freq="MS")
klanten = ["Klant A", "Klant B", "Klant C", "Klant D", "Klant E"]

data = []

klantprofielen = {
    "Klant A": {"omzet_mu": 12000, "omzet_sigma": 2500, "uren_mu": 100, "uren_sigma": 15, "uurtarief": 90},
    "Klant B": {"omzet_mu": 9000, "omzet_sigma": 1500, "uren_mu": 120, "uren_sigma": 20, "uurtarief": 100},
    "Klant C": {"omzet_mu": 7000, "omzet_sigma": 1800, "uren_mu": 80, "uren_sigma": 10, "uurtarief": 110},
    "Klant D": {"omzet_mu": 15000, "omzet_sigma": 3000, "uren_mu": 140, "uren_sigma": 25, "uurtarief": 85},
    "Klant E": {"omzet_mu": 11000, "omzet_sigma": 2000, "uren_mu": 60, "uren_sigma": 8, "uurtarief": 120},
}

for klant, prof in klantprofielen.items():
    omzet = np.random.normal(loc=prof["omzet_mu"], scale=prof["omzet_sigma"], size=len(maanden))
    uren = np.random.normal(loc=prof["uren_mu"], scale=prof["uren_sigma"], size=len(maanden))
    kosten = uren * prof["uurtarief"]
    roi = (omzet - kosten) / kosten
    for i, maand in enumerate(maanden):
        data.append({
            "datum": maand,
            "klant": klant,
            "omzet": omzet[i],
            "uren": uren[i],
            "kosten": kosten[i],
            "roi": roi[i]
        })

df = pd.DataFrame(data)

voorspellingen = {}
for klant in klanten:
    df_temp = df[df["klant"] == klant][["datum", "omzet"]].rename(columns={"datum": "ds", "omzet": "y"})
    model = Prophet()
    model.add_seasonality(name='yearly', period=365.25, fourier_order=10)
    model.fit(df_temp)
    future = model.make_future_dataframe(periods=12, freq='MS')
    forecast = model.predict(future)
    forecast["klant"] = klant
    voorspellingen[klant] = forecast

st.subheader("📊 Voorspelling omzet en ROI per klant (gesimuleerd)")
klant_keuze = st.selectbox("Kies een klant:", klanten)
metric_keuze = st.selectbox("Kies een KPI:", ["Omzet", "ROI"])

forecast = voorspellingen[klant_keuze]
fig_forecast = px.line(forecast, x="ds", y="yhat", title=f"{metric_keuze} Forecast - {klant_keuze}")
fig_forecast.add_scatter(x=forecast["ds"], y=forecast["yhat_upper"], mode="lines", line=dict(width=0), name="Upper Bound", showlegend=False)
fig_forecast.add_scatter(x=forecast["ds"], y=forecast["yhat_lower"], mode="lines", fill='tonexty', line=dict(width=0), name="Lower Bound", showlegend=False)
st.plotly_chart(fig_forecast, use_container_width=True)

laatste_maand = forecast["ds"].max().strftime("%B %Y")
laatste_waarde = forecast[forecast["ds"] == forecast["ds"].max()]["yhat"].values[0]
groei_tov_nu = laatste_waarde - forecast[forecast["ds"] == forecast["ds"].min()]["yhat"].values[0]
st.markdown(f"📈 **Verwachte waarde in {laatste_maand}:** €{laatste_waarde:,.2f}  \n🔍 **Groei t.o.v. nu:** €{groei_tov_nu:,.2f}")


st.markdown(
    "De voorspelling wordt gedaan met behulp van Facebook Prophet, een krachtig time-series forecasting model. "
    "Het houdt rekening met trends, seizoensinvloeden en outliers. "
    "Dit model kan eenvoudig worden uitgebreid met regressoren zoals marketinguitgaven of teamcapaciteit in toekomstige versies."
)

# --- Break-even Analyse ---
st.subheader("📉 Break-even Analyse")

df_breakeven = df[df["klant"] == klant_keuze].copy()
df_breakeven["break_even"] = df_breakeven["kosten"]
df_breakeven["verlies_winst"] = df_breakeven["omzet"] - df_breakeven["kosten"]

fig_break = px.line(df_breakeven, x="datum", y=["omzet", "break_even"], title=f"Break-even Analyse - {klant_keuze}")
fig_break.update_traces(mode="lines+markers")
fig_break.update_layout(
    xaxis_title="Datum",
    yaxis_title="€ Waarde",
    legend_title="Metingen",
    plot_bgcolor="white",
    paper_bgcolor="white",
    font=dict(color="#111827"),
    xaxis=dict(showgrid=True),
    yaxis=dict(showgrid=True),
)
st.plotly_chart(fig_break, use_container_width=True)

laatste_verlies_winst = df_breakeven.iloc[-1]["verlies_winst"]
status = "winst" if laatste_verlies_winst > 0 else "verlies"
st.markdown(f"📌 **Laatste maand ({df_breakeven.iloc[-1]['datum'].strftime('%B %Y')}) eindigde met {status}:** €{laatste_verlies_winst:,.2f}")