import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

st.logo("images/dunion-logo-def_donker-06.png")
st.set_page_config(
    page_title="Dunion KPI-dashboard",
    page_icon="images/dunion-logo-def_donker-06.png",
    layout="wide",
    initial_sidebar_state="expanded")

# --- Custom CSS for hover and fade-in ---
st.markdown("""
<style>
/* Hover effect voor KPI cards */
.css-1d391kg:hover {
    box-shadow: 0 8px 20px rgba(144, 202, 249, 0.8);
    transform: translateY(-4px);
    transition: all 0.3s ease;
}

/* Fade-in animatie voor de hele main container */
.main > div:first-child {
    animation: fadeIn 1s ease forwards;
}

@keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
}

/* Tooltip styling voor plotly */
.js-plotly-tooltip {
    background-color: #121212 !important;
    color: #E0E0E0 !important;
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif !important;
    font-size: 14px !important;
    border-radius: 8px !important;
    padding: 8px !important;
    box-shadow: 0 0 10px rgba(144, 202, 249, 0.7) !important;
}
</style>
""", unsafe_allow_html=True)

st.title("Dunion KPI Dashboard – Executive Overview")
st.markdown("Welkom bij het dashboard. Hieronder een snapshot van de belangrijkste bedrijfs-KPI's.")

# --- Simulatie data ---
np.random.seed(42)
klanten = [f"Klant {chr(i)}" for i in range(65, 91)]  # Klant A t/m Z
n_klanten = len(klanten)

# Simuleer ROI met focus op positief, maar met een paar negatieve uitschieters
roi_values = np.random.normal(loc=20, scale=15, size=n_klanten)
roi_values[roi_values < -10] = np.random.uniform(-30, -5, size=(roi_values < -10).sum())  # negatieve ROI voor enkelen

# Simuleer omzet (in tienduizenden)
omzet_values = np.random.normal(loc=200000, scale=100000, size=n_klanten)
omzet_values[omzet_values < 30000] = np.random.uniform(10000, 30000, size=(omzet_values < 30000).sum())

# Simuleer labels op basis van ROI en omzet thresholds
labels = []
for roi, omzet in zip(roi_values, omzet_values):
    if roi < 0:
        labels.append("❌ Slecht")
    elif roi < 15:
        labels.append("⚠️ Gemiddeld")
    else:
        labels.append("✅ Winstgevend")

df_summary = pd.DataFrame({
    "klant": klanten,
    "ROI": roi_values,
    "omzet": omzet_values,
    "label": labels
})

# KPI Cards: totale omzet, gemiddelde ROI, aantal winstgevende en negatieve ROI klanten
total_omzet = df_summary["omzet"].sum()
avg_roi = df_summary["ROI"].mean()
n_winstgevend = sum(df_summary["label"] == "✅ Winstgevend")
n_slecht = sum(df_summary["label"] == "❌ Slecht")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Totale Omzet", f"€{total_omzet:,.0f}", f"{np.random.uniform(1,10):.1f}% stijging")
col2.metric("Gemiddelde ROI", f"{avg_roi:.1f}%", f"{np.random.uniform(-5,5):+.1f}% t.o.v. vorig kwartaal")
col3.metric("Aantal winstgevende klanten", n_winstgevend, f"+{np.random.randint(0,3)} sinds vorige maand")
col4.metric("Aantal klanten met negatieve ROI", n_slecht, f"{np.random.randint(0,2)} nieuw")

st.markdown("---")

# Alerts op basis van slechte klanten en random teamload issues
st.subheader("🚨 Actuele Alerts")
alerts = []
if n_slecht > 0:
    slecht_klanten = df_summary[df_summary["label"] == "❌ Slecht"]["klant"].tolist()
    alerts.append(f"Klanten met negatieve ROI: {', '.join(slecht_klanten)}")
if np.random.rand() < 0.5:
    alerts.append(f"Teamlid Jansen werkt momenteel met 120% capaciteit!")

if alerts:
    for alert in alerts:
        st.warning(alert)
else:
    st.success("Geen kritieke alerts, alles loopt op rolletjes!")

st.markdown("---")

# Interactieve klantsegmentatie treemap
st.subheader("📊 Klantsegmentatie Overzicht")
fig = px.treemap(
    df_summary,
    path=["label", "klant"],
    values="omzet",
    color="ROI",
    color_continuous_scale=px.colors.sequential.Blues
)
fig.update_traces(
    hovertemplate=(
        "<b>%{label}</b><br>" +
        "Omzet: €%{value:,.0f}<br>" +
        "ROI: %{color:.1f}%<extra></extra>"
    )
)
fig.update_layout(
    margin=dict(t=50, l=25, r=25, b=25),
    paper_bgcolor='white',
    plot_bgcolor='white',
    font=dict(color='#E0E0E0'),
    transition={'duration': 500, 'easing': 'cubic-in-out'}
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# Top 5 beste klanten op basis van ROI
st.subheader("🏆 Top 5 Beste Klanten (ROI)")
top5 = df_summary.sort_values(by="ROI", ascending=False).head(5)
fig_top5 = px.bar(top5, x="klant", y="ROI", color="ROI", color_continuous_scale="blues", text="ROI")
fig_top5.update_layout(yaxis_title="ROI (%)", xaxis_title="Klant", paper_bgcolor='#111827', plot_bgcolor='#111827', font=dict(color='#E0E0E0'))
st.plotly_chart(fig_top5, use_container_width=True)

# Bottom 5 klanten met negatieve ROI
st.subheader("⚠️ Bottom 5 Klanten (Negatieve ROI)")
bottom5 = df_summary[df_summary["ROI"] < 0].sort_values(by="ROI").head(5)
fig_bottom5 = px.bar(bottom5, x="klant", y="ROI", color="ROI", color_continuous_scale="reds", text="ROI")
fig_bottom5.update_layout(yaxis_title="ROI (%)", xaxis_title="Klant", paper_bgcolor='#111827', plot_bgcolor='#111827', font=dict(color='#E0E0E0'))
st.plotly_chart(fig_bottom5, use_container_width=True)

st.markdown("---")

# Forecasting omzet voor de komende 6 maanden
st.subheader("🔮 Omzet Forecast (6 maanden)")
months = pd.date_range(start=pd.Timestamp.today(), periods=6, freq='M').strftime('%b %Y')
forecast_omzet = total_omzet * (1 + np.cumsum(np.random.normal(0.02, 0.01, 6)))
df_forecast = pd.DataFrame({"Maand": months, "Forecast Omzet": forecast_omzet})
fig_forecast = px.line(df_forecast, x="Maand", y="Forecast Omzet", markers=True)
fig_forecast.update_layout(yaxis_title="Omzet (€)", xaxis_title="Maand", paper_bgcolor='#111827', plot_bgcolor='#111827', font=dict(color='#E0E0E0'))
st.plotly_chart(fig_forecast, use_container_width=True)

st.markdown("---")

# Workload heatmap simulatie per teamlid en week
st.subheader("📅 Team Workload Heatmap")
teamleden = ["Jansen", "De Vries", "Meijer", "Smit", "Bakker"]
weken = [f"Wk {i}" for i in range(1, 13)]
workload = np.random.randint(40, 120, size=(len(teamleden), len(weken)))
df_workload = pd.DataFrame(workload, index=teamleden, columns=weken)
fig_heatmap = px.imshow(df_workload, color_continuous_scale='Viridis', labels=dict(x="Week", y="Teamlid", color="Workload (%)"))
fig_heatmap.update_layout(paper_bgcolor='#111827', plot_bgcolor='#111827', font=dict(color='#E0E0E0'))
st.plotly_chart(fig_heatmap, use_container_width=True)

st.markdown("---")

# AI Recommendations (simulated)
st.subheader("🤖 AI Aanbevelingen")
recommendations = [
    "Focus op klanten met gemiddelde ROI voor upsell kansen.",
    "Onderzoek oorzaken van negatieve ROI bij Klant E en Klant Q.",
    "Optimaliseer teamcapaciteit om overbelasting te voorkomen.",
    "Investeer in marketingcampagnes gericht op top 5 klanten.",
    "Plan kwartaalbijeenkomsten voor kennisdeling binnen het team."
]
for rec in recommendations:
    st.info(rec)

st.markdown("---")

# Footer
st.markdown("""
<footer style='text-align:center; padding:10px; color:#888;'>
    <hr>
    <p>© 2024 Dunion - KPI Dashboard. Alle rechten voorbehouden.</p>
</footer>
""", unsafe_allow_html=True)
