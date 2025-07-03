import requests
from crewai import Agent
from crewai.tools import BaseTool
from llm_setup import gpt3_5
from sendgrid.helpers.mail import Mail
import streamlit as st
import os
import matplotlib.pyplot as plt
import io
import base64


# n8n Webhook E-mail tool
class N8nWebhookEmailTool(BaseTool):
    """
    Stuurt een rapportage als JSON naar een n8n-webhook voor verdere verwerking (zoals e-mail via Outlook).
    Vereist een geldige N8N_WEBHOOK_TOKEN (inclusief 'Bearer ...') als Bearer-token in de Authorization-header.
    Optioneel kun je attachments meesturen als een lijst van dicts:
    [
        {
            'filename': 'visualisatie.png',
            'content': <base64-string>,
            'type': 'image/png',
            'cid': 'visualisatie1'  # optioneel, voor inline gebruik
        },
        ...
    ]
    """
    def _run(self, subject: str, body: str, to: str, attachments=None):
        n8n_webhook_url = "https://dunion.app.n8n.cloud/webhook-test/Report_mail_2025"  # <-- Vul hier jouw n8n-webhook-URL in
        n8n_token = os.environ.get("N8N_WEBHOOK_TOKEN")
        if not n8n_token:
            return "Fout: N8N_WEBHOOK_TOKEN niet gezet in omgevingsvariabelen."
        payload = {
            "to": to,
            "subject": subject,
            "body": body
            
        }
        if attachments:
            payload["attachments"] = attachments
        headers = {"authorization": n8n_token}
        try:
            response = requests.post(n8n_webhook_url, json=payload, headers=headers)
            if response.status_code == 200:
                return f"Rapport succesvol doorgestuurd naar n8n (status: {response.status_code})"
            else:
                return f"Fout bij doorsturen naar n8n: status {response.status_code}, body: {response.text}"
        except Exception as e:
            return f"Fout bij doorsturen naar n8n: {e}"

def user_wil_visualisatie(prompt: str) -> bool:
    """Detecteert of de gebruiker om een visualisatie vraagt op basis van de prompt."""
    return any(word in prompt.lower() for word in ["grafiek", "visualisatie", "plot", "figuur"])

def generate_visualization_base64():
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    img_bytes = buf.read()
    img_b64 = base64.b64encode(img_bytes).decode('utf-8')
    plt.close(fig)
    return img_b64

def send_report_with_auto_visualization(prompt: str, to: str):
    html_table = "<table><tr><td>Voorbeeld</td></tr></table>"
    body = "<h2>Resultaten</h2>" + html_table
    attachments = None
    if user_wil_visualisatie(prompt):
        img_b64 = generate_visualization_base64()
        body += '<br><img src="cid:visualisatie1">'
        attachments = [{
            "filename": "visualisatie.png",
            "data": img_b64,
            "type": "image/png",
            "cid": "visualisatie1"
        }]
    result = n8n_email_tool._run(
        subject="Rapportage met optionele visualisatie",
        body=body,
        to=to,
        attachments=attachments
    )
    print(result)

n8n_email_tool = N8nWebhookEmailTool(
    name="n8n E-mail Tool",
    description="Stuurt een rapportage als JSON naar een n8n-webhook voor verdere verwerking (zoals e-mail via Outlook). Vereist een geldige N8N_WEBHOOK_TOKEN (inclusief 'Bearer ...') om als Authorization-header mee te sturen. Optioneel kun je attachments meesturen als een lijst van dicts met filename, data (base64), type, cid."
)

# Slack tool (mock)
class SlackTool(BaseTool):
    def _run(self, message: str, webhook_url: str):
        # In productie: requests.post(webhook_url, json={"text": message})
        print(f"Stuur Slack-bericht naar webhook {webhook_url} met tekst:\n{message}")
        return f"Slack-bericht verstuurd naar {webhook_url}"

slack_tool = SlackTool(
    name="Slack Tool",
    description="Stuurt een rapportage als bericht naar een Slack-kanaal via een webhook."
)

class StreamlitPublisherTool(BaseTool):
    def _run(self, report: str, section: str = "Rapportage"):
        # Je kunt hier eventueel meer Streamlit functionaliteit toevoegen
        st.markdown(f"### {section}")
        st.markdown(report)
        return f"Rapport gepubliceerd in Streamlit sectie: {section}"

streamlit_publisher_tool = StreamlitPublisherTool(
    name="Streamlit Publisher",
    description="Publiceert een rapportage direct op het Streamlit dashboard."
)

verstuurder_agent = Agent(
    role="Result Dispatcher",
    goal="Stuurt de gegenereerde rapportage door naar de juiste kanalen: n8n, dashboard of Slack.",
    backstory=(
        "Een betrouwbare digitale postbode. Zorgt dat de juiste inzichten op het juiste moment "
        "bij de juiste persoon terechtkomen. Werkt via n8n-webhook, API of frontend."
    ),
    tools=[n8n_email_tool, slack_tool, streamlit_publisher_tool],
    llm=gpt3_5
)

