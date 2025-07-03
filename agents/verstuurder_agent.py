import requests
from crewai import Agent
from crewai.tools import BaseTool
from llm_setup import gpt3_5
from sendgrid.helpers.mail import Mail
import streamlit as st
import os


# n8n Webhook E-mail tool
class N8nWebhookEmailTool(BaseTool):
    def _run(self, subject: str, body: str, to: str):
        n8n_webhook_url = "https://jouw-n8n-server/webhook/rapport"  # <-- Vul hier jouw n8n-webhook-URL in
        n8n_token = os.environ.get("N8N_WEBHOOK_TOKEN")
        if not n8n_token:
            return "Fout: N8N_WEBHOOK_TOKEN niet gezet in omgevingsvariabelen."
        payload = {
            "subject": subject,
            "body": body,
            "to": to
        }
        headers = {"Authorization": f"Bearer {n8n_token}"}
        try:
            response = requests.post(n8n_webhook_url, json=payload, headers=headers)
            if response.status_code == 200:
                return f"Rapport succesvol doorgestuurd naar n8n (status: {response.status_code})"
            else:
                return f"Fout bij doorsturen naar n8n: status {response.status_code}, body: {response.text}"
        except Exception as e:
            return f"Fout bij doorsturen naar n8n: {e}"

n8n_email_tool = N8nWebhookEmailTool(
    name="n8n E-mail Tool",
    description="Stuurt een rapportage als JSON naar een n8n-webhook voor verdere verwerking (zoals e-mail via Outlook). Vereist een geldige N8N_WEBHOOK_TOKEN om als Bearer-token mee te sturen."
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