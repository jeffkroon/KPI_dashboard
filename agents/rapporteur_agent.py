from crewai import Agent
from crewai.tools import BaseTool
from llm_setup import gpt4
from jinja2 import Template

class Jinja2ReportTool(BaseTool):
    def _run(self, context: dict, template_str: str):
        template = Template(template_str)
        return template.render(**context)

jinja2_tool = Jinja2ReportTool(
    name="Jinja2 Rapportage Tool",
    description="Genereert een zakelijke rapportage op basis van een Jinja2-template en contextdata."
)

rapporteur_agent = Agent(
    role="KPI Reporter",
    goal="Vertaalt de conclusies en inzichten in duidelijke, zakelijke samenvattingen.",
    backstory=(
        "Een professionele rapporteur met gevoel voor managementtaal. "
        "Zorgt dat de cijfers en adviezen begrijpelijk worden gepresenteerd voor besluitvorming, "
        "zonder data-overload. Gebruikt templates en tekstuele consistentie."
    ),
    tools=[jinja2_tool],
    llm=gpt4
) 