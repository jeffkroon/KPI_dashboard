# Main crew orchestration stub
# Import agents and define CrewAI crew and tasks here 

from crewai import Crew, Task, Process
from agents.analist_agent import analist_agent
from agents.consultant_agent import consultant_agent, forecaster_agent
from agents.rapporteur_agent import rapporteur_agent
from agents.redacteur_agent import redacteur_agent
from agents.verstuurder_agent import verstuurder_agent
from llm_setup import gpt4
from sql_tool import sql_agent_tool

# Define tasks for each agent
analist_task = Task(
    description="Voer een SQL-query uit op Supabase om de laatste KPI's per product op te halen.",
    expected_output="Een tabel met de meest recente KPI's per product.",
    agent=analist_agent,
)

consultant_task = Task(
    description="Analyseer de verschillen t.o.v. vorige periodes en detecteer afwijkingen of optimalisatiekansen op basis van de KPI-data.",
    expected_output="Een lijst met opvallende afwijkingen, trends en mogelijke optimalisaties.",
    agent=consultant_agent,
)

forecaster_task = Task(
    description="Voer een tijdreeksvoorspelling uit op basis van de geanalyseerde KPI-data en geef een verwachting voor de komende periode.",
    expected_output="Een voorspelling van de KPI's voor de komende periode, inclusief onzekerheidsmarges.",
    agent=forecaster_agent,
)

rapporteur_task = Task(
    description="Vat de conclusies en inzichten samen in duidelijke, zakelijke managementtaal.",
    expected_output="Een heldere, zakelijke samenvatting van de bevindingen.",
    agent=rapporteur_agent,
)

redacteur_task = Task(
    description="Redigeer de samenvatting tot een leesbare, professionele tekst in de tone of voice van Podobrace.",
    expected_output="Een foutloze, aansprekende en consistente rapportagetekst.",
    agent=redacteur_agent,
)

verstuurder_task = Task(
    description="Stuur de rapportage door naar de juiste kanalen: e-mail, dashboard of Slack.",
    expected_output="Bevestiging dat de rapportage succesvol is verzonden.",
    agent=verstuurder_agent,
)

def main():
    crew = Crew(
        agents=[
            analist_agent,
            consultant_agent,
            forecaster_agent,
            rapporteur_agent,
            redacteur_agent,
            verstuurder_agent
        ],
        tasks=[
            analist_task,
            consultant_task,
            forecaster_task,
            rapporteur_task,
            redacteur_task,
            verstuurder_task
        ],
        process=Process.sequential,
        verbose=True,
    )
    result = crew.kickoff()
    print("\nCrew result:", result)

if __name__ == "__main__":
    main() 