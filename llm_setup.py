import os
from dotenv import load_dotenv
load_dotenv()
from langchain_openai import ChatOpenAI

gpt4 = ChatOpenAI(
    model="gpt-4o",
    temperature=0.2
)

gpt3_5 = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0.2
) 