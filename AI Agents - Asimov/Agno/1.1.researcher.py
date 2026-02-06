from agno.agent import Agent
from agno.tools.tavily import TavilyTools
from agno.models.groq import Groq
from dotenv import load_dotenv
import os

load_dotenv()  # carrega as variáveis do .env

api_key = os.getenv("TAVILY_API_KEY")
print(api_key)  # só para testar se está lendo


agent = Agent(
    model=Groq(id='llama-3.3-70b-versatile'),
    tools=[TavilyTools()] # O Tavily faz a pesquisa na Internet e devolve para o modelo
)

agent.print_response('Use suas ferramentas para pesquisar a temperatura de hoje em Porto Alegre')