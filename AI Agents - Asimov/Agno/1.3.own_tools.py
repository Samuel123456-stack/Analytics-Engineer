from agno.agent import Agent
from agno.tools.tavily import TavilyTools
from agno.models.groq import Groq
from dotenv import load_dotenv
import os

load_dotenv()  # carrega as variáveis do .env

api_key = os.getenv("TAVILY_API_KEY")
print(api_key)  # só para testar se está lendo

def celsius_to_fh(celsius_temperature: float):
    """
    Convert a temperature from Celsius to Fahrenheit.
    
    Args:
        celsius_temperature (float): The temperature in Celsius to convert.
    
    Returns:
        float: The equivalent temperature in Fahrenheit.
    
    Example:
        >>> celsius_to_fh(0)
        32.0
        >>> celsius_to_fh(100)
        212.0
    """
    return (celsius_temperature * 9/5) + 32

agent = Agent(
    model=Groq(id='llama-3.3-70b-versatile'),
    tools=[
        TavilyTools(), # O Tavily faz a pesquisa na Internet e devolve para o modelo
        celsius_to_fh
    ],
    debug_mode=True 
)

agent.print_response('Use suas ferramentas para pesquisar a temperatura de hoje em Porto Alegre em Fahrenheit')