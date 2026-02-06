from agno.models.groq import Groq
from agno.agent import Agent
from agno.tools.yfinance import YFinanceTools #Importa o modelo que faz pesquisas sobre o mercado financeiro pelo Yahoo
from agno.db.sqlite import SqliteDb
from dotenv import load_dotenv
import os

load_dotenv()

db = SqliteDb(db_file="tmp/data.db") #Invocação do sqlite para armazenamento de conversas

agent = Agent(
    name='analista_financeiro',
    model=Groq(id='openai/gpt-oss-120b', api_key=os.getenv('GROQ_API_KEY')),
    tools=[YFinanceTools()], #Ferramenta de pesquisa que ele deve usar
    instructions='Você é um analista e tem diferentes clientes. Lembre-se de cada cliente, suas informações e preferências', # Passar instruções para formatar a saída da resposta
    add_history_to_context=True, #Persistir o histórico de conversa
    db=db,
    num_history_runs=3, # Número de conversas que ele lembre
    enable_user_memories=True, # Dá ao agente a habilidade de lembrar as preferências do usuário, o contexto e interações anteriores.
    add_memories_to_context=True
)

agent.print_response('Qual a cotação da petrobrás?', session_id='petrobras_session_2', user_id='analista_petrobras')
agent.print_response('Qual a cotação da vale?', session_id='vale_session_2', user_id='analista_vale')
#agent.print_response('Quais empresas já consultamos as cotações?', session_id='empresas_session', user_id='analista_empresas')