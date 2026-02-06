from agno.models.openai import OpenAIChat
from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.vectordb.chroma import ChromaDb
from agno.knowledge.knowledge import Knowledge
from agno.knowledge.reader.pdf_reader import PDFReader
from agno.os import AgentOS

import os
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

# RAG

vector_db = ChromaDb(
    collection='pdf_agent',
    path='tmp/chromadb',
    persistent_client=True
)


knowledge = Knowledge(vector_db=vector_db)

db = SqliteDb(session_table='agent_session', db_file='tmp/agent.db')

agent = Agent(
    id='agente_pdf',
    name='Agente PDF',
    model=OpenAIChat(id='gpt-5-nano', api_key=os.getenv('OPENAI_API_KEY')),
    instructions='Você deve chamar o usuário de senhor',
    db=db,
    knowledge=knowledge,
    enable_user_memories=True,
    add_knowledge_to_context=True,
    add_memories_to_context=True,
    num_history_runs=3,
    search_knowledge=True
)

# AgentOS ------------------------------------------------------

agent_os = AgentOS(
    name='Agente de PDF',
    agents=[agent]
)

app = agent_os.get_app()

# RUN ----------------------------------------------------------

if __name__ == '__main__':

    knowledge.add_content(
            url='https://s3.sa-east-1.amazonaws.com/static.grendene.aatb.com.br/releases/2417_2T25.pdf',
            metadata={
                'source': 'Grendene',
                'type': 'pdf',
                'description': 'Relatório Trimestral'
            },
            skip_if_exists=True,
            reader=PDFReader()
        )
    
    agent_os.serve(app='my_os:app', reload=True)