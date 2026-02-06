from agno.models.openai import OpenAIChat
from agno.agent import Agent
from agno.knowledge.embedder.google import GeminiEmbedder
from agno.db.sqlite import SqliteDb
from agno.vectordb.chroma import ChromaDb
from agno.knowledge.knowledge import Knowledge
from agno.knowledge.reader.pdf_reader import PDFReader

import os
from dotenv import load_dotenv, find_dotenv

import asyncio

load_dotenv(find_dotenv())

# RAG

vector_db = ChromaDb(
    collection='pdf_agent',
    path='tmp/chromadb',
    persistent_client=True,
    embedder=GeminiEmbedder(
        id='text-embedding-001',
        api_key=os.getenv('GOOGLE_API_KEY')
    )
)


knowledge = Knowledge(vector_db=vector_db)

db = SqliteDb(session_table='agent_session', db_file='tmp/agent.db')

agent = Agent(
    name='Agente PDF',
    model=OpenAIChat(id='gemini-2.0-flash-lite', api_key=os.getenv('GOOGLE_API_KEY')),
    instructions='Você deve chamar o usuário de senhor',
    db=db,
    knowledge=knowledge,
    enable_user_memories=True,
    add_knowledge_to_context=True,
    add_memories_to_context=True,
    num_history_runs=3,
    search_knowledge=True
)

# FastAPI ------------------------------------------------------

from fastapi import FastAPI
import uvicorn

app = FastAPI(title='Agente PDF', description='API para responde perguntas sobre o PDF')

@app.post('/agent_pdf')
def agent_pdf(question:str):
    return {'message': agent.run(question)}

# RUN ----------------------------------------------------------

if __name__ == '__main__':

    asyncio.run(
        knowledge.add_content_async(
            url='https://s3.sa-east-1.amazonaws.com/static.grendene.aatb.com.br/releases/2417_2T25.pdf',
            metadata={
                'source': 'Grendene',
                'type': 'pdf',
                'description': 'Relatório Trimestral'
            },
            skip_if_exists=True,
            reader=PDFReader()
        )
    )
    uvicorn.run('deploy_3:app', host='0.0.0.0', port=8000, reload=True)