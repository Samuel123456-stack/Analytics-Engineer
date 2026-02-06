from agno.models.groq import Groq
from agno.db.sqlite import SqliteDb

from agno.vectordb.chroma import ChromaDb
from agno.knowledge.knowledge import Knowledge
from agno.knowledge.reader.pdf_reader import PDFReader
from agno.knowledge.chunking.semantic import SemanticChunking
from agno.knowledge.embedder.openai import OpenAIEmbedder

import os
from dotenv import load_dotenv

load_dotenv()

db = SqliteDb(db_file='tmp/data.db')

# RAG
# Initialize ChromaDB
# Realiza a vetorização de textos e são armazenados no banco relacional (Chroma)

vector_db = ChromaDb(
    collection='empresas_relatorios',
    path='tmp/chromadb',
    embedder=OpenAIEmbedder(
        id='text-embedding-3-small',
        api_key=os.getenv('GROQ_API_KEY')
    ),
    persistent_client=True
)

# Create knowledge base
# Gerenciar o conhecimento com base nos dados do banco

knowledge = Knowledge(
    vector_db=vector_db
)

# Passa para 'knowledge' os arquivos dentro da pasta informada.
# Faz a leitura dos arquivos em PDF.
# E a estratégia de chunking é declarada.

knowledge.add_content(
    path='file/PETR/',
    reader=PDFReader(
        chunking_strategy=SemanticChunking()
    ),
    metadata={
        'company': 'Petrobras',
        'sector': 'Petróleo e Gás',
        'country': 'Brazil'
    },
    skip_if_exists=True
)

knowledge.add_content(
    path='file/VALE/',
    reader=PDFReader(
        chunking_strategy=SemanticChunking()
    ),
    metadata={
        'company': 'Vale',
        'sector': 'Mineração',
        'country': 'Brazil'
    },
    skip_if_exists=True
)

# AGENT #############################################

from agno.agent import Agent
from agno.tools.yfinance import YFinanceTools

agent = Agent(
    name='analista_financeiro',
    model=Groq(
        id='openai/gpt-oss-120b',
        api_key=os.getenv('GROQ_API_KEY')
    ),
    tools=[YFinanceTools],
    instructions='Você é um analista e tem diferentes clientes. Lembre-se de cada cliente, suas informações e preferências.',
    db=db,
    add_history_to_context=True,
    num_history_runs=3,
    enable_user_memories=True,
    add_memories_to_context=True,
    enable_agentic_memory=True,
    knowledge=knowledge,
    add_knowledge_to_context=True
)

agent.print_response(
    f'Olá, qual foi o lucro liquido da Petrobras em 2T25?',
    session_id='petrobras_session_4',
    user_id='analista_petrobras'
)

agent.print_response(
    f'Olá, qual foi o lucro liquido da Vale em 2T25?',
    session_id='vale_session_4',
    user_id='analista_vale'
)