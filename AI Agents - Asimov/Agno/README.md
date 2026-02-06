# __0.llm_call.py__: Exemplos simples de chamadas direta a um modelo de LLM via *Groq*, demonstrando como criar mensagens estruturas (`Message`) e obter uma resposta do modelo.

# __1.1.researcher.py__: Implementa um agente de pesquisa web utilizando *TavilyTools*, permitindo consultar informações atualizadas na internet. Demonstra o uso de ferramentas externas integradas a um agente LLM.

# __1.2.analyst.py__: Cria um *agente financeiro com memória persistente*, capaz de consultar dados de mercado via *Yahoo Finance* e manter histórico de interações em *SQLite*, simulando múltiplos clientes com contexto independente.

# __1.3.own_tools.py__: Exemplo de *criação e uso de ferramentas customizadas*, adicionando uma função própria (`celsius_to_fh`) ao agente, combinada com pesquisa web para realizar conversões automáticas de temperatura.

# __1.4.RAG.py__: Implementa um *pipeline completo de RAG (Retrieval-Augmented Generation)*:

# - Vetorização de PDFs com *ChromaDB*.
# - Indexação semântica de relatórios financeiros.
# - Consulta combinada entre dados financeiros em tempo real e base documental histórica

# __deploy.py__: Exemplo básico de *API FastAPI*, com endpoints simples para teste de funcionamento e deploy local via *Uvicorn (servidor web)*.

# __deploy_v2.py__: API FastAPI que simula uma *conta bancária*, permitindo:

# - Consulta de saldo.
# - Realização de saques
# - Controle básico de clientes em memória

# __deploy_v3.py__: API que disponibiliza um *agente RAG como serviço*, permitindo perguntas sobre um *PDF Remoto*, utilizando:

# - ChromaDB
# - OpenAI Embedder (O texto é convertido em vetor numérico)
# - FastAPI como camada de exposição

# __my_os.py__: Implementa um *AgentOS*, orquestrando agentes em sistema operacional de IA, expondo uma *API completa para consultas RAG*, com persistência de contexto e busca semântica

# __stream.py__: Este script implementa uma aplicação web baseada em *Streamlit* que oferece uma interface de chat conversacional com um agente de PDF. Ele se comunica com um endpoint de API local (`http://localhost:7777/agents/agente_pdf/runs`) para enviar mensagens do usuário e receber respostas em streaming, tratando atualizações em tempo real e eventos como chamadas de ferramentas

#