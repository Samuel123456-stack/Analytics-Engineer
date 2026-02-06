import requests
import json
from pprint import pprint
import streamlit as st

AGENT_ID = 'agente_pdf'
ENDPOINT = f'http://localhost:7777//agents/{AGENT_ID}/runs'

def get_response_stream(message:str):
    response = requests.post(
        url=ENDPOINT,
        data={
            'message': message,
            'stream': 'true'
        },
        stream=True
    )

    for line in response.iter_lines():
        if line:
            if line.startswith(b'data: '): # Remove o prefixo 'data: '
                data = line[6:]
                try:
                    event = json.loads(data)
                    yield event
                except json.JSONDecodeError:
                    continue

# Streamlit -----------------------------------------------
st.set_page_config(page_title='Page Chat PDF')
st.title('Agent Chat PDF')

# Histórico -----------------------------------------------
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Mostra histórico ----------------------------------------
for msg in st.session_state.messages:
    with st.chat_message(msg['role']):
        if msg['role'] == 'assistant' and msg.get('process'):
            with st.expander(label='Process', expanded=False):
                st.json(msg['process'])
        st.markdown(msg['content'])

# Input do usuário
if prompt := st.chat_input('Digite sua mensagem: '):
    # Adicionar mensagem do usuário (memória do streamlit)
    st.session_state.messages.append({
        'role': 'user',
        'content': prompt
    })
    with st.chat_message('user'):
        st.markdown(prompt)

    # Resposta do assistente
    with st.chat_message('assistant'):
        response_placeholder = st.empty()
        full_response = ''

    # processando streaming
    for event in get_response_stream(prompt):
        event_type = event.get('event', '')

        if event_type == 'ToolCallStarted':
            tool_name = event.get('tool', {}).get('tool_name')
            with st.status(f'Executando {tool_name}...', expanded=True):
                st.json(event.get('tool', {}).get('tool_args', {}))

        # Conteúdo da resposta
        elif event_type == 'RunContent':
            content = event.get('content', '')
            if content:
                full_response += content
                response_placeholder.markdown(full_response + '|')

    response_placeholder.markdown(full_response)

    # salvar a resposta e histórico na session_state
    st.session_state.messages.append({
        'role': 'assistant',
        'content': full_response
    })