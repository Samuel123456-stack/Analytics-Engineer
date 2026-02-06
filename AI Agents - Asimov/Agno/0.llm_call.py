from agno.models.groq import Groq
from agno.models.message import Message
from dotenv import load_dotenv

load_dotenv()

model = Groq(id='llama-3.3-70b-versatile')
user_msg = Message(
    role='user',
    content=[
        {
            'type': 'text',
            'text': 'Olá, meu nome é Samuel.'
        }
    ]
)

assistant_msg = Message(
    role='assistant',
    content=''
)

response = model.invoke([user_msg], assistant_msg)
print(response.content)  # ou response.text