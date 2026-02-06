# Conta corrente Bancária - FastAPI
# Gerenciar saques e depósitos de clientes

# IMPORTS-------------------------
from fastapi import FastAPI
from pydantic import BaseModel, Field #Responsável por manter a estrutura e validação dos dados de entrada
import uvicorn

# Inicializa o fastAPI

app = FastAPI(title='Conta Bancária - Conta Corrente')

# Adicionar clientes
db_customers = {
    'João': 0,
    'Maria': 0,
    'Pedro': 0
}

#---------------------------------------------

# Criar um endpoint para consultar o saldo
# Criar um endpoint para realizar saques
# Criar um endpoit para realizar depósitos

#---------------------------------------------

class Movimentacao(BaseModel):
    customer : str = Field(..., description='Nome do cliente')
    value : float = Field(..., gt=0, description='Valor da movimentação')

@app.get('/')
def read_root():
    return {'message': 'Conta Bancária - Conta Corrente'}

@app.post('/amount')
def amount(customer:str):
    return {'message': f'Saldo de {customer}: {db_customers[customer]}'}

@app.post('/withdraw')
def withDraw(movimentacao: Movimentacao):
    db_customers[movimentacao.customer] -= movimentacao.value
    return {
        'message': f'Saque de {movimentacao.customer}: {db_customers[movimentacao.customer]}'
    }



if __name__ == '__main__':
    uvicorn.run('deploy_2:app', host='0.0.0.0', port=8000, reload=True)