import pandas as pd
import numpy as np
import tensorflow as tf
import warnings

import scikeras
from scikeras.wrappers import KerasClassifier
from sklearn.model_selection import cross_val_score #Validação Cruzada
from tensorflow.keras.models import Sequential #Cria a estrutura da rede neural (sequência de camadas)
from tensorflow.keras import backend as k

df_input = pd.read_csv('entradas_breast.csv', encoding='utf-8')
df_output = pd.read_csv('saidas_breast.csv', encoding='utf-8')
df = pd.concat([df_input, df_output], axis=1, ignore_index=False)
df.head()

df = df.rename(columns={df.columns[-1]: 'Class'})

def create_net():
    k.clear_session() #Limpa as sessões antes de criar a estrutura da rede neural
    neural_net = Sequential([
        tf.keras.layers.InputLayer(shape=(30,)), #Camada de entrada (atributos previsores)
        tf.keras.layers.Dense(units=16, activation='relu', kernel_initializer='random_uniform'), #Primeira camada oculta
        tf.keras.layers.Dropout(rate=0.2), #Dropout reduz o overfitting (rate = porcentagem de neurônios que serão removidos)
        tf.keras.layers.Dense(units=16, activation='relu', kernel_initializer='random_uniform'), #Segunda camada oculta
        tf.keras.layers.Dropout(rate=0.2), #Dropout reduz o overfitting (rate = porcentagem de neurônios que serão removidos)
        tf.keras.layers.Dense(units=1, activation='sigmoid') #Camada de saída (resultado)
    ])
    otimizador = tf.keras.optimizers.Adam(learning_rate=0.001, clipvalue=0.5)
    neural_net.compile(optimizer=otimizador, loss='binary_crossentropy', metrics=['binary_accuracy'])
    return neural_net

neural_net.summary()
neural_net = KerasClassifier(model=create_net, epochs=100, batch_size=10) #Faz o ajuste dos pesos de 10 em 10 registros
pred_cols = [cname for cname in df.select_dtypes(exclude=['int']).columns.values.ravel()]

x = df.loc[:, pred_cols]
y = df[['Class']]

confusion_matrix(y_test, y_pred)

#cv indica o número de folds divididos
outcome = cross_val_score(estimator=neural_net, X=x, y=y, cv=10, scoring='accuracy')
outcome.mean() #Acurácia média
outcome.std() #Acurácia média
