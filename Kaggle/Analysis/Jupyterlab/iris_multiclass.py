import pandas as pd
import numpy as np
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from yellowbrick.classifier import ConfusionMatrix
from sklearn.metrics import accuracy_score
from sklearn.datasets import load_iris

import tensorflow as tf
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.models import Sequential
from tensorflow.keras import backend as k
from tensorflow.keras import utils as np_utils
from keras.models import model_from_json

from scipy import stats
import warnings
from datetime import datetime

# pip install scikeras
import scikeras
from scikeras.wrappers import KerasClassifier

# Ignora advertências e erros
warnings.filterwarnings('ignore')
print(f'Data inicio: {datetime.now()}')

# Carrega o dataset
iris = load_iris()
df = pd.DataFrame(iris.data, columns=iris.feature_names)
df['Class'] = iris.target

df.head()

# Descrição estatística
stats.describe(iris.data)

feature_cols = [cname.replace('(cm)', "").strip() 
                for cname in df.select_dtypes(exclude=['int']).columns.values.ravel()]

df = df.rename(columns={'sepal length (cm)': feature_cols[0], 'sepal width (cm)': feature_cols[1],
                       'petal length (cm)': feature_cols[2], 'petal width (cm)': feature_cols[3]})

class_map = {0: 'Iris-setosa', 1: 'Iris-versicolor', 2: 'Iris-virginica'}

df['target'] = df['Class'].map(class_map)
df.drop(columns=['Class'], axis=1, inplace=True)

x = df.iloc[:, 0:4].values
y = df[['target']]
lbl_enc = LabelEncoder()
y = lbl_enc.fit_transform(y)

y = np_utils.to_categorical(y)

# Divisão de treino e teste
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.25, random_state=0)

# Construção da rede neural
classifier = Sequential()
classifier.add(Dense(units=8, activation='relu', kernel_initializer='normal', input_dim=4))
classifier.add(Dropout(rate=0.2))
classifier.add(Dense(units=8, activation='relu', kernel_initializer='normal'))
classifier.add(Dropout(rate=0.2))
classifier.add(Dense(units=3, activation='softmax'))
    
classifier.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
classifier.fit(x_train, y_train, batch_size=10, epochs=100)   

# Salva o classificador
neural_net_json = classifier.to_json()
with open('classifier_iris.json', 'w') as json_file:
    json_file.write(neural_net_json)
classifier.save_weights('classifier_iris.weights.h5')   

# Carrega o classificador
file = open('classifier_iris.json', 'r')
struct_classifier = file.read()
file.close()
loaded_classifier = model_from_json(struct_classifier)
loaded_classifier.load_weights('classifier_iris.weights.h5')

accuracy_score(y_test, y_pred)
