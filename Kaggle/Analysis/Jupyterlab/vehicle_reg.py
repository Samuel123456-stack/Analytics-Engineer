import pandas as pd
import numpy as np
#import seaborn as sns
#import matplotlib.pyplot as plt
#%matplotlib inline
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras import backend as k
import warnings
from datetime import datetime
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
#from sklearn.experimental import enable_iterative_imputer
#from sklearn.impute import IterativeImputer
from sklearn.model_selection import cross_val_score, GridSearchCV
import scikeras
from scikeras.wrappers import KerasRegressor

warnings.filterwarnings('ignore')
print(f'Data inicio: {datetime.now()}')
df = pd.read_csv('autos.csv', encoding='latin-1', delimiter=',')
df.drop(columns=['name','dateCrawled', 'dateCreated', 'lastSeen', 
                 'nrOfPictures', 'postalCode'], axis=1, inplace=True)

df_grp = df.groupby(['seller']).size()
df.drop(['seller'], axis=1, inplace=True)
df['offerType'].value_counts()
df.drop(['offerType'], axis=1, inplace=True)

df[df['price'] <= 10].sort_values('brand', ascending=False).iloc[0:7]
np.mean(df['price'])
df = df[(df['price'] > 10) & (df['price'] < 350000)]
df.where(df['vehicleType'].isna())

df.isnull().sum()

cat_cols = [cname for cname in df.select_dtypes(include=['object']).columns.values.ravel()
           if np.sum(df[cname].isna().sum()) > 0]

for column in np.array(cat_cols):
    if column=='vehicleType':
        df[column] = df[column].fillna(df[column].mode()[0])
    if column=='gearbox':
        df[column] = df[column].fillna(df[column].mode()[0])
    if column=='model':
        df[column] = df[column].fillna(df[column].mode()[0])
    if column=='fuelType':
        df[column] = df[column].fillna(df[column].mode()[0])
    if column=='notRepairedDamage':
        df[column] = df[column].fillna(df[column].mode()[0])

x = df.iloc[:, 1:12].values
y = df.iloc[:, 0].values

hot_enc = ColumnTransformer(transformers=[("OneHot", OneHotEncoder(), [0, 1, 3, 5, 8, 9, 10])], remainder='passthrough')
x = hot_enc.fit_transform(x).toarray()

def create_net(loss):
    k.clear_session()
    reg = Sequential([
        tf.keras.layers.InputLayer(shape=(x.shape[1],)),
        tf.keras.layers.Dense(units=158, activation='relu'),
        tf.keras.layers.Dense(units=158, activation='relu'),
        tf.keras.layers.Dense(units=1, activation='linear')
    ])
    reg.compile(optimizer='adam', loss=loss, metrics=['mean_absolute_error'])
    return reg

reg = KerasRegressor(model=create_net, batch_size=300, epochs=100)
params = {
    'model__loss': ['mean_squared_error', 'mean_absolute_error','mean_absolute_percentage_error', 
                    'mean_squared_logarithmic_error','squared_hinge']
}
grid_src = GridSearchCV(estimator=reg, param_grid=params, cv=2).fit(x, y)
grid_src.mean()
grid_src.std()
