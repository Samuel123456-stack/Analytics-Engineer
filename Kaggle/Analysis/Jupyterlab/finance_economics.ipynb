import pandas as pd
import numpy as np
import seaborn as sns
from pandas.plotting import register_matplotlib_converters, scatter_matrix
register_matplotlib_converters()
import matplotlib.pyplot as plt
%matplotlib inline
import warnings
from datetime import datetime
#from sklearn.decomposition import PCA
#from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score, GridSearchCV
from sklearn.linear_model import LinearRegression
#from yellowbrick.regressor import ResidualsPlot
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.metrics import accuracy_score, root_mean_squared_error, mean_absolute_error


warnings.filterwarnings('ignore')
parse_date = lambda date: datetime.strptime(date, '%Y-%m-%d')
df = pd.read_csv('./csv files/finance_economics_dataset.csv', 
                 encoding='utf-8', 
                 na_values=False, 
                 verbose=0,
                 parse_dates=['Date'], 
                 index_col='Date', 
                 date_parser=parse_date
                )

#df = df.set_index(df.apply(lambda x: x['Date'])).dropna()
df.iloc[0:3]

attr_names = [cname for cname in df.select_dtypes(exclude=['object']).columns.to_numpy().ravel()]
df.describe()
if np.sum(len(df.index)) > 0:
    pass;
else:
    df = df.set_index(df.apply(lambda x: x['Date'])).dropna(inplace=True, axis=0)
df = df.dropna()

df['Trading Volume'].describe().apply(lambda x: format(x, 'f'))

stock_idx = np.random.permutation(df['Stock Index'].unique().transpose())
fig, ax = plt.subplots(figsize=(3, 4), layout='constrained')
bplot = sns.boxplot(x=df['Stock Index'], y=df['Open Price'], notch=True, label='Price', 
                    patch_artist=True, ax=ax, palette=['#FD625E', '#01B8AA', '#5E5EF2'],
                   width=0.6).set_title('Price Outliers by Stock', fontsize=10)
ax.set_xlabel('')
ax.set_xticklabels(labels=np.array(['Dow Jones', 'S&P 500', 'NASDAQ']), fontsize=9)
#for patch, color, in zip(bplot['boxes'], colors):
 #   patch.set_facecolor(color)
plt.show()

fig, axes = plt.subplots(nrows=5, ncols=4, figsize=(20, 15), layout='constrained')

for cname, ax in zip(attr_names, axes.flatten()):
    hist = sns.histplot(data=df, x=cname, ax=ax, kde=True, bins=20)
    hist.lines[0].set_color('red')

plt.show()

#----------------------------------------------------------------#
def split_train_test(file:pd.DataFrame, test_ratio=0.2):
    shuffle_index = np.random.permutation(int(len(file)))
    set_partition = int(len(df) * test_ratio)
    train_size = shuffle_index[set_partition:]
    test_size = shuffle_index[:set_partition]

    return file.iloc[train_size], file.iloc[test_size]
#----------------------------------------------------------------#

attr_corr = df[attr_names].corr()
fig, ax = plt.subplots(figsize=(12, 8), layout='constrained', sharex=True)
plot_heatmap = sns.heatmap(attr_corr, annot=True, fmt='.2f', cmap='coolwarm', ax=ax) \
                .set_title('Finance Correlation')

plt.show()

scatter_matrix(df[['Open Price', 'Close Price', 'Interest Rate (%)', 'Inflation Rate (%)']], figsize=(7, 7), 
               color='red')

plt.show()

#strat = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=0)
x = df.iloc[:, [6, 9]].values
y = df.iloc[:, 7].values

x_train, x_test, y_train, y_test = train_test_split(x, y, 
                                                    test_size=0.2, 
                                                    random_state=0)

x_train.shape, y_train.shape, x_test.shape, y_test.shape

lnr = LinearRegression(fit_intercept=True).fit(x_train, y_train)
y_pred = lnr.predict(x_test)
lnr.score(x_train, y_train)

from sklearn.tree import DecisionTreeRegressor
tree_reg = DecisionTreeRegressor(random_state=42).fit(x_train, y_train)
y_pred_tree_reg = tree_reg.predict(x_test)
y_pred_tree_reg[:10]

#---------------------------------------------
def display_score(score):
    print(f'Score: {score}')
    print(f'Mean: {score.mean()}')
    print(f'Deviation: {score.std()}')
#---------------------------------------------

print(f'RMSE: {root_mean_squared_error(y_test, y_pred_tree_reg)}')
score = cross_val_score(tree_reg, x_train, y_train, scoring='neg_mean_squared_error', cv=10)
rmse_tree_scores = np.sqrt(-score)
rmse_tree_scores
display_score(rmse_tree_scores)

from sklearn.ensemble import RandomForestRegressor
forest_reg = RandomForestRegressor(n_estimators=1).fit(x_train, y_train)
y_pred_forest_reg = forest_reg.predict(x_test)
y_pred_forest_reg[:10]
print(f'RMSE: {root_mean_squared_error(y_test, y_pred_forest_reg)}')

forest_score = cross_val_score(forest_reg, x_train, y_train, scoring='neg_mean_squared_error', cv=10)
rmse_forest_reg = np.sqrt(-forest_score)
display_score(rmse_forest_reg)


params_grid = {
    'Random Forest': {
        'n_estimators': [100, 200],
        'max_depth': [None, 10, 20]
    },
    'DecisionTree Regressor': {
        'criterion': ['absolute_error', 'squared_error'],
    }
}

#seed = np.random.seed(42)
#cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=seed)

models = {
    'Linear Regression': LinearRegression(fit_intercept=True),
    'DecisionTree Regressor': DecisionTreeRegressor(random_state=42),
    'Random Forest': RandomForestRegressor(random_state=42)
}

best_models = {}
for name, model in models.items():
    if name in params_grid:
        try:
            grid = GridSearchCV(model, params_grid[name], scoring='neg_mean_squared_error', cv=5,
                           n_jobs=-1, verbose=False)
            grid.fit(x_train, y_train)
            best_models[name] = grid.best_estimator_
            print(f'{name} best param: {grid.best_params_}')
        except Exception as e:
            print(f"Erro ao treinar {name}: {e}")
    else:
        model.fit(x_train, y_train)
        best_models[name] = model

cvres = grid.cv_results_
for mean_score, std_score, params in zip(cvres['mean_test_score'], cvres['std_test_score'], cvres['params']):
    print(f'mean_score: {np.sqrt(-mean_score)}, std_score: {std_score}, params: {params}')
