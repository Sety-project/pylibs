#!/usr/bin/env python3
import pandas as pd
import datetime
import sys
from histfeed.ftx_history import main

from scipy.interpolate import CubicSpline
from scipy.fft import fft, fftfreq
from sklearn.preprocessing import StandardScaler,FunctionTransformer
from sklearn.compose import ColumnTransformer,TransformedTargetRegressor
from sklearn.decomposition import PCA
from sklearn.pipeline import FeatureUnion
from sklearn.model_selection import TimeSeriesSplit,cross_val_score,cross_val_predict,train_test_split
from sklearn.linear_model import ElasticNet,LinearRegression,LassoLars

class LaplaceTransformer(FunctionTransformer):
    '''horizon_window in hours'''
    def __init__(self,horizon_window):
        super().__init__(lambda x: x.ewm(times = x.index,halflife=datetime.timedelta(hours=horizon_window)).mean())
        self._horizon_windows = horizon_window
    def get_feature_names_out(self):
        return [f'ewma{i}' for i in self._horizon_windows]
class ShiftTransformer(FunctionTransformer):
    def __init__(self,horizon_windows):
        super().__init__(func=
                         lambda x: pd.DataFrame({i: x.shift(periods=i)
                                                 for i in horizon_windows}),
                         inverse_func=
                         lambda x: pd.DataFrame({i: x.shift(periods=-i)
                                                 for i in horizon_windows}))
        self._horizon_windows = horizon_windows

    def get_feature_names_out(self):
        return [f'shift{i}' for i in self._horizon_windows]
class FwdMeanTransformer(TransformedTargetRegressor):
    def __init__(self,horizon_windows):
        super().__init__(lambda x: pd.DataFrame({i: x.shift(periods=-i-1).rolling(i).mean()
                                                 for i in horizon_windows}))
        self._horizon_windows = horizon_windows

    def get_feature_names_out(self):
        return [f'fwdmean{i}' for i in self._horizon_windows]

class ColumnNames:
    @staticmethod
    def funding(coin): return f'{coin}-PERP/rate/funding'
    @staticmethod
    def price(coin): return f'{coin}/price/c'
    @staticmethod
    def borrowOI(coin): return f'{coin}/rate/size'
    @staticmethod
    def volume(coin): return f'{coin}/price/volume'
    @staticmethod
    def borrow(coin): return f'{coin}/rate/borrow'

def ftx_forecaster_main(*args):
    coins = ['ETH','AAVE']
    features = ['funding','price']
    horizon_windows = [1, 2, 3, 4, 6, 8, 12, 18, 24, 36, 48, 60, 72, 84, 168]

    labels = lambda df: df['funding']-df['borrow'] # to apply to a single coin df
    holding_windows = [1,4,8,12,24,36,48]

    models = [LinearRegression]
    n_split = 7
    pca_n = None
    smoother = PCA(pca_n)

    # grab data
    data = main('get', coins, 'ftx', 1000)
    data = data[[getattr(ColumnNames,feature)(coin) for feature in features for coin in coins]]
    data.columns = pd.MultiIndex.from_product([coins,features],
                                             names=['coin','feature'])

    if 'price' in features:
        data.loc[:,(slice(None),'price')] = data.loc[:,(slice(None),'price')].diff() / data.loc[:,(slice(None),'price')]


    FeatureUnion([(f'ewma{horizon}',LaplaceTransformer(horizon_window=horizon)) for horizon in horizon_windows])
    # laplace transform
    laplace_transformer = LaplaceTransformer(horizon_windows)
    list_df =[]
    for feature in data.columns:
        laplace_expansion = laplace_transformer.fit_transform(data[feature]).dropna()
        laplace_expansion.columns = pd.MultiIndex.from_tuples([(*feature,c) for c in laplace_expansion.columns],names=data.columns.names+['mode'])
        list_df += [laplace_expansion]

    data = pd.concat(list_df,join='outer',axis=1)

if __name__ == "__main__":
    ftx_forecaster_main(*sys.argv[1:])