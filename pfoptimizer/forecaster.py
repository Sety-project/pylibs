#!/usr/bin/env python3
import pandas as pd
import sys
import asyncio
from histfeed.ftx_history import ftx_history_main_wrapper

from scipy.interpolate import CubicSpline
from scipy.fft import fft, fftfreq
from sklearn.preprocessing import StandardScaler,FunctionTransformer
from sklearn.compose import ColumnTransformer,TransformedTargetRegressor
from sklearn.decomposition import PCA
from sklearn.pipeline import FeatureUnion,Pipeline
from sklearn.model_selection import TimeSeriesSplit,cross_val_score,cross_val_predict,train_test_split
from sklearn.linear_model import ElasticNet,LinearRegression,LassoCV

class LaplaceTransformer(FunctionTransformer):
    '''horizon_windows in hours'''
    def __init__(self,horizon_windows: list[int]):
        super().__init__(lambda x: [x.ewm(times = x.index,halflife=timedelta(hours=horizon_window)).mean()
                          for horizon_window in horizon_windows])
        self._horizon_windows = horizon_windows
    def get_feature_names_out(self):
        return [f'ewma{horizon_window}' for horizon_window in self._horizon_windows]
class ShiftTransformer(FunctionTransformer):
    def __init__(self,horizon_window):
        super().__init__(func=
                         lambda x: x.shift(periods=horizon_window),
                         inverse_func=
                         lambda x: x.shift(periods=-horizon_window))
        self._horizon_window = horizon_window
    def get_feature_names_out(self):
        return f'shift{self._horizon_window}'
class FwdMeanTransformer(TransformedTargetRegressor):
    def __init__(self,horizon_windows):
        super().__init__(lambda x: {horizon_window:x.shift(periods=-horizon_window-1).rolling(horizon_window).mean()
                                    for horizon_window in horizon_windows})
        self._horizon_windows = horizon_windows
    def get_feature_names_out(self):
        return [f'fwdmean{horizon_window}' for horizon_window in self._horizon_windows]

class ColumnNames:
    @staticmethod
    def funding(coin): return f'{coin}-PERP_rate_funding'
    @staticmethod
    def price(coin): return f'{coin}_price_c'
    @staticmethod
    def borrowOI(coin): return f'{coin}_rate_size'
    @staticmethod
    def volume(coin): return f'{coin}_price_volume'
    @staticmethod
    def borrow(coin): return f'{coin}_rate_borrow'

def ftx_forecaster_main(*args):
    coins = ['ETH','AAVE']
    features = ['funding','price', 'borrowOI', 'volume', 'borrow']
    label_func = lambda coin,data: data[getattr(ColumnNames,'funding')(coin)]-data[getattr(ColumnNames,'borrow')(coin)]
    horizon_windows = [1, 2, 3, 4, 6, 8, 12, 18, 24, 36, 48, 60, 72, 84, 168]
    holding_windows = [1,4,8,12,24,36,48]

    n_split = 7
    models = [LassoCV()]
    pca_n = None

    # grab data
    #main(['build', coins, 'ftx', 1000])
    data = asyncio.run(ftx_history_main_wrapper('ftx', 'get', 'wide', 1000))

    features_list = []
    labels_list = {holding_window:[] for holding_window in holding_windows}
    fitted_model_list = {holding_window:None for holding_window in holding_windows}
    for coin in coins:
        data_list = []
        for feature in features:
            feature_data = data[getattr(ColumnNames,feature)(coin)]

            if feature in ['funding','borrow']:
                standardizer = 'passthrough'
            else:
                if feature == 'price':
                    rel_diff = FunctionTransformer(func = lambda data: data.diff() / data)
                    standardizer = Pipeline([('rel_diff',rel_diff),
                                             ('standardizer', StandardScaler)])
                else:
                    standardizer = StandardScaler()

            laplace_expansion = LaplaceTransformer(horizon_windows)
            dimensionality_reduction = PCA(n_components=pca_n,svd_solver='full') if pca_n else 'passthrough'
            data_list += [Pipeline([('standardizer', standardizer),
                                    ('laplace_expansion', laplace_expansion),
                                    ('dimensionality_reduction', dimensionality_reduction)])
                              .fit_transform(feature_data)]

        coin_features = pd.concat(data_list,axis=1,join='inner')
        coin_labels = FwdMeanTransformer(holding_windows).fit_transform(label_func(coin,data))

        features_list += [coin_features]
        for holding_window in holding_windows:
            labels_list[holding_window] += [coin_labels[holding_window]]
            fitted_model_list[holding_window] |= {coin:
                                       models[0].fit(coin_features,coin_labels[holding_window])}

    features = pd.concat(features_list)
    labels = {holding_window: pd.concat(labels_list[holding_window])
              for holding_window in holding_windows}

    fitted_model = {holding_window: models[0].fit(features,labels[holding_window])
                    for holding_window in holding_windows}

if __name__ == "__main__":
    ftx_forecaster_main(*sys.argv[1:])