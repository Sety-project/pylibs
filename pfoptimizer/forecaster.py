#!/usr/bin/env python3
import pandas as pd
import sys
import asyncio
import datetime
from histfeed.ftx_history import ftx_history_main_wrapper
from utils.config_loader import configLoader

from scipy.interpolate import CubicSpline
from scipy.fft import fft, fftfreq
from sklearn.preprocessing import StandardScaler,FunctionTransformer
from sklearn.compose import ColumnTransformer,TransformedTargetRegressor
from sklearn.decomposition import PCA
from sklearn.pipeline import FeatureUnion,Pipeline
from sklearn.model_selection import TimeSeriesSplit,cross_val_score,cross_val_predict,train_test_split
from sklearn.linear_model import ElasticNet,LinearRegression,LassoCV

class ZscoreTransformer(StandardScaler):
    def get_feature_names_out(self):
        return 'zscore'
    def fit_transform(self, X, y=None, **fit_params):
        return pd.DataFrame(index=X.index,
                            data=super(StandardScaler,self).fit_transform(X.values.reshape(-1,1), y, **fit_params))
class LaplaceTransformer(FunctionTransformer):
    '''horizon_windows in hours'''
    def __init__(self,horizon_windows: list[int]):
        self._horizon_windows = horizon_windows
    def get_feature_names_out(self):
        return [f'ewma{horizon_window}' for horizon_window in self._horizon_windows]
    def fit_transform(self,X,y=None):
        return pd.DataFrame({horizon_window: X.ewm(times = X.index,halflife = datetime.timedelta(hours=horizon_window)).mean()
                          for horizon_window in self._horizon_windows}).dropna()
class ShiftTransformer(FunctionTransformer):
    def __init__(self,horizon_window):
        self._horizon_window = horizon_window
    def get_feature_names_out(self):
        return f'shift{self._horizon_window}'
    def fit_transform(self, X, y=None):
        return X.shift(periods=self._horizon_window)
class FwdMeanTransformer(TransformedTargetRegressor):
    def __init__(self,horizon_windows):
        self._horizon_windows = horizon_windows
    def get_feature_names_out(self):
        return [f'fwdmean{horizon_window}' for horizon_window in self._horizon_windows]
    def fit_transform(self,X,y=None):
        return pd.DataFrame({horizon_window: X.shift(periods=-horizon_window - 1).rolling(horizon_window).mean()
                      for horizon_window in self._horizon_windows})
class PCATransformer(PCA):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    def get_feature_names_out(self):
        return [f'pca{i}' for i in range(self.n_components)]
    def fit_transform(self,X,y=None):
        self.fit(X.values)
        result = self.transform(X.values)
        return pd.DataFrame(index=X.index,columns=[f'pca_{i}' for i in range(self.n_components_)],data=result)

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
    # Try loading the config
    try:
        universe = configLoader.get_bases('max')
    except FileNotFoundError as err:
        print(str(err))
        print("---> Terminating...")
        sys.exit(1)

    coins = ['ETH','AAVE']
    features = ['funding','price', 'borrowOI', 'volume', 'borrow']
    label_func = lambda coin,data: -data[getattr(ColumnNames,'funding')(coin)]-data[getattr(ColumnNames,'borrow')(coin)]
    horizon_windows = [1, 2, 3, 4, 6, 8, 12, 18, 24, 36, 48, 60, 72, 84, 168]
    holding_windows = [1,4,8,12,24,36,48]

    n_split = 7
    models = [LassoCV()]
    pca_n = .99

    # grab data
    #main(['build', coins, 'ftx', 1000])
    data = asyncio.run(ftx_history_main_wrapper('ftx', 'get', universe, 1000))

    features_list = []
    labels_list = {holding_window:[] for holding_window in holding_windows}
    fitted_model_list = {holding_window:None for holding_window in holding_windows}
    for coin in coins:
        data_list = []
        for feature in features:
            feature_data = data[getattr(ColumnNames,feature)(coin)]

            if feature in ['funding','borrow']:
                incrementer = 'passthrough'
                standardizer = 'passthrough'
            elif feature == 'price':
                incrementer = FunctionTransformer(func = lambda data: (data.diff() / data).dropna())
                standardizer = ZscoreTransformer()
            else:
                incrementer = 'passthrough'
                standardizer = ZscoreTransformer()

            laplace_expansion = LaplaceTransformer(horizon_windows)
            dimensionality_reduction = PCATransformer(n_components=pca_n,svd_solver='full') if pca_n else 'passthrough'
            data_list += [Pipeline([('incrementer', incrementer),
                                    ('standardizer', standardizer),
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