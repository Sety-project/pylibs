#!/usr/bin/env python3
import copy
import itertools
from datetime import *
from typing import NewType
import numpy as np
import pandas as pd
import os
import plotly.express as px
import sklearn.base
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, LassoLarsIC, RidgeCV, ElasticNetCV, LassoCV, LassoLarsCV, \
    LogisticRegressionCV
from sklearn.svm import SVR, SVC
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, AdaBoostClassifier
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import TimeSeriesSplit, cross_val_score, cross_val_predict, train_test_split

Instrument = NewType("Instrument", str)  # eg 'ETHUSDT'
RawFeature = NewType("RawFeature", str)  # eg 'volume'
FeatureExpansion = NewType("FeatureExpansion", int)  # frequency in min
Feature = NewType("Feature", tuple[Instrument, RawFeature, FeatureExpansion])
Data = NewType("Data", dict[Feature, pd.Series])

def remove_duplicate_rows(df):
    return df[~df.index.duplicated()]

def winning_trade(df: pd.Series,
                  lvl_scaling_window=72 * 60,
                  takeprofit=1.5,  # in stdev
                  stop=-0.25,
                  verbose=False) -> pd.Series:
    '''
    df are log returns not standardized
    returns:
    * 1  if long ended up takeprofit
    * -1 if short ended up takeprofit
    * 0  if both ended up stopped
    * note that both can't takeprofit if stop+takeprofit > 0
    '''
    result = pd.Series(index=df.index[lvl_scaling_window + 1:], name='winning_trade', data=0)
    rolling_std = df.rolling(lvl_scaling_window).sum().expanding(100).std().squeeze()
    cum_perf = df.cumsum().squeeze()
    for i in df.index[(lvl_scaling_window + 1):]:
        future = cum_perf[i:] - cum_perf[i]

        hitting_times = {name: datetime.now() if events.dropna().empty else events.dropna().index[0] for name, events in
                         {'long_stopped': future[future < stop],
                          'long_takeprofit': future[future > takeprofit],
                          'short_stopped': future[future > -stop],
                          'short_takeprofit': future[future < -takeprofit]}.items()}

        if hitting_times['long_takeprofit'] < hitting_times['long_stopped']: result[i] = 1
        if hitting_times['short_takeprofit'] < hitting_times['short_stopped']: result[i] = -1

    # test
    # if verbose:
    #     daterange = pd.date_range('start_date', periods=5000, freq="1min")
    #     trajectory = pd.DataFrame(index=daterange, data=np.random.randn(len(daterange)))
    #     winning_trades = winning_trade(trajectory)
    #     px.line(pd.concat([winning_trades, trajectory.cumsum()], axis=1))

    return result


class ResearchEngine:
    def __init__(self, feature_map, label_map, run_parameters, **kwargs):
        self.feature_map = feature_map
        self.label_map = label_map
        self.run_parameters = run_parameters
        self.data: Data = Data(dict())
        self.X_Y: np.ndarray = np.ndarray(2)
        self.X: np.ndarray = np.ndarray(2)
        self.Y: np.ndarray = np.ndarray(2)
        self.models: list[sklearn.base.BaseEstimator]
        self.cached_scaler: dict[(Instrument, str, int), StandardScaler] = dict()

    @staticmethod
    def read_history(dirname,
                     start_date,
                     selected_instruments: set[Instrument]) -> dict[tuple[Instrument, RawFeature], pd.Series]:
        raw_data: dict[Instrument, pd.DataFrame] = dict()
        for filename in os.listdir(dirname):
            filesplit = filename.split('-')
            instrument: Instrument = Instrument(filesplit[0])
            data_type: RawFeature = RawFeature(filesplit[-1].replace('.csv', ''))
            if instrument in selected_instruments:
                new_df = pd.read_csv(os.path.join(os.sep, dirname, filename))
                new_df['open_time'] = new_df['open_time'].apply(lambda t: datetime.fromtimestamp(t / 1000))
                new_df = new_df.set_index('open_time').sort_index(ascending=True)[start_date:]
                if instrument in raw_data and data_type in raw_data[instrument]:
                    raw_data[instrument][data_type] = pd.concat([raw_data[instrument][data_type], new_df])
                else:
                    if instrument in raw_data:
                        raw_data[instrument][data_type] = new_df
                    else:
                        raw_data[instrument] = {data_type: new_df}
        for instrument, data in raw_data.items():
            #
            raw_data[instrument] = data['klines'].join(data['premium'], rsuffix='_premium', how='outer').ffill()

        return raw_data

    def standard_scaler(self, df: pd.Series, column: tuple[Instrument, Feature, FeatureExpansion] = None) -> pd.Series:
        '''store the scaler for later (only for labels)'''
        scaler = StandardScaler().fit(pd.DataFrame(df).values)
        res = scaler.transform(pd.DataFrame(df).values)
        if column is not None:
            self.cached_scaler[column] = scaler
        return pd.Series(index=df.index,
                         name=df.name,
                         data=res[:, 0])

    def inverse_scaler(self, df: pd.Series, column: tuple[Instrument, Feature, FeatureExpansion]) -> pd.Series:
        scaler = self.cached_scaler[column]
        res = scaler.inverse_transform(pd.DataFrame(df).values)
        return pd.Series(index=df.index,
                         name=df.name,
                         data=res[:, 0])

    def compute_features(self,
                         raw_data: dict[tuple[Instrument, RawFeature], pd.Series],
                         feature_map: dict[Feature, dict]):
        for instrument, df in raw_data.items():
            for feature in feature_map:
                if feature == 'close':
                    # take log increment and rename
                    data = raw_data[instrument][feature]
                    result = (data / data.shift(1)).apply(np.log).dropna()
                elif feature == 'volume':
                    # take log
                    data = raw_data[instrument][feature]
                    result = data.apply(lambda x: np.log(max([x, 1e-32])))
                elif feature in ['taker_imbalance', 'open_interest']:
                    # take arctanh
                    result = (raw_data[instrument]['taker_buy_volume'] / raw_data[instrument]['volume']).apply(
                        lambda x: np.arctanh(np.clip((2 * x - 1), 1e-8 - 1, 1 - 1e-8)))
                elif feature == 'premium':
                    # as is
                    result = raw_data[instrument]['close_premium']
                elif feature == 'vw_premium':
                    # volume weighted increments
                    result = raw_data[instrument]['close_premium'] * raw_data[instrument]['volume']
                elif feature == 'vw_increment':
                    # volume weighted increments
                    data = raw_data[instrument]['close']
                    result = (data / data.shift(1)).apply(np.log).dropna() * raw_data[instrument]['volume']
                else:
                    raise NotImplementedError

                result = remove_duplicate_rows(result)
                result.dropna(inplace=True)
                result.rename((instrument, feature, 'live'), inplace=True)
                self.data[(instrument, feature, 'live')] = result

    def laplace_expand(self):
        original_data = copy.deepcopy(self.data)
        for (instrument, feature, _), data in original_data.items():
            for i in self.feature_map[feature]['ewma_windows']:
                self.data[(instrument, feature, f'ewma_{i}')] = \
                    self.data[(instrument, feature, 'live')].transform(
                        lambda x: x.ewm(times=x.index, halflife=timedelta(hours=i + 1)).mean()).dropna().rename(
                        (instrument, feature, f'ewma_{i}'))  # x.shift(periods=i))#

    def compute_labels(self, raw_data, label_map):
        result = dict()
        for instrument, label in itertools.product(raw_data.keys(), label_map.keys()):
            for horizon in label_map[label]['horizons']:
                if label == 'performance':
                    temp = np.log(1-raw_data[instrument]['close'].diff(-horizon) / raw_data[instrument]['close'])
                    if self.run_parameters['normalize']:
                        temp = self.standard_scaler(temp, (instrument, label, horizon))
                elif label == 'sign':
                    temp = (-raw_data[instrument]['close'].diff(-horizon)).apply(lambda x: 1 if x >= 0 else -1)
                elif label == 'big':
                    temp = -raw_data[instrument]['close'].diff(-horizon) > 1.3 * raw_data[instrument][
                        'close'].diff(-horizon).expanding(horizon + 1).std()
                elif label == 'stop_limit':
                    temp = winning_trade(raw_data[instrument]['close'])
                result[(instrument, label, horizon)] = remove_duplicate_rows(temp).rename(
                    (instrument, label, horizon))
        return result

    def build_X_Y(self, raw_data, feature_map, label_map):
        self.compute_features(raw_data, feature_map)
        self.laplace_expand()
        feature_dict = self.data
        if self.run_parameters['normalize']:
            feature_dict = {feature: self.standard_scaler(data) for feature, data in self.data.items()}

        label_dict = self.compute_labels(raw_data, label_map)
        self.data |= label_dict

        self.X_Y = pd.concat(self.data, axis=1).dropna()
        self.X = self.X_Y[feature_dict.keys()]
        self.Y = self.X_Y[label_dict.keys()]

        self.fitted_model: dict[tuple(Feature, FeatureExpansion), list[sklearn.base.BaseEstimator]] = dict()

    def run(self) -> pd.DataFrame():
        holding_windows = [window for windows in self.label_map.values() for window in windows['horizons']]
        result = pd.DataFrame()

        # for each (all instruments, 1 feature, 1 horizon)
        for (feature, frequency), label_df in self.Y.groupby(level=[1, 2], axis=1):
            self.fitted_model[(feature, frequency)] = list()
            # for each model for that feature
            for model in self.run_parameters['models'][feature]:
                tscv = TimeSeriesSplit(n_splits=int(np.log2(len(self.X_Y.index)/frequency/5)), gap=max(holding_windows)).split(self.X_Y)
                for split_index, (train_index, test_index) in enumerate(tscv):
                    # fit model on all instruments
                    X_allinstruments = self.X.iloc[train_index].stack(level=0)
                    Y_allinstruments = label_df.iloc[train_index].stack(level=0)
                    fitted_model = model.fit(X_allinstruments, Y_allinstruments.squeeze())
                    if callable(getattr(fitted_model, 'predict_proba')):
                        delta_strategy = getattr(fitted_model, 'predict_proba')
                    else:
                        delta_strategy = getattr(fitted_model, 'predict')
                    self.fitted_model[(feature, frequency)].append(fitted_model)

                    # backtest each instrument
                    for instrument in Y_allinstruments.index.levels[1]:
                        column = (instrument, 'performance', frequency)
                        # denormalize log increments, read in test set, resample to frequency
                        performance = self.inverse_scaler(self.Y[column], column).iloc[test_index].resample(f'{frequency}min').sum()
                        # set delta from prediction
                        features = self.X.xs(instrument, level=0, axis=1).loc[performance.index]
                        delta = pd.DataFrame(index=performance.index,
                                             columns=self.Y[(instrument, feature, frequency)].unique(),
                                             data=delta_strategy(features))[1]
                        # apply log increment
                        cumulative_pnl = (delta * (np.exp(performance)-1)).cumsum()

                        cumulative_performance = performance.cumsum()
                        result = pd.concat([result,
                                            pd.DataFrame(index=range(len(delta.index)),
                                                         columns=[(type(model), split_index, instrument, feature, frequency, datatype)
                                                                  for datatype in ['cumulative_performance', 'delta', 'cumulative_pnl']],
                                                         data=np.array([np.array(cumulative_performance),
                                                                        np.array(delta.values),
                                                                        np.array(cumulative_pnl.values)]).transpose())],
                                           axis=1, join='outer')

        result.columns = pd.MultiIndex.from_tuples(result.columns, names=['model', 'split_index', 'instrument', 'label', 'frequency', 'datatype'])

        return result

def perf_analysis(df: pd.DataFrame()) -> pd.DataFrame():
    result = pd.DataFrame()
    for frequency in next(list(level) for level in df.columns.levels if level.name == 'frequency') :
        cum_norm_pnl = df.xs((frequency, 'cumulative_pnl'), level=['frequency', 'datatype'], axis=1).dropna()
        total_perf = cum_norm_pnl.iloc[-1]/len(cum_norm_pnl.index)
        vol = cum_norm_pnl.std()
        sharpe = pd.concat({'sharpe': total_perf / vol}, names=['datatype'])
        drawdown = pd.concat({'drawdown': (cum_norm_pnl-cum_norm_pnl.cummax()).cummin().iloc[-1]}, names=['datatype'])

        result = pd.concat([result, sharpe, drawdown], axis=1)

    return result

def cta_main(parameters):
    engine = ResearchEngine(**parameters)
    raw_data = ResearchEngine.read_history(**parameters['data'])
    engine.build_X_Y(raw_data, parameters['feature_map'], parameters['label_map'])
    result = engine.run()
    result.to_csv('data.csv')
    analysis = perf_analysis(result).to_csv('analysis.csv')

    return result, analysis

if __name__ == "__main__":
    parameters = {'data': {'dirname': '/home/david/mktdata/binance/downloads',
                           'start_date': datetime(2022, 8, 15),
                           'selected_instruments': {'AVAXUSDT'}
                           },
                  'run_parameters': {'verbose': False,
                                     'normalize': True,
                                     'n_split': 9,
                                     'models': {'performance': [],  # LinearRegression(),
                                                # MLPRegressor(hidden_layer_sizes=(10,)),
                                                # LassoLarsCV(cv=TimeSeriesSplit(9), normalize=True)],
                                                'sign': [#LogisticRegressionCV(cv=TimeSeriesSplit(9), max_iter=1000)],
                                                RandomForestClassifier(min_samples_split=10, n_jobs=-1, warm_start=False)],#,
                                                # SVC(kernel='sigmoid', probability=True),
                                                # AdaBoostClassifier()],
                                                'big': [],
                                                # LogisticRegressionCV(cv=TimeSeriesSplit(9), max_iter=1000),
                                                # SVC(kernel='sigmoid'),  # works ?
                                                # RandomForestClassifier(min_samples_split=10, n_jobs=-1,
                                                #                       warm_start=True),
                                                # AdaBoostClassifier()]
                                                }
                                     },
                  'feature_map': {'close': {'transform': 'increment',
                                            'ewma_windows': [2, 5, 15, 60, 4 * 60, 8 * 60, 24 * 60, 72 * 60]},
                                  'volume': {'transform': 'log',
                                             'ewma_windows': [72 * 60]},
                                  'taker_imbalance': {'transform': 'arctanh',
                                                      'ewma_windows': []},
                                  # 'open_interest': {'transform': 'arctanh',
                                  #                   'ewma_windows': [2, 5, 72 * 60]},
                                  'premium': {'transform': 'as_is',
                                              'ewma_windows': [5, 60, 72 * 60]},
                                  'vw_increment': {'transform': 'vw_increment',
                                                   'ewma_windows': [2, 5, 15, 60, 4 * 60, 8 * 60, 24 * 60, 72 * 60]},
                                  'vw_premium': {'transform': 'vw_premium',
                                                 'ewma_windows': [5, 60, 72 * 60]}
                                  },
                  'label_map': {'performance': {'horizons': [5, 60, 24 * 60]},
                                'sign': {'horizons': [5, 60, 24 * 60]},
                                'big': {'horizons': [5, 60, 24 * 60]}
                                }}
    cta_main(parameters)