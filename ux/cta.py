#!/usr/bin/env python3
import copy
import itertools
from datetime import *
from typing import NewType, Union, Iterator
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
FileData = NewType("Data", dict[Instrument, pd.DataFrame])

RawFeature = NewType("RawFeature", str)  # eg 'volume'
RawData = NewType("Data", dict[tuple[Instrument, RawFeature], pd.Series])

FeatureExpansion = NewType("FeatureExpansion", Union[int, str])  # frequency in min
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
    data_interval = 'min'
    stdev_big = 1.3

    def __init__(self, feature_map, label_map, run_parameters, **kwargs):
        self.feature_map = feature_map
        self.label_map = label_map
        self.run_parameters = run_parameters

        self.temp_feature_data: Data = Data(dict()) # temporary, until we have a DataFrame
        self.temp_label_data: Data = Data(dict())  # temporary, until we have a DataFrame
        self.cached_scaler: dict[(Instrument, str, int), StandardScaler] = dict()

        self.X_Y: pd.DataFrame = pd.DataFrame()
        self.X: pd.DataFrame = pd.DataFrame()
        self.Y: pd.DataFrame = pd.DataFrame()

        self.models: list[sklearn.base.BaseEstimator]

    @staticmethod
    def read_history(dirname,
                     start_date,
                     selected_instruments: set[Instrument]) -> FileData:
        file_data: FileData = FileData(dict())
        for filename in os.listdir(dirname):
            filesplit = filename.split('-')
            instrument: Instrument = Instrument(filesplit[0])
            data_type: RawFeature = RawFeature(filesplit[-1].replace('.csv', ''))
            if Instrument(instrument) in selected_instruments:
                new_df = pd.read_csv(os.path.join(os.sep, dirname, filename))
                new_df['open_time'] = new_df['open_time'].apply(lambda t: datetime.fromtimestamp(t / 1000))
                new_df = new_df.set_index('open_time').sort_index(ascending=True)[start_date:]
                if instrument in file_data and data_type in file_data[instrument]:
                    file_data[instrument][data_type] = pd.concat([file_data[instrument][data_type], new_df])
                else:
                    if instrument in file_data:
                        file_data[instrument][data_type] = new_df
                    else:
                        file_data[instrument] = {data_type: new_df}
        for instrument, data in file_data.items():
            #
            file_data[instrument] = data['klines'].join(data['premium'], rsuffix='_premium', how='outer').ffill()

        return file_data

    def logistic_unit_test(self, feature_data: pd.DataFrame) -> Iterator[pd.DataFrame]:
        '''
        generate performance as a weighted mean of feature/windows (like LinearRegression)
        generate sign as an indicator over a weighted mean of feature/windows (like LogisticRegressionCV)
        '''
        n = len(feature_data.columns)
        weights = pd.Series(index=feature_data.columns,
                            data=np.random.normal(loc=0,
                                                  scale=1,
                                                  size=n))
        result = pd.DataFrame()
        performance = (feature_data * weights.T).sum(axis=1, level=['instrument'])
        for label, label_data in self.label_map.items():
            for horizon in label_data['horizons']:
                temp = performance.rolling(horizon).sum()
                std_dev = performance.rolling(horizon).sum().expanding(horizon + 1).std()
                for instrument in feature_data.columns.get_level_values('instrument'):
                    if label == 'performance':
                        feature = (instrument, 'performance', horizon)
                        result[feature] = temp[instrument]
                    elif label == 'sign':
                        result[(instrument, 'sign', horizon)] = temp[instrument].apply(np.sign)
                    elif label == 'big':
                        result[(instrument, 'big', horizon)] = temp[instrument] > ResearchEngine.stdev_big * std_dev[instrument]

        result.columns = pd.MultiIndex.from_tuples(result.columns,
                                                   names=['instrument', 'feature', 'horizons'])
        self.temp_label_data = result

        yield result

    def normalize(self, input: pd.DataFrame) -> pd.DataFrame:
        '''
        store the scaler for later
        '''
        result: pd.DataFrame = input

        for feature, data in input.items():
            if (feature[1] in self.feature_map) or self.label_map[feature[1]]['normalize']:
                scaler: StandardScaler = StandardScaler()
                scaler.fit(pd.DataFrame(data))
                self.cached_scaler[feature] = scaler
                result[feature] = pd.Series(index=data.index,
                                            name=data.name,
                                            data=scaler.transform(pd.DataFrame(data).values)[:, 0])

        return result

    def inverse_normalize(self, input: pd.DataFrame) -> pd.DataFrame:
        '''
        uses the stored scaler
        '''
        result: pd.DataFrame = input
        for feature, data in input.items():
            if (feature[1] in self.feature_map) or self.label_map[feature[1]]['normalize']:
                scaler = self.cached_scaler[feature]
                result[feature] = pd.Series(index=data.index,
                                            name=data.name,
                                            data=scaler.inverse_transform(pd.DataFrame(data).values)[:, 0])

        return result

    def compute_features(self,
                         file_data: FileData,
                         feature_map: dict[RawFeature, dict]) -> RawData:
        result: RawData = RawData(dict())
        for instrument, df in file_data.items():
            for raw_feature in feature_map:
                if raw_feature == 'close':
                    result[(instrument, raw_feature)] = file_data[instrument][raw_feature]
                elif raw_feature == 'volume':
                    # take log
                    data = file_data[instrument][raw_feature]
                    result[(instrument, raw_feature)] = data.apply(lambda x: np.log(max([x, 1e-32])))
                elif raw_feature in ['taker_imbalance', 'open_interest']:
                    # take arctanh
                    result[(instrument, raw_feature)] = (file_data[instrument]['taker_buy_volume'] / file_data[instrument]['volume']).apply(
                        lambda x: np.arctanh(np.clip((2 * x - 1), 1e-8 - 1, 1 - 1e-8)))
                elif raw_feature == 'premium':
                    # as is
                    result[(instrument, raw_feature)] = file_data[instrument]['close_premium']
                elif raw_feature == 'vw_premium':
                    # volume weighted premium
                    result[(instrument, raw_feature)] = file_data[instrument]['close_premium'] * file_data[instrument]['volume']
                elif raw_feature == 'vw_close':
                    # volume weighted increments
                    result[(instrument, raw_feature)] = file_data[instrument]['close'] * file_data[instrument]['volume']
                else:
                    raise NotImplementedError

                result[(instrument, raw_feature)] = remove_duplicate_rows(result[(instrument, raw_feature)])
                result[(instrument, raw_feature)].dropna(inplace=True)

        return result

    def laplace_expand(self, data_dict: RawData) -> Data:
        result: Data = Data(dict())
        for (instrument, raw_feature), data in data_dict.items():
            result[(instrument, raw_feature, 'live')] = data_dict[(instrument, raw_feature)]
            for window in self.feature_map[raw_feature]['ewma_windows']:
                result[(instrument, raw_feature, window)] = \
                    data_dict[(instrument, raw_feature)].transform(
                        lambda x: x.ewm(times=x.index, halflife=timedelta(hours=window + 1)).mean()).dropna().rename(
                        (instrument, raw_feature, window))  # x.shift(periods=i))#
        return result

    def compute_labels(self, file_data: FileData, label_map: dict[RawFeature, dict]) -> Data:
        result: Data = Data(dict())
        for instrument, raw_feature in itertools.product(file_data.keys(), label_map.keys()):
            for horizon in label_map[raw_feature]['horizons']:
                if raw_feature == 'performance':
                    temp = np.log(1 - file_data[instrument]['close'].diff(-horizon) / file_data[instrument]['close'])
                elif raw_feature == 'sign':
                    temp = (-file_data[instrument]['close'].diff(-horizon)).apply(lambda x: 1 if x >= 0 else -1)
                elif raw_feature == 'big':
                    temp = -file_data[instrument]['close'].diff(-horizon) > ResearchEngine.stdev_big * file_data[instrument][
                        'close'].diff(-horizon).expanding(horizon + 1).std()
                elif raw_feature == 'stop_limit':
                    temp = winning_trade(file_data[instrument]['close'])
                else:
                    raise NotImplementedError

                feature = (instrument, raw_feature, horizon)
                result[feature] = remove_duplicate_rows(temp).rename(feature)

        return result

    def build_X_Y(self, file_data: FileData, feature_map: dict[RawFeature, dict], label_map: dict[RawFeature, dict], build_test=False) -> None:
        raw_data = self.compute_features(file_data, feature_map)
        self.temp_feature_data = self.laplace_expand(raw_data)

        if build_test:
            self.X = self.normalize(pd.concat(self.temp_feature_data, axis=1).dropna())
            self.X.columns.rename(['instrument', 'feature', 'window'], inplace=True)
            self.Y = self.normalize(next(self.logistic_unit_test(self.X)))
            self.X_Y = pd.concat([self.X, self.Y], axis=1).dropna()
            # concat / dropna to have same index
            self.X = self.X_Y[self.X.columns]
            self.Y = self.X_Y[self.Y.columns]
        else:
            self.temp_label_data = self.compute_labels(file_data, label_map)
            # concat / dropna to have same index
            self.X_Y = self.normalize(pd.concat(self.temp_feature_data | self.temp_label_data, axis=1).dropna())
            self.X_Y.columns.rename(['instrument', 'feature', 'window'], inplace=True)
            self.X = self.X_Y[self.temp_feature_data.keys()]
            self.Y = self.X_Y[self.temp_label_data.keys()]

        self.fitted_model: dict[tuple[RawFeature, FeatureExpansion], list[sklearn.base.BaseEstimator]] = dict()

    def run(self) -> pd.DataFrame():
        holding_windows = [window for windows in self.label_map.values() for window in windows['horizons']]
        result = pd.DataFrame()
        denormalized_labels = self.inverse_normalize(self.Y)

        # for each (all instruments, 1 feature, 1 horizon)
        for (raw_feature, frequency), label_df in self.Y.groupby(level=[1, 2], axis=1):
            self.fitted_model[(raw_feature, frequency)] = list()
            # for each model for that feature
            for model in self.run_parameters['models'][raw_feature]:
                tscv = TimeSeriesSplit(n_splits=int(np.log2(len(self.X_Y.index)/frequency/5)), gap=max(holding_windows)).split(self.X_Y)
                for split_index, (train_index, test_index) in enumerate(tscv):
                    # fit model on all instruments
                    X_allinstruments = self.X.iloc[train_index].stack(level=0)
                    Y_allinstruments = label_df.iloc[train_index].stack(level=0)
                    fitted_model = model.fit(X_allinstruments, Y_allinstruments.squeeze())
                    if callable(getattr(fitted_model, 'predict_proba')):
                        delta_strategy = lambda x: 2*getattr(fitted_model, 'predict_proba')(x) - 1
                    else:
                        delta_strategy = getattr(fitted_model, 'predict')
                    self.fitted_model[(raw_feature, frequency)].append(fitted_model)

                    # backtest each instrument
                    times = self.X.iloc[test_index].index[::frequency]
                    for instrument in Y_allinstruments.index.levels[1]:
                        # denormalize close, read on test set, resample to frequency
                        log_increment = self.temp_label_data[(instrument, 'performance', frequency)][times]
                        # set delta from prediction
                        features = self.X.xs(instrument, level=0, axis=1).loc[times]
                        delta = pd.DataFrame(index=times,
                                             columns=self.Y[(instrument, raw_feature, frequency)].unique(),
                                             data=delta_strategy(features))[1]
                        # apply log increment
                        cumulative_pnl = (delta * (log_increment.apply(np.exp) - 1)).cumsum().shift(1)

                        cumulative_performance = log_increment.cumsum().apply(np.exp)-1
                        result = pd.concat([result,
                                            pd.DataFrame(index=range(len(delta.index)),
                                                         columns=[(type(model), split_index, instrument, raw_feature, frequency, datatype)
                                                                  for datatype in ['cumulative_performance', 'delta', 'cumulative_pnl']],
                                                         data=np.array([np.array(cumulative_performance),
                                                                        np.array(delta.values),
                                                                        np.array(cumulative_pnl.values)]).transpose())],
                                           axis=1, join='outer')

        result.columns = pd.MultiIndex.from_tuples(result.columns, names=['model', 'split_index', 'instrument', 'label', 'frequency', 'datatype'])

        return result

def perf_analysis(df: pd.DataFrame()) -> pd.DataFrame():
    result = pd.DataFrame()
    for frequency in df.get_level_values('frequency') :
        cum_norm_pnl = df.xs((frequency, 'cumulative_pnl'), level=['frequency', 'datatype'], axis=1).dropna()
        total_perf = cum_norm_pnl.iloc[-1]/len(cum_norm_pnl.index)
        vol = cum_norm_pnl.std()
        sharpe = pd.concat({'sharpe': total_perf / vol}, names=['datatype'])
        drawdown = pd.concat({'drawdown': (cum_norm_pnl-cum_norm_pnl.cummax()).cummin().iloc[-1]}, names=['datatype'])

        result = pd.concat([result, sharpe, drawdown], axis=1)

    return result

def cta_main(parameters):
    engine = ResearchEngine(**parameters)
    file_data = ResearchEngine.read_history(**parameters['data'])
    engine.build_X_Y(file_data, parameters['feature_map'], parameters['label_map'], build_test=False)
    result = engine.run()
    result.to_csv('data.csv')
    analysis = perf_analysis(result).to_csv('analysis.csv')

    return result, analysis

if __name__ == "__main__":
    parameters = {'data': {'dirname': '/home/david/mktdata/binance/downloads',
                           'start_date': datetime(2022, 8, 15),
                           'selected_instruments': {'AVAXUSDT','ETHUSDT'}
                           },
                  'run_parameters': {'verbose': False,
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
                                  'premium': {'transform': 'passthrough',
                                              'ewma_windows': [5, 60, 72 * 60]},
                                  'vw_close': {'transform': 'passthrough',
                                                   'ewma_windows': [2, 5, 15, 60, 4 * 60, 8 * 60, 24 * 60, 72 * 60]},
                                  'vw_premium': {'transform': 'passthrough',
                                                 'ewma_windows': [5, 60, 72 * 60]}
                                  },
                  'label_map': {'performance': {'horizons': [5, 60, 24 * 60], 'normalize': True},
                                'sign': {'horizons': [5, 60, 24 * 60], 'normalize': False},
                                'big': {'horizons': [5, 60, 24 * 60], 'normalize': False}
                                }}
    cta_main(parameters)