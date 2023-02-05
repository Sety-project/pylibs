#!/usr/bin/env python3
from datetime import datetime, timedelta, timezone
from typing import NewType, Union, Iterator, Callable, Any
from pathlib import Path

import dateutil.parser
import numpy as np
import pandas as pd
import os, sys, copy, itertools, sklearn, json, pickle, asyncio
from scipy.interpolate import CubicSpline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, LassoLarsIC, RidgeCV, ElasticNetCV, LassoCV, LassoLars, \
    LogisticRegression
from sklearn.svm import SVR, SVC
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, AdaBoostClassifier, VotingClassifier
#from xgboost import XGBClassifier
from sklearn.neural_network import MLPRegressor, MLPClassifier
from sklearn.model_selection import TimeSeriesSplit, cross_validate, cross_val_score, cross_val_predict, train_test_split
from sklearn.metrics import RocCurveDisplay
# import shap
from sklearn_utils import GroupTimeSeriesSplit
from histfeed.history import get_history, history_start

Instrument = NewType("Instrument", str)  # eg 'ETHUSDT'
FileData = NewType("Data", dict[Instrument, pd.DataFrame])

RawFeature = NewType("RawFeature", str)  # eg 'volume'
RawData = NewType("Data", dict[tuple[Instrument, RawFeature], pd.Series])

FeatureExpansion = NewType("FeatureExpansion", Union[int, str])  # frequency in min
Feature = NewType("Feature", tuple[Instrument, RawFeature, FeatureExpansion])
Data = NewType("Data", dict[Feature, pd.Series])

def extract_args_kwargs(command,not_passed_token="not_passed"):
    args = [arg.split('=')[0] for arg in command if len(arg.split('=')) == 1]
    args = args[1:]
    kwargs = dict()
    for arg in command:
        key_value = arg.split('=')
        if len(key_value) == 2 and key_value[1] != not_passed_token:
            kwargs |= {key_value[0]:key_value[1]}
    return args,kwargs

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(index=df.index,
                        columns=df.columns,
                        data=StandardScaler().fit_transform(df))

def remove_duplicate_rows(df):
    return df[~df.index.duplicated()]

def weighted_ew_mean(temp_data: pd.DataFrame,
                     halflife: timedelta,
                     weights: pd.Series=None) -> pd.DataFrame:
    if weights is not None:
        result = (temp_data * weights)\
                     .ewm(times=temp_data.index, halflife=halflife).mean() \
                 / weights \
                     .ewm(times=temp_data.index, halflife=halflife).mean()
    else:
        result = temp_data \
                .ewm(times=temp_data.index, halflife=halflife).mean()
    return result

def weighted_ew_vol(temp_data: pd.DataFrame,
                    increment: int,
                    halflife: timedelta,
                    weights: pd.Series = None) -> pd.DataFrame:
    increments = temp_data.apply(np.log).diff(increment)
    if weights is not None:
        incr_sq = (increments * increments * weights)\
                     .ewm(times=temp_data.index, halflife=halflife).mean() \
                 / weights \
                     .ewm(times=temp_data.index, halflife=halflife).mean()
        incr_mean = (increments * weights)\
                     .ewm(times=temp_data.index, halflife=halflife).mean() \
                 / weights \
                     .ewm(times=temp_data.index, halflife=halflife).mean()
        result = (incr_sq - incr_mean*incr_mean).apply(np.sqrt)
    else:
        result = increments.ewm(times=temp_data.index, halflife=halflife).std()
    return result

def winning_trade(log_perf: pd.Series,
                  horizon: int,
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
    result = pd.DataFrame({'log_perf': log_perf})
    result['rolling_std'] = result['log_perf'].rolling(lvl_scaling_window).std()
    result['cum_perf'] = result['log_perf'].cumsum()
    result['trade_outcome'] = 0
    result.dropna(inplace=True)

    cum_perf = result['cum_perf'].values
    rolling_std = result['rolling_std'].values
    trade_outcome = result['trade_outcome'].values

    for i in range(len(result.index)):
        future = (cum_perf[i:(i+horizon)] - cum_perf[i])/rolling_std[i]
        # longs: break on stop or takeprofit
        for x in future:
            if x < stop:
                break
            elif x > takeprofit:
                trade_outcome[i] = 1
                break
        # shorts: break on stop or takeprofit
        for x in future:
            if x > -stop:
                break
            elif x < -takeprofit:
                trade_outcome[i] = -1
                break
    return pd.Series(index=result.index,
                     name='winning_trade',
                     data=trade_outcome)

class ResearchEngine:
    data_interval = timedelta(hours=8)
    execution_lag = 0
    stdev_quantum = 1.5
    stdev_cap = 3

    def __init__(self, feature_map, label_map, run_parameters, input_data,**paramsNOTUSED):
        self.feature_map = feature_map
        self.label_map = label_map
        self.run_parameters = run_parameters
        self.input_data = input_data

        self.temp_feature_data: Data = Data(dict()) # temporary, until we have a DataFrame
        self.temp_label_data: Data = Data(dict())  # temporary, until we have a DataFrame
        self.performance: dict[Instrument, pd.Series] = dict()

        self.X: pd.DataFrame = pd.DataFrame()
        self.Y: pd.DataFrame = pd.DataFrame()

        self.models: list[sklearn.base.BaseEstimator] = []
        self.fitted_model: dict[tuple[RawFeature, FeatureExpansion, str, int], sklearn.base.BaseEstimator] = dict()

    @staticmethod
    async def read_data(**kwargs):
        reader = getattr(ResearchEngine, kwargs.pop('source_type'))
        return await reader(**kwargs)

    @staticmethod
    async def read_from_binance(dirpath,
                          start_date,
                          selected_instruments: set[Instrument]) -> FileData:
        await asyncio.sleep(0)

        file_data: FileData = FileData(dict())
        start_date = dateutil.parser.isoparse(start_date)
        dirname = os.path.join(os.sep, Path.home(), *dirpath)
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
            file_data[instrument] = remove_duplicate_rows(
                data['klines'].join(
                    data['premium'], rsuffix='_premium', how='outer'
                ).ffill())

        #  just inserted a hack to print out marks, here. nothing to do with CTA.
        if False:
            marks = pd.concat({instrument: remove_duplicate_rows(data['mark'].ffill())
                               for instrument, data in file_data.items()}, axis=1)
            marks.to_csv(os.path.join(os.sep, Path.home(), "mktdata", "binance", "results", "marks.csv"))

        return file_data

    @staticmethod
    async def read_from_disk(dirpath,
                          start_date,
                          selected_instruments: set[Instrument]) -> FileData:
        file_data: FileData = FileData(dict())
        start_date = dateutil.parser.isoparse(start_date).replace(tzinfo=timezone.utc)
        dirname = os.path.join(os.sep, Path.home(), *dirpath)

        futures= pd.DataFrame()
        for future in selected_instruments:
            futures.loc[future, 'type'] = 'perpetual'
            futures.loc[future, 'underlying'] = future.split('USDT')[0]
            futures.loc[future, 'quote'] = 'USDT'

        history = await get_history(dirname, futures, start_date)

        result = FileData(dict())
        for future in selected_instruments:
            df = history[[column for column in history.columns if future in column]]
            df['close_premium'] = (df[f'{future}_mark_c']/df[f'{future}_indexes_c']-1)*24*365.25/8
            column_mapping = {f'{future}_price_o': 'open',
                              f'{future}_price_h': 'high',
                              f'{future}_price_l': 'low',
                              f'{future}_price_c': 'close',
                              f'{future}_price_volume': 'volume'}
            df.rename(columns=column_mapping, inplace=True)
            result |= {future: df[list(column_mapping.values())+['close_premium']].dropna()}

        return result

    def linear_unit_test(self, feature_data: pd.DataFrame, target_vol=1.0) -> Iterator[pd.DataFrame]:
        '''
        generate performance as a weighted mean of feature/windows (like LinearRegression)
        generate sign as an indicator over a weighted mean of feature/windows (like LogisticRegressionCV)
        '''
        nb_feature_per_instrument = int(feature_data.shape[1] / feature_data.columns.levshape[0])
        weights = np.random.normal(loc=0,
                                   scale=1,
                                   size=nb_feature_per_instrument)

        result = pd.DataFrame(columns=pd.MultiIndex.from_tuples([], names=['instrument', 'feature', 'window']))
        for instrument, instrument_df in feature_data.groupby(level='instrument', axis=1):
            # record performance for use in backtest
            self.performance[instrument] = file_data[instrument]['close']

            performance = pd.Series(index=instrument_df.index,
                                    data=StandardScaler().fit_transform(pd.DataFrame((instrument_df * weights).sum(axis=1)))[:, 0])
            performance *= target_vol * np.sqrt(ResearchEngine.data_interval.total_seconds() / timedelta(days=365.25).total_seconds())

            for label, label_data in self.label_map.items():
                for horizon in label_data['horizons']:
                    temp = performance.rolling(horizon).sum()
                    if label == 'quantized_zscore':
                        result[(instrument, 'quantized_zscore', horizon)] = temp.apply(np.sign)
                    else:
                        raise NotImplementedError

        self.temp_label_data = copy.deepcopy(result)

        yield result

    def transform_features(self,
                           file_data: FileData,
                           feature_map: dict[RawFeature, dict]) -> RawData:
        '''
        transforms file_data (eg log volume)
        '''
        result: RawData = RawData(dict())
        for instrument, df in file_data.items():
            for raw_feature, params in feature_map.items():
                if 'transform' in params:
                    data = file_data[instrument][raw_feature]
                    if params['transform'] == 'log':
                        result[(instrument, raw_feature)] = data.apply(lambda x: np.log(max([x, 1e-32])))
                    elif params['transform'] == 'arctanh':
                        result[(instrument, raw_feature)] = (
                                    file_data[instrument]['taker_buy_volume'] / file_data[instrument]['volume']).apply(
                            lambda x: np.arctanh(np.clip((2 * x - 1), 1e-8 - 1, 1 - 1e-8)))
                else:
                    result[(instrument, raw_feature)] = file_data[instrument][raw_feature]

                result[(instrument, raw_feature)] = remove_duplicate_rows(result[(instrument, raw_feature)])
                result[(instrument, raw_feature)].dropna(inplace=True)

        return result

    def expand_features(self, data_dict: RawData) -> Data:
        '''
        expands features
        '''
        result: Data = Data(dict())
        for (instrument, raw_feature), data in data_dict.items():
            # add several ewma, volume_weighted if requested
            for method, params in self.feature_map[raw_feature].items():
                if hasattr(self, method):
                    getattr(self, method)(data_dict, instrument, raw_feature, result, params)
        return result

    def ewma_expansion(self, data_dict, instrument, raw_feature, result, params) -> None:
        '''add several ewma, volume_weighted if requested'''
        temp: Data = Data(dict())
        if 'weight_by' in params:
            # exp because self.feature_map[volume] = log
            weights = np.exp(data_dict[(instrument, params['weight_by'])])
        else:
            weights = None

        all_windows = params['windows']
        for window in all_windows:
            # weighted mean = mean of weight*temp / mean of weigths
            data = weighted_ew_mean(data_dict[(instrument, raw_feature)],
                                    halflife=window * ResearchEngine.data_interval,
                                    weights=weights)
            if 'quantiles' in params:
                data = data.rolling(5*window).rank(pct=True)
            else:
                rolling = data.rolling(5 * window)
                data = (data-rolling.mean())/rolling.std()
            temp[window] = data
        if 'cross_ewma' in params and params['cross_ewma']:
            for i1, w1 in enumerate(all_windows):
                for i2, w2 in enumerate(all_windows[:i1]):
                    result[(instrument, raw_feature, f'ewma_{w2}_{w1}')] = temp[w2] - temp[w1]
        else:
            for window in all_windows:
                result[(instrument, raw_feature, f'ewma_{window}')] = temp[window]

    def min_expansion(self, data_dict, instrument, raw_feature, result, params) -> None:
        '''
        add several min max, may use volume clock.
        for windows = [t0, t1] returns minmax over [0,t0] and [t0,t1]
        '''
        if 'weight_by' in params:
            # exp because self.feature_map[volume] = log
            raise NotImplementedError
            volume = np.exp(data_dict[(instrument, params['weight_by'])])
            volume_cumsum = volume.cumsum() / volume.mean()
            volume_cumsum.drop(volume_cumsum.index[volume_cumsum.diff() == 0],inplace=True)
            volume_2_time = CubicSpline(volume_cumsum.values, volume_cumsum.index)
            volume_clocked = [data_dict[(instrument, raw_feature)].iloc[int(volume_2_time(i))]
                              for i in range(volume_cumsum.shape[0])]
        else:
            volume_clocked = data_dict[(instrument, raw_feature)].values

        windows = params['windows']
        for i, window in enumerate(windows):
            start = windows[i-1] if i>0 else 0
            length = window - start
            temp = data_dict[(instrument, raw_feature)].shift(start).rolling(length).min()
            result[(instrument, raw_feature, f'min_{start}_{window}')] = temp

    def max_expansion(self, data_dict, instrument, raw_feature, result, params) -> None:
        '''
        add several min max, may use volume clock.
        for windows = [t0, t1] returns minmax over [0,t0] and [t0,t1]
        '''
        if 'weight_by' in params:
            # exp because self.feature_map[volume] = log
            raise NotImplementedError
            volume = np.exp(data_dict[(instrument, params['weight_by'])])
            volume_cumsum = volume.cumsum() / volume.mean()
            volume_cumsum.drop(volume_cumsum.index[volume_cumsum.diff() == 0],inplace=True)
            volume_2_time = CubicSpline(volume_cumsum.values, volume_cumsum.index)
            volume_clocked = [data_dict[(instrument, raw_feature)].iloc[int(volume_2_time(i))]
                              for i in range(volume_cumsum.shape[0])]
        else:
            volume_clocked = data_dict[(instrument, raw_feature)].values

        windows = params['windows']
        for i, window in enumerate(windows):
            start = windows[i-1] if i>0 else 0
            length = window - start
            temp = data_dict[(instrument, raw_feature)].shift(start).rolling(length).max()
            result[(instrument, raw_feature, f'max_{start}_{window}')] = temp

    def hvol_expansion(self, data_dict, instrument, raw_feature, result, params) -> None:
        '''add several historical vols quantiles, volume_weighted if requested'''
        if 'weight_by' in params:
            # exp because self.feature_map[volume] = log
            weights = np.exp(data_dict[(instrument, params['weight_by'])])
        else:
            weights = None

        for increment_window in params['incr_window']:
            # weigthed std  = weighted mean of squares - square of weighted mean
            temp = weighted_ew_vol(data_dict[(instrument, raw_feature)],
                                   increment=increment_window[0],
                                   halflife=increment_window[1] * ResearchEngine.data_interval,
                                   weights=weights)
            if 'quantiles' in params:
                temp = temp.rolling(5*increment_window[1]).rank(pct=True)
            else:
                rolling = temp.rolling(5 * increment_window[1])
                temp = (temp-rolling.mean())/rolling.std()
            result[(instrument, raw_feature, f'hvol_{increment_window[0]}_{increment_window[1]}')] = temp

    def compute_labels(self, file_data: FileData, label_map: dict[RawFeature, dict]) -> Data:
        '''
        performance, sign, big and stop_limit from t+1 to t+1+horizon
        '''
        result: Data = Data(dict())
        for instrument, raw_feature in itertools.product(file_data.keys(), label_map.keys()):

            for horizon in label_map[raw_feature]['horizons']:
                if raw_feature == 'quantized_zscore':
                    # record performance for use in backtest
                    self.performance[instrument] = file_data[instrument]['close']

                    future_perf = (file_data[instrument]['close'].shift(-horizon - ResearchEngine.execution_lag) / file_data[instrument]['close'].shift(-ResearchEngine.execution_lag)).apply(np.log)
                    z_score = future_perf / future_perf.shift(horizon).rolling(50*horizon).std()

                    def quantized(x,buckets=label_map[raw_feature]['stdev_buckets']):
                        return next((key for key,values in buckets.items() if values[0] < x <= values[1]), np.NaN)
                    temp = z_score.apply(quantized).dropna()
                elif raw_feature == 'stop_limit':
                    # record performance for use in backtest
                    self.performance[instrument] = file_data[instrument]['close']

                    log_perf = (file_data[instrument]['close'].shift(-1) /
                                file_data[instrument]['close']).apply(np.log)
                    temp = winning_trade(log_perf, horizon)
                elif raw_feature == 'carry':
                    future_carry = file_data[instrument]['close_premium'].rolling(horizon).mean().shift(-horizon) # rescale for 1d ???
                    # record performance for use in backtest
                    self.performance[instrument] = future_carry

                    z_score = future_carry

                    def quantized(x,buckets=label_map[raw_feature]['carry_buckets']):
                        return next((key for key,values in buckets.items() if values[0] < x <= values[1]), np.NaN)
                    temp = z_score.apply(quantized).dropna()
                else:
                    raise NotImplementedError

                feature = (instrument, raw_feature, horizon)
                result[feature] = remove_duplicate_rows(temp).rename(feature)

        return result

    def build_X_Y(self, file_data: FileData, feature_map: dict[RawFeature, dict], label_map: dict[RawFeature, dict], unit_test=False) -> None:
        '''
        aggregates features and labels
        '''
        raw_data = self.transform_features(file_data, feature_map)
        self.temp_feature_data = self.expand_features(raw_data)

        if unit_test:
            self.X = pd.concat(self.temp_feature_data, axis=1).dropna()
            self.X.columns.rename(['instrument', 'feature', 'window'], inplace=True)
            self.Y = next(self.linear_unit_test(self.X))
            X_Y = pd.concat([self.X, self.Y], axis=1).dropna()
            # concat / dropna to have same index
            self.X = normalize(X_Y[self.X.columns])
            self.Y = X_Y[self.Y.columns]
        else:
            self.temp_label_data = self.compute_labels(file_data, label_map)
            # concat / dropna to have same index
            X_Y = pd.concat(self.temp_feature_data | self.temp_label_data, axis=1).dropna()
            X_Y.columns.rename(['instrument', 'feature', 'window'], inplace=True)
            self.X = normalize(X_Y[self.temp_feature_data.keys()])
            self.Y = X_Y[self.temp_label_data.keys()]

    def fit(self):
        '''
        for each label/model/fold, fits one model for all instruments.
        Then runs a backtest per instrument.
        '''
        # for each (all instruments, 1 feature, 1 horizon)...
        for (raw_feature, frequency), label_df in self.Y.groupby(level=['feature', 'window'], axis=1):
            # for each model for that feature...
            if raw_feature not in self.run_parameters['models']:
                continue
            for model_name, model_params in self.run_parameters['models'][raw_feature].items():
                if 'hidden_layer_sizes' in model_params['params']:
                    model_params['params']['hidden_layer_sizes'] = (model_params['params']['hidden_layer_sizes'],)
                model_obj = globals()[model_name](**model_params['params'])
                # ...cross validation
                n_splits = int(model_params['splits_for_1000'] * np.log2(len(self.X.index) / frequency) / 10)
                tscv = TimeSeriesSplit(n_splits=max(2, n_splits),
                                       gap=frequency)
                # cross_val = cross_validate(model_obj, X_allinstruments, Y_allinstruments,
                #                #scoring=[],
                #                n_jobs=-1,
                #                cv=tscv,
                #                return_estimator=True, return_train_score=True)
                # for split_index in range(n_splits):
                #     self.fitted_model[(raw_feature, frequency, model_name, split_index)] = cross_val['estimator'][split_index]
                # fitted_model = VotingClassifier(estimators=list(zip(map(str, range(n_splits)), cross_val['estimator'])), voting='soft')
                for split_index, (train_index, test_index) in enumerate(tscv.split(self.X)):
                    # fit 1 model on all instruments,on train_set index.
                    X_allinstruments_train = pd.concat([self.X.iloc[train_index].xs(instrument, level='instrument', axis=1)
                                                  for instrument in self.input_data['selected_instruments']],
                                                 axis=0)
                    Y_allinstruments_train = pd.concat([label_df.iloc[train_index].xs(instrument, level='instrument', axis=1)
                                                  for instrument in self.input_data['selected_instruments']],
                                                 axis=0).squeeze()
                    fitted_model = model_obj.fit(X_allinstruments_train, Y_allinstruments_train)
                    self.fitted_model[(raw_feature, frequency, model_name, split_index)] = (copy.deepcopy(fitted_model),test_index)

                    # display score
                    X_allinstruments_test = pd.concat([self.X.iloc[test_index].xs(instrument, level='instrument', axis=1)
                                                  for instrument in self.input_data['selected_instruments']],
                                                 axis=0)
                    Y_allinstruments_test = pd.concat([label_df.iloc[test_index].xs(instrument, level='instrument', axis=1)
                                                  for instrument in self.input_data['selected_instruments']],
                                                 axis=0).squeeze()
                    print(f'      r2 = {fitted_model.score(X_allinstruments_test, Y_allinstruments_test)} for split {split_index} / model {model_name} / label {(raw_feature, frequency)}')

                    if 'pickle_file' in self.run_parameters:
                        with open(self.run_parameters['pickle_file'], 'wb') as fp:
                            pickle.dump(self, fp)
                    print(f'    ran split {split_index} for {model_name} for {(raw_feature, frequency)}')
                print(f'  ran {model_name} for {(raw_feature, frequency)}')
            print(f'ran {(raw_feature, frequency)}')

class DeltaStrategy:
    def __init__(self, params: dict):
        self.parameters = params
    def run(self, features: pd.DataFrame, fitted_model: sklearn.base.BaseEstimator) -> pd.Series:
        '''
        converts a prediction into a delta (eg take position above proba_threshold proba, up to max_leverage for 100% proba)
        '''
        if isinstance(fitted_model, sklearn.base.ClassifierMixin):
            probas = getattr(fitted_model, 'predict_proba')(features)

            # prob->delta
            if self.parameters['type'] == 'threshold_leverage':
                expectation_threshold = self.parameters['params']['expectation_threshold']
                max_leverage = self.parameters['params']['max_leverage']

                threshold_applied = np.sign(fitted_model.classes_) * np.clip(np.abs(fitted_model.classes_) - expectation_threshold, a_min=0, a_max=999999999)
                leverage_applied = np.sign(threshold_applied) * max_leverage * np.clip(abs(threshold_applied), a_min=0, a_max=1)
                deltas = np.dot(leverage_applied, probas.transpose())
            elif self.parameters['type'] in ['max_loss_probability', 'carry_strategy']:
                max_loss_probability = self.parameters['params']['max_loss_probability']

                deltas = []
                class_down = np.array([1 if 'down' in class_ else 0 for class_ in fitted_model.classes_])
                class_up = np.array([1 if 'up' in class_ else 0 for class_ in fitted_model.classes_])
                class_deltas = np.array([self.parameters['params']['class_deltas'][class_] for class_ in fitted_model.classes_])
                for p in probas:
                    down_proba = sum(p * class_down)
                    up_proba = sum(p * class_up)
                    if (down_proba < max_loss_probability and up_proba > max_loss_probability) \
                            or (up_proba < max_loss_probability and down_proba > max_loss_probability):
                        deltas.append(sum(p * class_deltas))
                    else:
                        deltas.append(0.0)
            else:
                raise NotImplementedError
        elif isinstance(fitted_model, sklearn.base.RegressorMixin):
            deltas = getattr(fitted_model, 'predict')(features)
        else:
            raise NotImplementedError

        result = pd.Series(index=features.index, data=deltas)
        return result

class BacktestEngine:
    def __init__(self, params: dict, delta_strategy: DeltaStrategy):
        self.parameters = params
        self.delta_strategy = delta_strategy

    def backtest(self, features, fitted_model, frequency: int, rebalance_times, history) -> dict[str, pd.Series]:
        '''
        Runs a backtest on one instrument.
        '''
        # features -> prediction -> delta
        delta = self.delta_strategy.run(features, fitted_model)
        # apply log increment to delta
        tx_cost = self.parameters['tx_cost'] * delta.diff().apply(np.abs)
        if delta_strategy.parameters['type'] == "carry_strategy":
            performance = history[rebalance_times]
        else:
            performance = (history.shift(-frequency - ResearchEngine.execution_lag) / history.shift(-ResearchEngine.execution_lag) - 1)[rebalance_times]
        cumulative_pnl = (delta * performance - tx_cost).cumsum().shift(1)

        return {'cumulative_performance': history[delta.index]/history[delta.index[0]]-1,
                'delta': delta,
                'tx_cost': tx_cost,
                'cumulative_pnl': cumulative_pnl}

    def backtest_all(self, prediction_engine: ResearchEngine) -> pd.DataFrame():
        '''
        Runs a backtest per instrument.
        '''
        backtests = []
        for (raw_feature, frequency), label_df in prediction_engine.Y.groupby(level=['feature', 'window'], axis=1):
            for model_name, model_params in prediction_engine.run_parameters['models'][raw_feature].items():
                for _, _, _, split_index in filter(lambda x: x[0] == raw_feature
                                                    and x[1] == frequency
                                                    and x[2] == model_name,
                                          prediction_engine.fitted_model.keys()):
                    fitted_model, test_index = prediction_engine.fitted_model[(raw_feature, frequency, model_name, split_index)]
                    for instrument in prediction_engine.input_data['selected_instruments']:
                        rebalance_times = prediction_engine.X.iloc[test_index].index[::frequency]
                        features = prediction_engine.X.xs(instrument, level='instrument', axis=1).loc[rebalance_times]
                        temp = self.backtest(features, fitted_model,
                                             frequency, rebalance_times,
                                             prediction_engine.performance[instrument])
                        temp = {(model_name, split_index, instrument, raw_feature, frequency, key): value for key,value in temp.items()}

                        # add to backtest, putting all folds on a relative time axis
                        new_backtest = pd.DataFrame(temp)
                        new_backtest.index = new_backtest.index - rebalance_times[0]
                        backtests.append(new_backtest)

        backtest = pd.concat(backtests, axis=1, join='outer')
        backtest.columns = pd.MultiIndex.from_tuples(backtest.columns, names=['model', 'split_index', 'instrument', 'feature', 'frequency', 'datatype'])

        return backtest

def model_analysis(engine: ResearchEngine):
    feature_list=list(engine.X.xs(engine.input_data['selected_instruments'][0], level='instrument', axis=1).columns)
    result = []
    for model_tuple, (model, _) in engine.fitted_model.items():
        # result.append(pd.Series(name=model_tuple,
        #                         index=feature_list,
        #                         data=shap.Explainer(model)(engine.X)))
        if hasattr(model, 'intercept_') and hasattr(model, 'coef_'):
            if len(model.classes_) == 2:
                data = np.append(model.intercept_,model.coef_)
            else:
                data = np.append(np.dot(model.classes_, model.intercept_), np.dot(model.classes_, model.coef_))
            result.append(pd.Series(name=model_tuple,
                                    index=[('intercept', None)] + feature_list,
                                    data=data))
        if hasattr(model, 'feature_importances_'):
            result.append(pd.Series(name=model_tuple,
                                    index=feature_list,
                                    data=model.feature_importances_))
    return pd.concat(result, axis=1)

def perf_analysis(df_or_path: Union[pd.DataFrame,str]) -> pd.DataFrame:
    '''
    returns quantile across folds/instument for annual_perf, sortino, drawdown
    '''
    new_values = []
    if os.path.isfile(df_or_path):
        df = pd.read_csv(df_or_path, header=list(range(6)), index_col=0)
    else:
        df = df_or_path

    for model, label, frequency in set(zip(*[df.columns.get_level_values(level) for level in ['model', 'feature', 'frequency']])):
        cumulative_pnl = df.xs((model, label, frequency, 'cumulative_pnl'),
                               level=['model', 'feature', 'frequency', 'datatype'],
                               axis=1)
        # remove index
        cumulative_pnl = pd.DataFrame({col: data.dropna().values
                                       for col, data in cumulative_pnl.items()})
        # annual_perf. I consider signal works if mean(annual_perf over splits and instruments) > std over the same
        annual_perf = pd.Series(cumulative_pnl.iloc[-1] / len(cumulative_pnl.index) / float(frequency) * timedelta(days=365.25).total_seconds() / ResearchEngine.data_interval.total_seconds(),
                                name=(model, label, frequency, 'annual_perf'))


        # sortino
        pnl = cumulative_pnl.diff()
        rescaling = timedelta(days=365.25).total_seconds() / ResearchEngine.data_interval.total_seconds() / int(frequency)
        downside_dev = pnl.applymap(lambda x: min(0, x)**2).mean().apply(np.sqrt)
        sortino = pd.Series(pnl.mean() / downside_dev * np.sqrt(rescaling), name=(model, label, frequency, 'sortino'))

        # hit ratio
        hit_ratio = pnl.applymap(lambda x: x > 0).sum() / pnl.applymap(lambda x: x != 0).sum()
        hit_ratio.name = (model, label, frequency, 'hit_ratio')

        # drawdown
        drawdown = pd.Series((1 - cumulative_pnl/cumulative_pnl.cummax()).cummin().iloc[-1], name=(model, label, frequency, 'drawdown'))

        new_values.append(pd.concat([annual_perf, sortino, drawdown, hit_ratio], axis=1))

    result = pd.concat(new_values, axis=1)
    result.columns = pd.MultiIndex.from_tuples(result.columns,
                                               names=['model', 'feature', 'frequency',
                                                      'datatype'])
    return result.describe()

if __name__ == "__main__":
    args, kwargs = extract_args_kwargs(sys.argv)
    # load parameters
    with open(args[0], 'r') as fp:
        parameters = json.load(fp)
    outputdir = os.path.join(os.sep, Path.home(), "mktdata", "binance", "results")

    # load or build engine
    if 'pickle_file' in parameters['run_parameters']:
        outputfile = os.path.join(os.sep, outputdir, parameters['run_parameters']['pickle_file'])
        parameters['run_parameters']['pickle_file'] = outputfile
    else:
        outputfile = None
    if False: # outputfile is not None and os.path.isfile(outputfile):
        with open(outputfile, 'rb') as fp:
            engine = pickle.load(fp)
        print(f'read {outputfile}')
    else:
        engine = ResearchEngine(**parameters)
        file_data = asyncio.run(ResearchEngine.read_data(**parameters['input_data']))
        engine.build_X_Y(file_data, parameters['feature_map'], parameters['label_map'], unit_test=parameters['run_parameters']['unit_test'])
        engine.fit()
        print(f'fit engine')

    # backtest
    delta_strategy = DeltaStrategy(parameters['strategy'])
    strategy = BacktestEngine(parameters['backtest'], delta_strategy)
    result = strategy.backtest_all(engine)
    result.to_csv(os.path.join(os.sep, outputdir, 'backtest_trajectories.csv'))
    print(f'backtest')

    # analyse perf
    analysis = perf_analysis(os.path.join(os.sep, outputdir, 'backtest_trajectories.csv'))
    analysis.to_csv(os.path.join(os.sep, outputdir, 'perf_analysis.csv'))
    print(f'analyse perf')

    # inspect model
    model = model_analysis(engine)
    model.to_csv(os.path.join(os.sep, outputdir, 'model_inspection.csv'))
    print(f'inspect model')