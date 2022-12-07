#!/usr/bin/env python3
from datetime import datetime, timedelta
from typing import NewType, Union, Iterator, Callable
from utils.api_utils import extract_args_kwargs, api

import dateutil.parser
import numpy as np
import pandas as pd
import os, sys, copy, itertools, sklearn, json
import plotly.express as px
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, LassoLarsIC, RidgeCV, ElasticNetCV, LassoCV, LassoLars, \
    LogisticRegression
from sklearn.svm import SVR, SVC
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, AdaBoostClassifier
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import TimeSeriesSplit, cross_val_score, cross_val_predict, train_test_split
import utils.io_utils

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
    data_interval = timedelta(seconds=60)
    stdev_big = 1.3

    def __init__(self, feature_map, label_map, run_parameters, input_data):
        self.feature_map = feature_map
        self.label_map = label_map
        self.run_parameters = run_parameters
        self.input_data = input_data

        self.temp_feature_data: Data = Data(dict()) # temporary, until we have a DataFrame
        self.temp_label_data: Data = Data(dict())  # temporary, until we have a DataFrame
        self.cached_scaler: dict[(Instrument, str, int), StandardScaler] = dict()

        self.X_Y: pd.DataFrame = pd.DataFrame()
        self.X: pd.DataFrame = pd.DataFrame()
        self.Y: pd.DataFrame = pd.DataFrame()

        self.models: list[sklearn.base.BaseEstimator] = []
        self.fitted_model: dict[tuple[RawFeature, FeatureExpansion, int], sklearn.base.BaseEstimator] = dict()

    @staticmethod
    def read_history(dirname,
                     start_date,
                     selected_instruments: set[Instrument]) -> FileData:
        file_data: FileData = FileData(dict())
        start_date = dateutil.parser.isoparse(start_date)
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
            performance = pd.Series(index=instrument_df.index,
                                    data=StandardScaler().fit_transform(pd.DataFrame((instrument_df * weights).sum(axis=1)))[:, 0])
            performance *= target_vol * np.sqrt(ResearchEngine.data_interval.total_seconds() / timedelta(days=365.25).total_seconds())

            for label, label_data in self.label_map.items():
                for horizon in label_data['horizons']:
                    temp = performance.rolling(horizon).sum()
                    if label == 'performance':
                        result[(instrument, 'performance', horizon)] = temp
                    elif label == 'sign':
                        result[(instrument, 'sign', horizon)] = temp.apply(np.sign)
                    elif label == 'big':
                        std_dev = performance.rolling(horizon).sum().expanding(horizon + 1).std()
                        result[(instrument, 'big', horizon)] = temp > ResearchEngine.stdev_big * std_dev

        self.temp_label_data = copy.deepcopy(result)

        yield result

    def normalize(self, input: pd.DataFrame) -> pd.DataFrame:
        '''
        store the scaler for later
        '''
        result: pd.DataFrame = input

        for feature, data in input.items():
            if not (feature[1] in self.run_parameters['no_normalize']):
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
            if not (feature[1] in self.run_parameters['no_normalize']):
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
                else:
                    raise NotImplementedError

                result[(instrument, raw_feature)] = remove_duplicate_rows(result[(instrument, raw_feature)])
                result[(instrument, raw_feature)].dropna(inplace=True)

        return result

    def laplace_expand(self, data_dict: RawData) -> Data:
        '''
        expands features into several ewma
        + volume_weighted features
        '''
        result: Data = Data(dict())
        for (instrument, raw_feature), data in data_dict.items():
            result[(instrument, raw_feature, 'live')] = data_dict[(instrument, raw_feature)]
            for window in self.feature_map[raw_feature]['ewma_windows']:
                temp_data = data_dict[(instrument, raw_feature)]
                result[(instrument, raw_feature, window)] = \
                    temp_data.ewm(times=temp_data.index, halflife=window*ResearchEngine.data_interval).mean() \
                        .dropna().rename((instrument, raw_feature, window)) # x.shift(periods=i))#
                if 'add_weight_by' in self.feature_map[raw_feature]:
                    temp_data = data_dict[(instrument, raw_feature)]
                    # exp because self.feature_map[volume] = log
                    weights = np.exp(data_dict[(instrument, self.feature_map[raw_feature]['add_weight_by'])])
                    result[(instrument, f'vw_{raw_feature}', window)] = \
                        ((temp_data * weights)
                            .ewm(times=temp_data.index, halflife=window*ResearchEngine.data_interval).mean() /
                        weights
                            .ewm(times=temp_data.index, halflife=window*ResearchEngine.data_interval).mean()
                         ).dropna().rename((instrument, raw_feature, window))
        return result

    def compute_labels(self, file_data: FileData, label_map: dict[RawFeature, dict]) -> Data:
        result: Data = Data(dict())
        for instrument, raw_feature in itertools.product(file_data.keys(), label_map.keys()):
            for horizon in label_map[raw_feature]['horizons']:
                future_perf = (file_data[instrument]['close'].shift(-horizon)/file_data[instrument]['close']).apply(np.log)
                if raw_feature == 'performance':
                    temp = future_perf
                elif raw_feature == 'sign':
                    temp = future_perf.apply(lambda x: 1 if x >= 0 else -1)
                elif raw_feature == 'big':
                    stdev = future_perf.expanding(horizon + 1).std()
                    temp = 0.5*(1+(future_perf - ResearchEngine.stdev_big *stdev).apply(np.sign))
                elif raw_feature == 'stop_limit':
                    temp = winning_trade(file_data[instrument]['close'])
                else:
                    raise NotImplementedError

                feature = (instrument, raw_feature, horizon)
                result[feature] = remove_duplicate_rows(temp).rename(feature)

        return result

    def build_X_Y(self, file_data: FileData, feature_map: dict[RawFeature, dict], label_map: dict[RawFeature, dict], unit_test=False) -> None:
        raw_data = self.compute_features(file_data, feature_map)
        self.temp_feature_data = self.laplace_expand(raw_data)

        if unit_test:
            self.X = self.normalize(pd.concat(self.temp_feature_data, axis=1).dropna())
            self.X.columns.rename(['instrument', 'feature', 'window'], inplace=True)
            self.Y = self.normalize(next(self.linear_unit_test(self.X)))
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

    def delta_strategy(self, features: pd.DataFrame, fitted_model: sklearn.base.BaseEstimator, feature: Feature) -> pd.Series:
        if hasattr(fitted_model, 'predict_proba'):
            probas = getattr(fitted_model, 'predict_proba')(features)
            predicted = 2 * probas.transpose()[1] - 1
        else:
            predicted = getattr(fitted_model, 'predict')(features)

        result = pd.DataFrame(index=features.index, columns=[feature], data=predicted)
        return self.inverse_normalize(result)[feature]

    def run(self) -> pd.DataFrame():
        holding_windows = [window for windows in self.label_map.values() for window in windows['horizons']]
        backtest = pd.DataFrame()

        # for each (all instruments, 1 feature, 1 horizon)
        for (raw_feature, frequency), label_df in self.Y.groupby(level=['feature', 'window'], axis=1):
            # for each model for that feature
            for model_name, model_params in self.run_parameters['models'][raw_feature].items():
                n_splits = self.run_parameters['splits_for_1000'] * np.log2(len(self.X_Y.index) / frequency) / 10
                tscv = TimeSeriesSplit(n_splits=max(2, int(n_splits)), gap=max(holding_windows)).split(self.X_Y)
                # cross validation
                for split_index, (train_index, test_index) in enumerate(tscv):
                    # fit 1 model on all instruments,on train_set index.
                    # cannot use stack as it reshuffles columns !!
                    model_obj = globals()[model_name](**model_params)
                    X_allinstruments = pd.concat([self.X.xs(instrument, level='instrument', axis=1).iloc[train_index]
                                                  for instrument in self.input_data['selected_instruments']],
                                                 axis=0)
                    Y_allinstruments = pd.concat([label_df.xs(instrument, level='instrument', axis=1).iloc[train_index]
                                                  for instrument in self.input_data['selected_instruments']],
                                                 axis=0)
                    fitted_model = model_obj.fit(X_allinstruments, Y_allinstruments.squeeze())
                    self.fitted_model[(raw_feature, frequency, split_index)] = fitted_model
                    if raw_feature == 'performance':
                        print(f'      r2 = {fitted_model.score(X_allinstruments, Y_allinstruments)} for split {split_index} / model {model_name} / label {(raw_feature, frequency)}')

                    # backtest each instrument, on test_set
                    times = self.X.iloc[test_index].index[::frequency]
                    for instrument in self.input_data['selected_instruments']:
                        # read log_increment at horizon = frequency, on test set. cumulative_perf is for display.
                        log_increment = self.temp_label_data[(instrument, 'performance', frequency)][times]
                        cumulative_performance = log_increment.cumsum().apply(np.exp)-1

                        # features -> prediction -> delta
                        features = self.X.xs(instrument, level='instrument', axis=1).loc[times]
                        delta = self.delta_strategy(features, fitted_model, Feature((instrument, raw_feature, frequency)))
                        # apply log increment to delta
                        cumulative_pnl = (delta * (log_increment.apply(np.exp) - 1)).cumsum().shift(1)

                        # add to backtest, putting all folds on a relative time axis
                        new_backtest = pd.DataFrame(dict(zip([(model_name, split_index, instrument, raw_feature, frequency, datatype)
                                                                     for datatype in ['cumulative_performance', 'delta', 'cumulative_pnl']],
                                                                    [cumulative_performance, delta, cumulative_pnl])))

                        new_backtest.index = new_backtest.index - new_backtest.index[0]
                        backtest = pd.concat([backtest, new_backtest], axis=1, join='outer')
                    print(f'    ran split {split_index} for {model_name} for {(raw_feature, frequency)}')
                print(f'  ran {model_name} for {(raw_feature, frequency)}')
            print(f'ran {(raw_feature, frequency)}')

        backtest.columns = pd.MultiIndex.from_tuples(backtest.columns, names=['model', 'split_index', 'instrument', 'feature', 'frequency', 'datatype'])
        coefs = pd.DataFrame(index=list(self.fitted_model.keys()),
                             columns=[('intercept', None)]+list(self.X.xs(self.input_data['selected_instruments'][0], level='instrument', axis=1).columns),
                             data=[np.append(data.intercept_, data.coef_) for data in self.fitted_model.values()
                                   if hasattr(data, 'intercept_') and hasattr(data, 'coef_')])

        return backtest, coefs

def perf_analysis(df_or_path: Union[pd.DataFrame(),str]) -> pd.DataFrame():
    new_values = []
    if os.path.isfile(df_or_path):
        df = pd.read_csv(df_or_path, header=list(range(6)), index_col=0)
    else:
        df = df_or_path.dropna()

    for model, label, frequency in set(zip(*[df.columns.get_level_values(level) for level in ['model', 'feature', 'frequency']])):
        cumulative_pnl = df.xs((model, label, frequency, 'cumulative_pnl'), level=['model', 'feature', 'frequency', 'datatype'], axis=1)

        # annual_perf
        annual_perf = pd.Series(cumulative_pnl.iloc[-1]/len(cumulative_pnl.index)/float(frequency) * timedelta(days=365.25).total_seconds() / ResearchEngine.data_interval.total_seconds(),
                                name=(model, label, frequency, 'annual_perf')).T

        # sortino
        pnl = cumulative_pnl.diff() / cumulative_pnl
        downside_dev = pnl.applymap(lambda x: min(0, x)**2).mean().apply(np.sqrt)
        sortino = pd.Series(pnl.mean() / downside_dev, name=(model, label, frequency, 'sortino')).T

        # drawdown
        drawdown = pd.Series((1 - cumulative_pnl/cumulative_pnl.cummax()).cummin().iloc[-1], name=(model, label, frequency, 'drawdown')).T

        new_values.append(pd.concat([annual_perf, sortino, drawdown], axis=1))

    result = pd.concat(new_values, axis=1)
    result.columns = pd.MultiIndex.from_tuples(result.columns,
                                               names=['model', 'feature', 'frequency',
                                                      'datatype'])
    return result.quantile(q=[.25, .5, .75])

def main(parameters):
    engine = ResearchEngine(**parameters)
    file_data = ResearchEngine.read_history(**parameters['input_data'])
    engine.build_X_Y(file_data, parameters['feature_map'], parameters['label_map'], unit_test=parameters['run_parameters']['unit_test'])
    result = engine.run()

    return result

if __name__ == "__main__":
    args, kwargs = extract_args_kwargs(sys.argv)

    with open(args[0], 'r') as fp:
        parameters = json.load(fp)
    result = main(parameters)
    result[0].to_csv('/home/david/Sety-project/pylibs/ux/result.csv')
    result[1].to_csv('/home/david/Sety-project/pylibs/ux/coefs.csv')
    analysis = perf_analysis('/home/david/Sety-project/pylibs/ux/result.csv')
    analysis.to_csv('/home/david/Sety-project/pylibs/ux/analysis.csv')
    analysis.xs('annual_perf', level='datatype', axis=1).to_csv('/home/david/Sety-project/pylibs/ux/quantile.csv')