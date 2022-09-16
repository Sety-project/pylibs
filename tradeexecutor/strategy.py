import asyncio, logging, threading, os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import ccxtpro

from tradeexecutor.order_manager import OrderManager
from tradeexecutor.position_manager import PositionManager
from tradeexecutor.signal_engine import SignalEngine
from tradeexecutor.venue_api import VenueAPI
from utils.MyLogger import ExecutionLogger
from utils.async_utils import safe_gather_limit, safe_gather
from utils.io_utils import myUtcNow


class Strategy(dict):
    '''abstract class Strategy leverages other managers to implement quoter (ie generate orders from mkt change or order feed)
    If there was a bus, its graph would be a tree and Strategy would be the top'''
    class ReadyToShutdown(Exception):
        def __init__(self, text):
            super().__init__(text)
    def __init__(self,parameters,signal_engine,venue_api,order_manager,position_manager):
        '''Strategy contructor also opens channels btw objects (a simple member data for now)'''
        super().__init__()
        self.parameters = parameters

        # TODO: is this necessary ?
        self_keys = ['target','benchmark','update_time_delta','entry_level','rush_in_level','rush_out_level','edit_price_depth','edit_trigger_depth','aggressive_edit_price_depth','aggressive_edit_trigger_depth','stop_depth','slice_size']
        for symbol in self.parameters['symbols']:
            self[symbol] = dict(zip(self_keys,[None]*len(self_keys)))
        self.timestamp = None

        # pointers to other objects (like a bus in a single thread...)
        if position_manager is not None: position_manager.strategy = self
        if order_manager is not None: order_manager.strategy = self
        if venue_api is not None: venue_api.strategy = self
        if signal_engine is not None: signal_engine.strategy = self

        self.position_manager = position_manager
        self.order_manager = order_manager
        self.venue_api = venue_api
        self.signal_engine = signal_engine

        self.logger = logging.getLogger('tradeexecutor')
        self.data_logger = ExecutionLogger(exchange_name=venue_api.id)

        self.rest_semaphor = asyncio.Semaphore(safe_gather_limit)
        self.lock = {'reconciling':threading.Lock()}
        # self.lock |= {symbol: CustomRLock() for symbol in self.parameters['symbols']}

    def serialize(self) -> dict[list[dict]]:
        '''{data type:[dict]}'''
        return {'parameters': [{'timestamp':self.timestamp} | self.parameters],
                'strategy': [{'symbol': symbol, 'timestamp':self.timestamp} | data for symbol,data in self.items()],
                'order_manager': self.order_manager.serialize(),
                'position_manager': self.position_manager.serialize(),
                'signal_engine': self.signal_engine.serialize()}

    @staticmethod
    async def build(parameters):
        signal_engine = await SignalEngine.build(parameters['signal_engine'])
        symbols = {'symbols':signal_engine.parameters['symbols']}
        venue_api = await VenueAPI.build(symbols | parameters['venue_api'])
        order_manager = await OrderManager.build(symbols | parameters['order_manager'])
        position_manager = await PositionManager.build(symbols | parameters['position_manager'])

        result = ExecutionStrategy(symbols|parameters['strategy'],
                                   signal_engine,
                                   venue_api,
                                   order_manager,
                                   position_manager)

        await result.reconcile()

        return result

    @staticmethod
    async def build_listener(self, order, parameters):
        signal_engine = await SignalEngine.build(parameters['signal_engine'])
        symbols = {'symbols':signal_engine.parameters['symbols']}
        venue_api = await VenueAPI.build(symbols | parameters['venue_api'])

        result = Strategy(symbols|parameters['strategy'],
                                   signal_engine,
                                   venue_api,
                                   order_manager=None,
                                   position_manager=None)

        await result.reconcile()

        return result

    async def run(self):
        self.logger.warning('cancelling orders')
        await safe_gather([self.venue_api.cancel_all_orders(symbol) for symbol in self],semaphore=self.rest_semaphor)

        coros = [self.venue_api.monitor_fills(), self.periodic_reconcile()] + \
                sum([[self.venue_api.monitor_orders(symbol),
                      self.venue_api.monitor_order_book(symbol),
                      self.venue_api.monitor_trades(symbol)]
                     for symbol in self], [])

        await safe_gather(coros,semaphore=self.rest_semaphor)

    async def update_quoter_parameters(self):
        raise Exception("must be implemented below this abstract class")

    async def periodic_reconcile(self):
        '''redundant minutely risk check'''
        while self.position_manager:
            try:
                await asyncio.sleep(self.position_manager.limit.check_frequency)
                await self.reconcile()
                self.position_manager.check_limit()
            except ccxtpro.NetworkError as e:
                self.logger.warning(str(e))
                self.logger.warning('reconciling after periodic_reconcile dropped off')
                await self.periodic_reconcile()
            except Exception as e:
                self.logger.warning(e, exc_info=True)
                raise e

    async def reconcile(self):
        '''update risk using rest
        all symbols not present when state is built are ignored !
        if some tickers are not initialized, it just uses markets
        trigger interception of all incoming messages until done'''

        # if already running, skip reconciliation
        if self.lock['reconciling'].locked():
            return

        # or reconcile, and lock until done. We don't want to place orders while recon is running
        with self.lock['reconciling']:
            await self.signal_engine.reconcile()
            await self.order_manager.reconcile()
            await self.position_manager.reconcile()
            await self.update_quoter_parameters()
            self.replay_missed_messages()

        # critical job is done, release lock and print data
        if self.order_manager.fill_flag:
            await self.data_logger.write_history(self.serialize())
            self.order_manager.fill_flag = False

    def replay_missed_messages(self):
        # replay missed _messages.
        while self.venue_api.message_missed:
            message = self.venue_api.message_missed.popleft()
            data = message['data']
            channel = message['channel']
            self.logger.warning(f'replaying {channel} after recon')
            if channel == 'fills':
                fill = self.venue_api.parse_trade(data)
                self.position_manager.process_fill(fill | {'orderTrigger': 'replayed'})
                self.order_manager.process_fill(fill | {'orderTrigger': 'replayed'})
            elif channel == 'orders':
                order = self.venue_api.parse_order(data)
                if order['symbol'] in self:
                    self.order_manager.acknowledgment(order | {'comment': 'websocket_acknowledgment','orderTrigger': 'replayed'})
            # we don't intercept the mktdata anyway...
            elif channel == 'orderbook':
                pass
                #self.populate_ticker(message['symbol'], message)
            elif channel == 'trades':
                pass
                #self.signal_engine.trades_cache[message['symbol']].append(message)

class ExecutionStrategy(Strategy):
    '''specialized to execute externally generated client orders'''

    async def update_quoter_parameters(self):
        '''updates key/values from signal'''
        self.timestamp = myUtcNow()

        # overwrite key/value from signal_engine
        for symbol,data in self.signal_engine.items():
            for key, value in data.items():
                self[symbol][key] = value

        no_delta = [abs(self.position_manager[symbol]['delta']) < self.parameters['significance_threshold'] * self.position_manager.pv
                    for symbol in self]
        no_target = [abs(data['target']*data['benchmark']) < self.parameters['significance_threshold'] * self.position_manager.pv
                    for symbol,data in self.items()]
        if all(no_delta) and all(no_target):
            raise Strategy.ReadyToShutdown(
                f'no {self.keys()} delta and no {dict(self)} order --> shutting down bot')

        if not self.signal_engine.vwap:
            await self.signal_engine.initialize_vwap()
        self.signal_engine.compile_vwap(frequency=timedelta(minutes=1))

        def basket_vwap_quantile(series_list, diff_list, quantiles):
            series = pd.concat([serie * coef for serie, coef in zip(series_list, diff_list)], join='inner', axis=1)
            return [-1e18 if quantile<=0 else 1e18 if quantile>=1 else series.sum(axis=1).quantile(quantile) for quantile in quantiles]

        nowtime = myUtcNow()
        scaler = 1 # max(0,(time_limit-nowtime)/(time_limit-self.inventory_target.timestamp))
        for symbol, data in self.items():
            data['timestamp'] = nowtime
            data['update_time_delta'] = data['target'] - self.position_manager[symbol]['delta'] / self.venue_api.mid(symbol)

        for symbol, data in self.items():
            # entry_level = what level on basket is too bad to even place orders
            # rush_in_level = what level on basket is so good that you go in at market on both legs
            # rush_out_level = what level on basket is so bad that you go out at market on both legs
            quantiles = basket_vwap_quantile(
                [self.signal_engine.vwap[_symbol]['vwap'] for _symbol in self],
                [data['update_time_delta'] for data in self.values()],
                [self.parameters['entry_tolerance'],self.parameters['rush_in_tolerance'],self.parameters['rush_out_tolerance']])

            data['entry_level'] = quantiles[0]
            data['rush_in_level'] = quantiles[1]
            data['rush_out_level'] = quantiles[2]

            stdev = self.signal_engine.vwap[symbol]['vwap'].std().squeeze()
            if not (stdev>0): stdev = 1e-16

            # edit_price_depth = how far to put limit on risk increasing orders
            data['edit_price_depth'] = stdev * np.sqrt(self.parameters['edit_price_tolerance']) * scaler
            # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
            data['edit_trigger_depth'] = stdev * np.sqrt(self.parameters['edit_trigger_tolerance']) * scaler

            # aggressive version understand tolerance as price increment
            if isinstance(self.parameters['aggressive_edit_price_increments'],int):
                data['aggressive_edit_price_depth'] = max(1,self.parameters['aggressive_edit_price_increments']) * self.venue_api.static[symbol]['priceIncrement']
                data['aggressive_edit_trigger_depth'] = max(1,self.parameters['aggressive_edit_trigger_increments']) * self.venue_api.static[symbol]['priceIncrement']
            elif self.parameters['aggressive_edit_price_increments'] in 'taker_hedge':
                data['aggressive_edit_price_depth'] = self.parameters['aggressive_edit_price_increments']
                data['aggressive_edit_trigger_depth'] = None # takers don't peg

                # stop_depth = how far to set the stop on risk reducing orders
            data['stop_depth'] = stdev * np.sqrt(self.parameters['stop_tolerance']) * scaler

            # slice_size: cap to expected time to trade consistent with edit_price_tolerance
            volume_share = self.parameters['volume_share'] * self.parameters['edit_price_tolerance'] * \
                           self.signal_engine.vwap[symbol]['volume'].mean()
            data['slice_size'] = volume_share

    def process_order_book_update(self, symbol, orderbook):
        '''
            leverages orderbook and risk to issue an order
            Critical loop, needs to go quick
            all executes in one go, no async
        '''
        strategy = self[symbol]
        mid = 0.5*(orderbook['bids'][0][0]+orderbook['asks'][0][0])

        original_size = self.signal_engine[symbol]['target'] - self.position_manager[symbol]['delta'] / mid
        if abs(original_size) < self.parameters['significance_threshold'] * self.position_manager.pv / mid \
                or abs(original_size) < self.venue_api.static[symbol]['sizeIncrement'] \
                or self.lock['reconciling'].locked():
            return

        # size to do:
        original_size = strategy['target'] - self.position_manager[symbol]['delta'] / mid

        # risk
        globalDelta = self.position_manager.delta_bounds(symbol)['global_delta']
        delta_limit = self.position_manager.limit.delta_limit * self.position_manager.pv
        marginal_risk = np.abs(globalDelta / mid + original_size) - np.abs(globalDelta / mid)

        # if increases risk but not out of limit, trim and go passive.
        if np.sign(original_size) == np.sign(globalDelta):
            # if (global_delta_plus / mid + original_size) > delta_limit:
            #     trimmed_size = delta_limit - global_delta_plus / mid
            #     self.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # elif (global_delta_minus / mid + original_size) < -delta_limit:
            #     trimmed_size = -delta_limit - global_delta_minus / mid
            #     self.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # else:
            #     trimmed_size = original_size
            # if np.sign(trimmed_size) != np.sign(original_size):
            #     self.logger.debug(f'skipping (we don t flip orders)')
            #     return
            if abs(globalDelta / mid + original_size) > delta_limit:
                if (globalDelta / mid + original_size) > delta_limit:
                    trimmed_size = delta_limit - globalDelta / mid
                elif (globalDelta / mid + original_size) < -delta_limit:
                    trimmed_size = -delta_limit - globalDelta / mid
                else:
                    raise Exception('what??')
                if np.sign(trimmed_size) != np.sign(original_size):
                    self.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> skipping (we don t flip orders)')
                    return
                else:
                    self.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> trimming to {trimmed_size * mid}')
            else:
                trimmed_size = original_size

            size = np.clip(trimmed_size, a_min=-strategy['slice_size'], a_max=strategy['slice_size'])

            current_basket_price = sum(self.venue_api.mid(_symbol) * self[_symbol]['update_time_delta']
                                       for _symbol in self.keys())
            # mkt order if target reached.
            # TODO: pray for the other coin to hit the same condition...
            if current_basket_price + 2 * abs(strategy['update_time_delta']) * self.venue_api.static[symbol]['takerVsMakerFee'] * mid < \
                    strategy['rush_in_level']:
                # TODO: could replace current_basket_price by two way sweep after if
                self.venue_api.peg_or_stopout(symbol, size, orderbook,
                                              edit_trigger_depth=strategy['edit_trigger_depth'],
                                              edit_price_depth='rush_in', stop_depth=None)
                return
            elif current_basket_price - 2 * abs(strategy['update_time_delta']) * self.venue_api.static[symbol]['takerVsMakerFee'] * mid > \
                    strategy['rush_out_level']:
                # go all in as this decreases margin
                size = - self.position_manager[symbol]['delta'] / mid
                if abs(size) > 0:
                    self.venue_api.peg_or_stopout(symbol, size, orderbook,
                                                  edit_trigger_depth=strategy['edit_trigger_depth'],
                                                  edit_price_depth='rush_out', stop_depth=None)
                return
            # limit order if level is acceptable (saves margin compared to faraway order)
            elif current_basket_price < strategy['entry_level']:
                edit_trigger_depth = strategy['edit_trigger_depth']
                edit_price_depth = strategy['edit_price_depth']
                stop_depth = None
            # hold off to save margin, if level is bad-ish
            else:
                return
        # if decrease risk, go aggressive without flipping delta
        else:
            size = np.sign(original_size) * min(abs(- globalDelta / mid), abs(original_size))
            edit_trigger_depth = strategy['aggressive_edit_trigger_depth']
            edit_price_depth = strategy['aggressive_edit_price_depth']
            stop_depth = strategy['stop_depth']
        self.venue_api.peg_or_stopout(symbol, size, orderbook, edit_trigger_depth=edit_trigger_depth,
                                      edit_price_depth=edit_price_depth, stop_depth=stop_depth)

    def process_trades(self, trades):
        pass
class AlgoStrategy(Strategy):
    async def update_quoter_parameters(self):
        '''updates quoter inputs
        reads file if present. If not: if no risk then shutdown bot, else unwind.
        '''
        if all(abs(self.position_manager[symbol]['delta']) < self.parameters[
            'significance_threshold'] * self.position_manager.pv for symbol in self):
            raise Strategy.ReadyToShutdown(
                f'no {self.keys()} delta and no {self.signal_engine} order --> shutting down bot')

        nowtime = myUtcNow()
        quantiles = self.signal_engine.compile_spread_distribution()

        for symbol, data in self.items():
            # entry_level = what level on basket is too bad to even place orders
            # rush_in_level = what level on basket is so good that you go in at market on both legs
            # rush_out_level = what level on basket is so bad that you go out at market on both legs
            is_long_carry = self.signal_engine[symbol]['target'] * (-1 if ':USD' in symbol else 1) > 0
            data['rush_in_level'] = quantiles[1 if is_long_carry else 0] * self.venue_api.mid(symbol) / 365.25
            data['rush_out_level'] = quantiles[0 if is_long_carry else 1] * self.venue_api.mid(symbol) / 365.25

            stdev = self.signal_engine.vwap[symbol]['vwap'].std().squeeze()
            if not (stdev > 0): stdev = 1e-16

            # edit_price_depth = how far to put limit on risk increasing orders
            data['edit_price_depth'] = stdev * np.sqrt(self.parameters['edit_price_tolerance'])
            # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
            data['edit_trigger_depth'] = stdev * np.sqrt(self.parameters['edit_trigger_tolerance'])

            # aggressive version understand tolerance as price increment
            if isinstance(self.parameters['aggressive_edit_price_increments'], float):
                data['aggressive_edit_price_depth'] = max(1, self.parameters[
                    'aggressive_edit_price_increments']) * self.venue_api.static[symbol]['priceIncrement']
                data['aggressive_edit_trigger_depth'] = max(1, self.parameters[
                    'aggressive_edit_price_increments']) * self.venue_api.static[symbol]['priceIncrement']
            else:
                data['aggressive_edit_price_depth'] = self.parameters['aggressive_edit_price_increments']
                data['aggressive_edit_trigger_depth'] = None  # takers don't peg

                # stop_depth = how far to set the stop on risk reducing orders
            data['stop_depth'] = stdev * np.sqrt(self.parameters['stop_tolerance'])

            # slice_size: cap to expected time to trade consistent with edit_price_tolerance
            volume_share = self.parameters['volume_share'] * self.parameters[
                'edit_price_tolerance'] * \
                           self.signal_engine.vwap[symbol]['volume'].mean()
            data['slice_size'] = volume_share

    def quoter(self, symbol):
        '''
            leverages orderbook and risk to issue an order
            Critical loop, needs to go quick
            all executes in one go, no async
        '''
        mid = self.venue_api.tickers[symbol]['mid']
        params = self[symbol]

        # size to do:
        original_size = params['target'] - self.position_manager[symbol]['delta'] / mid

        # risk
        globalDelta = self.position_manager.delta_bounds(symbol)['global_delta']
        delta_limit = self.position_manager.limit.delta_limit * self.position_manager.pv
        marginal_risk = np.abs(globalDelta / mid + original_size) - np.abs(globalDelta / mid)

        # if increases risk but not out of limit, trim and go passive.
        if np.sign(original_size) == np.sign(globalDelta):
            # if (global_delta_plus / mid + original_size) > delta_limit:
            #     trimmed_size = delta_limit - global_delta_plus / mid
            #     self.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # elif (global_delta_minus / mid + original_size) < -delta_limit:
            #     trimmed_size = -delta_limit - global_delta_minus / mid
            #     self.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # else:
            #     trimmed_size = original_size
            # if np.sign(trimmed_size) != np.sign(original_size):
            #     self.logger.debug(f'skipping (we don t flip orders)')
            #     return
            if abs(globalDelta / mid + original_size) > delta_limit:
                if (globalDelta / mid + original_size) > delta_limit:
                    trimmed_size = delta_limit - globalDelta / mid
                elif (globalDelta / mid + original_size) < -delta_limit:
                    trimmed_size = -delta_limit - globalDelta / mid
                else:
                    raise Exception('what??')
                if np.sign(trimmed_size) != np.sign(original_size):
                    self.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> skipping (we don t flip orders)')
                    return
                else:
                    self.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> trimming to {trimmed_size * mid}')
            else:
                trimmed_size = original_size

            size = np.clip(trimmed_size, a_min=-params['slice_size'], a_max=params['slice_size'])

            current_basket_price = sum(self.venue_api.mid(_symbol) * self[_symbol]['update_time_delta']
                                       for _symbol in self.keys())
            # mkt order if target reached.
            # TODO: pray for the other coin to hit the same condition...
            if current_basket_price + 2 * abs(params['update_time_delta']) * self.venue_api.static[symbol]['takerVsMakerFee'] * mid < \
                    self[symbol]['rush_in_level']:
                # TODO: could replace current_basket_price by two way sweep after if
                self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                                      edit_trigger_depth=params['edit_trigger_depth'],
                                                      edit_price_depth='rush_in', stop_depth=None)
                return
            elif current_basket_price - 2 * abs(params['update_time_delta']) * self.venue_api.static[symbol]['takerVsMakerFee'] * mid > \
                    self[symbol]['rush_out_level']:
                # go all in as this decreases margin
                size = - self.position_manager[symbol]['delta'] / mid
                if abs(size) > 0:
                    self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                                          edit_trigger_depth=params['edit_trigger_depth'],
                                                          edit_price_depth='rush_out', stop_depth=None)
                return
            # limit order if level is acceptable (saves margin compared to faraway order)
            elif current_basket_price < self[symbol]['entry_level']:
                edit_trigger_depth = params['edit_trigger_depth']
                edit_price_depth = params['edit_price_depth']
                stop_depth = None
            # hold off to save margin, if level is bad-ish
            else:
                return
        # if decrease risk, go aggressive without flipping delta
        else:
            size = np.sign(original_size) * min(abs(- globalDelta / mid), abs(original_size))
            edit_trigger_depth = params['aggressive_edit_trigger_depth']
            edit_price_depth = params['aggressive_edit_price_depth']
            stop_depth = params['stop_depth']
        self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                              edit_trigger_depth=edit_trigger_depth,
                                              edit_price_depth=edit_price_depth, stop_depth=stop_depth)

class ListenerStrategy(Strategy):
    async def run(self):
        coros = [self.periodic_reconcile(),
                 self.venue_api.monitor_order_book(self.keys()[0]),
                self.venue_api.monitor_trades(self.keys()[0])]

        await safe_gather(coros,semaphore=self.rest_semaphor)

    def process_order_book_update(self, trades):
        self.signal_engine.process_order_book_update(trades)
    def process_trades(self, trades):
        self.signal_engine.process_trades(trades)