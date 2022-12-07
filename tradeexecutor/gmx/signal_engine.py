import collections
import copy

from tradeexecutor.gmx.api import GmxAPI
from tradeexecutor.external_signal_engine import ExternalSignal

class GLPSignal(ExternalSignal):
    def __init__(self, parameters):
        if 'parents' not in parameters:
            raise Exception('parents needed')
        super().__init__(parameters)

        self.pnlexplain = collections.deque(maxlen=ExternalSignal.cache_size)
        self.history = collections.deque(maxlen=ExternalSignal.cache_size)
        for data in GmxAPI.static.values():
            if data['volatile']:
                self.data[data['normalized_symbol']] = None

    async def set_weights(self):
        '''needs venue to be reconciled'''
        lp_strategy = self.strategy.parents['GLP'] if self.strategy is not None else self.parameters['parents']['GLP']  # for the first time..
        gmx_state = lp_strategy.venue_api.state

        # TODO: test only !
        # lp_strategy.hedge_ratio *= -1

        glp_position = lp_strategy.position_manager.data['GLP']['delta']/gmx_state.valuation()
        weights = {'ETH/USDT': {'target': - (lp_strategy.hedge_ratio-1) * glp_position * gmx_state.partial_delta('WETH'),
                           'benchmark': 0.5*(gmx_state.pricesDown['WETH']+gmx_state.pricesUp['WETH'])},
                 'AVAX/USDT': {'target': - (lp_strategy.hedge_ratio-1) * glp_position * gmx_state.partial_delta('WAVAX'),
                           'benchmark': 0.5*(gmx_state.pricesDown['WAVAX']+gmx_state.pricesUp['WAVAX'])},
                 'BTC/USDT': {'target': - (lp_strategy.hedge_ratio-1) * glp_position * (gmx_state.partial_delta('WBTC') + gmx_state.partial_delta('WBTC')),
                                 'benchmark': 0.5*(gmx_state.pricesDown['WBTC'] + gmx_state.pricesUp['WBTC'])}}

        if self.data != weights:
            # pls note also updates timestamp when benchmark changes :(
            self.data = weights
            self.timestamp = copy.deepcopy(lp_strategy.venue_api.timestamp)

            self.history += [{'symbol': symbol,'timestamp':self.timestamp} | data
                for symbol,data in self.data.items()]
            print({key:value['target'] for key,value in weights.items()})

    def serialize(self) -> list[dict]:
        return list(self.history)

    def compile_pnlexplain(self, do_calcs=True):
        # venue_api already reconciled by reconcile()
        current_state = self.strategy.parents['GLP'].venue_api.state
        current = self.strategy.parents['GLP'].venue_api.serialize()

        # compute risk
        if do_calcs:
            current |= {'delta': {key: current_state.partial_delta(key) for key in GmxAPI.static},
                        'valuation': {key: current_state.valuation(key) for key in GmxAPI.static}}
            current['delta']['total'] = sum(current['delta'].values())
            current['valuation']['total'] = sum(current['valuation'].values())
            # compute plex
            if len(self.pnlexplain) > 0:
                previous = self.pnlexplain[-1]
                # delta_pnl
                current |= {'delta_pnl': {
                    key: previous['delta'][key] * (current['pricesDown'][key] - previous['pricesDown'][key])
                    for key in GmxAPI.static}}
                current['delta_pnl']['total'] = sum(current['delta_pnl'].values())
                # other_pnl (non-delta)
                current |= {'other_pnl': {
                    key: current['valuation'][key] - previous['valuation'][key] - current['delta_pnl'][key] for key in
                    GmxAPI.static}}
                current['other_pnl']['total'] = sum(current['other_pnl'].values())
                # rewards
                current |= {'reward_pnl': {
                    key: current['rewards'][key]*current['pricesDown'][key] - previous['rewards'][key]*previous['pricesDown'][key] for key in
                    ['WAVAX']}}
                current['reward_pnl']['total'] = sum(current['reward_pnl'].values())
                # discrepancy btw actual and estimate
                current['discrepancy'] = {'total': (current['actualAum']['total'] - current['valuation']['total'])}

                # capital and tx cost. Don't update GLP.
                current['capital'] = {
                    key: abs(current['delta'][key]) * current['pricesDown'][key] * self.strategy.parents['GLP'].parameters['delta_buffer'] for key
                    in GmxAPI.static}

                # delta hedge cost
                # TODO:self.strategy.hedge_strategy.venue_api.sweep_price_atomic(symbol, sizeUSD, include_taker_vs_maker_fee=True)
                current['tx_cost'] = {
                    key: -abs(current['delta'][key] - previous['delta'][key]) * self.strategy.parents['GLP'].parameters['hedge_tx_cost'] for key in
                    GmxAPI.static}
                current['tx_cost']['total'] = sum(current['tx_cost'].values())
            else:
                # initialize capital
                current['capital'] = {
                    key: abs(current['delta'][key]) * current['pricesDown'][key] * self.strategy.parents['GLP'].parameters['delta_buffer'] for key
                    in GmxAPI.static}
                current['capital']['total'] = current['actualAum']['total']
                # initial delta hedge cost + entry+exit of glp
                current['tx_cost'] = {key: -abs(current['delta'][key]) * self.strategy.parents['GLP'].parameters['hedge_tx_cost'] for key in
                                      GmxAPI.static}
                current['tx_cost']['total'] = sum(current['tx_cost'].values()) - 2 * GmxAPI.tx_cost * \
                                              current['actualAum']['total']
        # done. record.
        self.pnlexplain.append(current)