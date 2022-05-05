from enum import Enum
class BookTypes(str, Enum):
    SPOT = 'spot'
    LOAN = 'loan'
    DEFI_SPOT = 'defi_spot'
    LINEAR_SWAP = 'linear_swap'
    INVERSE_SWAP = 'inverse_swap'
    QUANTO_SWAP = 'quanto_swap'
    LINEAR_FUTURE = 'linear_future'
    INVERSE_FUTURE = 'inverse_future'
    QUANTO_FUTURE = 'quanto_future'
    INDEX = 'index'
    OPTION = 'option'
    TOKENIZED_ASSET = 'tokenized_asset'
    VOLATILITY_DERIVATIVE = 'volatility_derivative'
    CONTRACT_SPOT = 'contract_spot'
    MARGIN_SPOT = 'margin_spot'
    CFD = 'cfd'


book_types_map = {BookTypes.SPOT.value: 0,
                BookTypes.LOAN.value: 1,
                BookTypes.CONTRACT_SPOT.value: 10,
                BookTypes.MARGIN_SPOT.value: 11,
                BookTypes.CFD.value: 12,
                BookTypes.LINEAR_SWAP.value: 101,
                BookTypes.INVERSE_SWAP.value: 102,
                BookTypes.QUANTO_SWAP.value: 103,
                BookTypes.LINEAR_FUTURE.value: 201,
                BookTypes.INVERSE_FUTURE.value: 202,
                BookTypes.QUANTO_FUTURE.value: 203,
                BookTypes.INDEX.value: 300,
                BookTypes.OPTION.value: 400,
                BookTypes.TOKENIZED_ASSET.value: 500,
                BookTypes.VOLATILITY_DERIVATIVE.value: 600,
                BookTypes.DEFI_SPOT.value: 1001}
            


instrument_uniform_naming_map = {
    'XBT':'BTC',
    'BCC': 'BCH',
    'DRK': 'DASH',
    'BCHSV':'BSV',
    'DSH':'DASH',
    'XRB':'NANO',
    'YEED':'YGG',
	'EUT':'EURT',
	'PAS':'PASS',
	'EUS':'EURS',
	'GAME.COM':'GTC',
	'HITCHAIN':'HIT',
	'MDOGE':'DOGE',
	'MERCURY':'MER',
	'BCHN':'BCH',
	'BCHA':'XEC',
	'BCHABC':'XEC',
	'BEYOND FINANCE':'BYN',
	'TERRAUST':'UST',
	'TOKAMAK NETWORK':'TON',
	'NFTX HASHMASKS INDEX':'MASK'
}
"""
    XBT → BTC: XBT is newer but BTC is more common among exchanges and sounds more like bitcoin (read more).
    BCC → BCH: The Bitcoin Cash fork is often called with two different symbolic names: BCC and BCH. The name BCC is ambiguous for Bitcoin Cash, it is confused with BitConnect. The ccxt library will convert BCC to BCH where it is appropriate (some exchanges and aggregators confuse them).
    DRK → DASH: DASH was Darkcoin then became Dash (read more).
    BCHABC → BCH: On November 15 2018 Bitcoin Cash forked the second time, so, now there is BCH (for BCH ABC) and BSV (for BCH SV).
    BCHSV → BSV: This is a common substitution mapping for the Bitcoin Cash SV fork (some exchanges call it BSV, others call it BCHSV, we use the former).
    DSH → DASH: Try not to confuse symbols and currencies. The DSH (Dashcoin) is not the same as DASH (Dash). Some exchanges have DASH labelled inconsistently as DSH, the ccxt library does a correction for that as well (DSH → DASH), but only on certain exchanges that have these two currencies confused, whereas most exchanges have them both correct. Just remember that DASH/BTC is not the same as DSH/BTC.
    XRB → NANO: NANO is the newer code for RaiBlocks, thus, CCXT unified API uses will replace the older XRB with NANO where needed. https://hackernoon.com/nano-rebrand-announcement-9101528a7b76
    USD → USDT: Some exchanges, like Bitfinex, HitBTC and a few other name the currency as USD in their listings, but those markets are actually trading USDT. The confusion can come from a 3-letter limitation on symbol names or may be due to other reasons. In cases where the traded currency is actually USDT and is not USD – the CCXT library will perform USD → USDT conversion. Note, however, that some exchanges have both USD and USDT symbols, for example, Kraken has a USDT/USD trading pair.

"""

