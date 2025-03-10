from algo import constants as co
from algo.asset import Algo
from algo.export_tx import (
    export_borrow_tx,
    export_deposit_collateral_tx,
    export_repay_tx,
    export_reward_tx,
    export_swap_tx,
    export_withdraw_collateral_tx
)
from algo.handle_simple import handle_participation_rewards, handle_unknown
from algo.util_algo import get_inner_transfer_asset, get_transfer_asset, get_transfer_receiver

# For reference
# https://github.com/Folks-Finance/folks-finance-js-sdk
# https://docs.folks.finance/developer/contracts

COMMENT_FOLKS = "Folks Finance"

APPLICATION_ID_FOLKS_LENDING_MANAGER = 465818260
APPLICATION_ID_FOLKS_GOVERNANCE = 694427622
APPLICATION_ID_FOLKS_ORACLE_ADAPTER = 689185988

APPLICATION_ID_FOLKS_POOL_ALGO = 686498781
APPLICATION_ID_FOLKS_POOL_USDC = 686500029
APPLICATION_ID_FOLKS_POOL_USDT = 686500844
APPLICATION_ID_FOLKS_POOL_GOBTC = 686501760
APPLICATION_ID_FOLKS_POOL_GOETH = 694405065
APPLICATION_ID_FOLKS_POOL_GALGO3 = 694464549

FOLKS_POOLS = [
    APPLICATION_ID_FOLKS_POOL_ALGO,
    APPLICATION_ID_FOLKS_POOL_USDC,
    APPLICATION_ID_FOLKS_POOL_USDT,
    APPLICATION_ID_FOLKS_POOL_GOBTC,
    APPLICATION_ID_FOLKS_POOL_GOETH,
    APPLICATION_ID_FOLKS_POOL_GALGO3,
]

APPLICATION_ID_FOLKS_REWARDS_ALGO = 686860954
APPLICATION_ID_FOLKS_REWARDS_USDC = 686862190
APPLICATION_ID_FOLKS_REWARDS_USDT = 686875498
APPLICATION_ID_FOLKS_REWARDS_GOBTC = 686876641
APPLICATION_ID_FOLKS_REWARDS_GOETH = 696044550

FOLKS_REWARDS_AGGREGATORS = [
    APPLICATION_ID_FOLKS_REWARDS_ALGO,
    APPLICATION_ID_FOLKS_REWARDS_USDC,
    APPLICATION_ID_FOLKS_REWARDS_USDT,
    APPLICATION_ID_FOLKS_REWARDS_GOBTC,
    APPLICATION_ID_FOLKS_REWARDS_GOETH,
]

FOLKS_TRANSACTION_MINT = "bQ=="             # "m"
FOLKS_TRANSACTION_DEPOSIT = "ZA=="          # "d"
FOLKS_TRANSACTION_WITHDRAW = "cg=="         # "r"
FOLKS_TRANSACTION_ADD_ESCROW = "YWU="       # "ae"
FOLKS_TRANSACTION_BORROW = "Yg=="           # "b"
FOLKS_TRANSACTION_REPAY_BORROW = "cmI="     # "rb"
FOLKS_TRANSACTION_INCREASE_BORROW = "aWI="  # "ib"
FOLKS_TRANSACTION_REDUCE_COLLATERAL = "cmM="  # "rc"
FOLKS_TRANSACTION_REWARD_CLAIM = "Yw=="     # "c"
FOLKS_TRANSACTION_REWARD_STAKED_EXCHANGE = "ZQ=="      # "e"
FOLKS_TRANSACTION_REWARD_IMMEDIATE_EXCHANGE = "aWU="   # "ie"

escrow_addresses = []


def _is_folks_galgo_optin_transaction(group):
    if len(group) != 2:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    op = transaction[co.TRANSACTION_KEY_APP_CALL].get("on-completion")
    return app_id == APPLICATION_ID_FOLKS_GOVERNANCE and op == "optin"


def _is_folks_galgo_mint_transaction(group):
    if len(group) != 2:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id == APPLICATION_ID_FOLKS_GOVERNANCE and FOLKS_TRANSACTION_MINT in appl_args


def _is_folks_deposit_transaction(group):
    if len(group) != 2:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_DEPOSIT in appl_args


def _is_folks_withdraw_transaction(group):
    if len(group) != 2:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_WITHDRAW in appl_args


def _is_folks_add_escrow_transaction(wallet_address, group):
    if len(group) != 2:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_PAYMENT:
        return False

    sender = transaction["sender"]
    if sender != wallet_address:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_DEPOSIT in appl_args


def _is_folks_borrow_transaction(group):
    if len(group) != 5:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    if app_id != APPLICATION_ID_FOLKS_ORACLE_ADAPTER:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    if app_id not in FOLKS_POOLS or FOLKS_TRANSACTION_BORROW not in appl_args:
        return False

    transaction = group[2]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_BORROW in appl_args


def _is_folks_repay_borrow_transaction(group):
    if len(group) != 4:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    if app_id not in FOLKS_POOLS or FOLKS_TRANSACTION_REPAY_BORROW not in appl_args:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_REPAY_BORROW in appl_args


def _is_folks_increase_borrow_transaction(group):
    if len(group) != 4:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    if app_id != APPLICATION_ID_FOLKS_ORACLE_ADAPTER:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_INCREASE_BORROW in appl_args


def _is_folks_reduce_collateral_transaction(group):
    if len(group) != 4:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    if app_id != APPLICATION_ID_FOLKS_ORACLE_ADAPTER:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_POOLS and FOLKS_TRANSACTION_REDUCE_COLLATERAL in appl_args


def _is_folks_reward_immediate_exchange_transaction(group):
    if len(group) != 2:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_REWARDS_AGGREGATORS and FOLKS_TRANSACTION_REWARD_IMMEDIATE_EXCHANGE in appl_args


def _is_folks_reward_staked_exchange_transaction(wallet_address, group):
    if len(group) != 3:
        return False

    transaction = group[0]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_PAYMENT:
        return False

    sender = transaction["sender"]
    if sender != wallet_address:
        return False

    transaction = group[1]
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_REWARDS_AGGREGATORS and FOLKS_TRANSACTION_REWARD_STAKED_EXCHANGE in appl_args


def is_folks_reward_claim_transaction(transaction):
    txtype = transaction["tx-type"]
    if txtype != co.TRANSACTION_TYPE_APP_CALL:
        return False

    app_id = transaction[co.TRANSACTION_KEY_APP_CALL]["application-id"]
    appl_args = transaction[co.TRANSACTION_KEY_APP_CALL]["application-args"]
    return app_id in FOLKS_REWARDS_AGGREGATORS and FOLKS_TRANSACTION_REWARD_CLAIM in appl_args


def is_folks_transaction(wallet_address, group):
    return (_is_folks_galgo_optin_transaction(group)
                or _is_folks_galgo_mint_transaction(group)
                or _is_folks_deposit_transaction(group)
                or _is_folks_withdraw_transaction(group)
                or _is_folks_add_escrow_transaction(wallet_address, group)
                or _is_folks_borrow_transaction(group)
                or _is_folks_repay_borrow_transaction(group)
                or _is_folks_increase_borrow_transaction(group)
                or _is_folks_reduce_collateral_transaction(group)
                or _is_folks_reward_immediate_exchange_transaction(group)
                or _is_folks_reward_staked_exchange_transaction(wallet_address, group))


def is_folks_escrow_address(address):
    return address in escrow_addresses


def handle_folks_transaction(wallet_address, group, exporter, txinfo):
    reward = Algo(group[0]["sender-rewards"])
    handle_participation_rewards(reward, exporter, txinfo)

    if _is_folks_galgo_optin_transaction(group):
        pass

    elif _is_folks_galgo_mint_transaction(group):
        _handle_folks_galgo_mint_transaction(group, exporter, txinfo)

    elif _is_folks_deposit_transaction(group):
        _handle_folks_deposit_transaction(group, exporter, txinfo)

    elif _is_folks_withdraw_transaction(group):
        _handle_folks_withdraw_transaction(group, exporter, txinfo)

    elif _is_folks_add_escrow_transaction(wallet_address, group):
        _handle_folks_add_escrow_transaction(group)

    elif _is_folks_borrow_transaction(group) or _is_folks_increase_borrow_transaction(group):
        _handle_folks_borrow_transaction(group, exporter, txinfo)

    elif _is_folks_repay_borrow_transaction(group):
        _handle_folks_repay_borrow_transaction(group, exporter, txinfo)

    elif _is_folks_reduce_collateral_transaction(group):
        _handle_folks_reduce_collateral_transaction(group, exporter, txinfo)

    elif _is_folks_reward_immediate_exchange_transaction(group):
        _handle_folks_reward_immediate_exchange_transaction(group, exporter, txinfo)

    elif _is_folks_reward_staked_exchange_transaction(wallet_address, group):
        _handle_folks_reward_staked_exchange_transaction(group, exporter, txinfo)

    else:
        handle_unknown(exporter, txinfo)


def _handle_folks_galgo_mint_transaction(group, exporter, txinfo):
    app_transaction = group[0]
    fee_amount = app_transaction["fee"]
    receive_asset = get_inner_transfer_asset(app_transaction)

    send_transaction = group[1]
    send_asset = get_transfer_asset(send_transaction)

    export_swap_tx(exporter, txinfo, send_asset, receive_asset, fee_amount, COMMENT_FOLKS)


# Note: For the moment we are ignoring fTokens as they are
# iliquid tokens that represent a deposit
def _handle_folks_deposit_transaction(group, exporter, txinfo):
    app_transaction = group[0]
    fee_amount = app_transaction["fee"]

    send_transaction = group[1]
    send_asset = get_transfer_asset(send_transaction)

    export_deposit_collateral_tx(exporter, txinfo, send_asset, fee_amount, COMMENT_FOLKS)


def _handle_folks_withdraw_transaction(group, exporter, txinfo):
    app_transaction = group[0]
    fee_amount = app_transaction["fee"]
    receive_asset = get_inner_transfer_asset(app_transaction)

    export_withdraw_collateral_tx(exporter, txinfo, receive_asset, fee_amount, COMMENT_FOLKS)


def _handle_folks_add_escrow_transaction(group):
    pay_transaction = group[0]
    receiver = get_transfer_receiver(pay_transaction)
    escrow_addresses.append(receiver)


def _handle_folks_borrow_transaction(group, exporter, txinfo):
    app_transaction = group[1]
    fee_amount = app_transaction["fee"]

    app_transaction = group[2]
    receive_asset = get_inner_transfer_asset(app_transaction)

    export_borrow_tx(exporter, txinfo, receive_asset, fee_amount, COMMENT_FOLKS)


def _handle_folks_repay_borrow_transaction(group, exporter, txinfo):
    app_transaction = group[0]
    fee_amount = app_transaction["fee"]

    send_transaction = group[3]
    send_asset = get_transfer_asset(send_transaction)

    export_repay_tx(exporter, txinfo, send_asset, fee_amount, COMMENT_FOLKS)


def _handle_folks_reduce_collateral_transaction(group, exporter, txinfo):
    pass


def _handle_folks_reward_immediate_exchange_transaction(group, exporter, txinfo):
    app_transaction = group[0]
    fee_amount = app_transaction["fee"]

    reward_asset = get_inner_transfer_asset(app_transaction)

    export_reward_tx(exporter, txinfo, reward_asset, fee_amount, COMMENT_FOLKS)


def _handle_folks_reward_staked_exchange_transaction(group, exporter, txinfo):
    pass


def handle_folks_reward_claim_transaction(transaction, exporter, txinfo):
    fee_amount = transaction["fee"]

    reward_asset = get_inner_transfer_asset(transaction)

    export_reward_tx(exporter, txinfo, reward_asset, fee_amount, COMMENT_FOLKS)
