"""
Data parsing functions applicable to all transactions
"""

import logging
import re
from datetime import datetime, timezone

from sol import util_sol
from sol.api_rpc import RpcAPI
from sol.constants import BILLION, CURRENCY_SOL, INSTRUCTION_TYPE_DELEGATE, MINT_SOL, PROGRAM_STAKE
from sol.tickers.tickers import Tickers
from sol.TxInfoSol import TxInfoSol
from sol.handle_transfer import is_transfer
import sol.util_sol


# Returns transfers_in, transfers_out, embedded in txinfo. CORRECT
def parse_tx(txid, data, wallet_info):
    """ Parses data returned by RcpAPI.fetch_tx().  Returns TxInfoSol object """
    wallet_address = wallet_info.wallet_address
    result = data.get("result", None)

    # Handle old transaction where api fails.  Return transaction with just txid, nothing else.
    if result is None:
        logging.warning("Unable to fetch txid=%s.  Probably old transaction where api "
                        "fails.", txid)
        txinfo = TxInfoSol(txid, "", "", wallet_address)
        return txinfo

    # Handle old transaction where timestamp missing (something like before 12/2020)
    if not result.get("blockTime"):
        logging.warning("Detected timestamp missing for txid=%s.  Probably old transaction", txid)
        txinfo = TxInfoSol(txid, "", "", wallet_address)
        return txinfo

    # Transactions that resulted in error
    meta = result["meta"]
    if meta is None:
        logging.error("empty meta field.  txid=%s", txid)
        return None
    err = result["meta"]["err"]
    if err is not None:
        return None

    ts = result["blockTime"]
    timestamp = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
    instructions = data["result"]["transaction"]["message"].get("instructions", [])

    logging.warning("Processing at %s : txid=%s.  ", timestamp, txid)
    txinfo = TxInfoSol(txid, timestamp, "", wallet_address)

    txinfo.fee_blockchain = float(result["meta"]["fee"]) / BILLION
    txinfo.instructions = instructions
    txinfo.instruction_types = _instruction_types(instructions)
    txinfo.program_ids = [x["programId"] for x in txinfo.instructions]
    txinfo.input_accounts = _input_accounts(instructions)

    txinfo.inner = _extract_inner_instructions(data)
    txinfo.inner_parsed = _inner_parsed(txinfo.inner)

    txinfo.log_instructions, txinfo.log, txinfo.log_string = _log_messages(txid, data)

    txinfo.wallet_accounts = _wallet_accounts(txid, wallet_address, txinfo.instructions, txinfo.inner)
    txinfo.account_to_mint, txinfo.mints = _mints(data, wallet_address)

    txinfo.balance_changes_all, txinfo.balance_changes_wallet = _balance_changes(data, txinfo.wallet_accounts, txinfo.mints)
    print ("parse_tx::Before1 _transfers txinfo.balance_changes_all: \n ----\n", txinfo.balance_changes_all, "\n----\n")
    print ("parse_tx::Before2 _transfers txinfo.balance_changes_wallet: \n ----\n", txinfo.balance_changes_wallet, "\n----\n")
    # txinfo.transfers = _transfers(txinfo.balance_changes_wallet)
    # NOT OK : this returns a _transfers_out null
    # txinfo.transfers are incorrect
    # txinfo.transfers = _transfers( txinfo.balance_changes_wallet) # Returns transfers_in and transfers_out
    # txinfo.transfers_net, txinfo.fee = _transfers_net(txinfo, txinfo.transfers)

    txinfo.transfers = _transfers_new(txinfo.balance_changes_wallet) # Returns transfers_in and transfers_out
    txinfo.transfers_net, txinfo.fee = _transfers_net_new(txinfo, txinfo.transfers, txinfo.balance_changes_all) # Add up one last argument for atomic trades

    #txinfo.lp_transfers = _transfers_instruction(txinfo, txinfo.inner)
    # OK Perfect : we have the atomic trade on stSol
    # txinfo.lp_transfers are correct
    txinfo.lp_transfers = _transfers_instruction_new(txinfo, txinfo.inner)
    print ("parse_tx :: txinfo.lp_transfers = ", txinfo.lp_transfers)

    # txinfo.lp_transfers_net, txinfo.lp_fee = _transfers_net(txinfo, txinfo.lp_transfers, mint_to=True)
    # TODO
    txinfo.lp_transfers_net, txinfo.lp_fee = _transfers_net_new(txinfo, txinfo.lp_transfers, txinfo.balance_changes_all, mint_to=True)

    # Update wallet_info with any staking addresses found
    addresses = _staking_addresses_found(wallet_address, txinfo.instructions)
    for address in addresses:
        wallet_info.add_staking_address(address)

    return txinfo


def _staking_addresses_found(wallet_address, instructions):
    out = []
    for instruction in instructions:
        parsed = instruction.get("parsed", None)
        instruction_type = parsed.get("type", None) if (parsed and type(parsed) is dict) else None
        program = instruction.get("program")

        if (program == PROGRAM_STAKE and instruction_type == INSTRUCTION_TYPE_DELEGATE):
            stake_account = parsed["info"]["stakeAccount"]
            stake_authority = parsed["info"]["stakeAuthority"]
            if stake_authority == wallet_address:
                out.append(stake_account)

    return out


def _has_empty_token_balances(data, mints):
    post_token_balances = data["result"]["meta"]["postTokenBalances"]
    pre_token_balances = data["result"]["meta"]["preTokenBalances"]

    if len(post_token_balances) == 0 and len(pre_token_balances) == 0 and len(mints.keys()) > 1:
        return True
    else:
        return False


def _transfers(balance_changes):
    transfers_in = []
    transfers_out = []

    print ("_transfers START : balance_changes= \n", balance_changes)
    for account_address, (currency, amount_change) in balance_changes.items():
        if amount_change > 0:
            transfers_in.append((amount_change, currency, "", account_address))
        elif amount_change < 0:
            transfers_out.append((-amount_change, currency, account_address, ""))
    print ("_transfers END : transfers_in=\n", transfers_in)
    print ("_transfers END : transfers_out=\n", transfers_out)

    return transfers_in, transfers_out, []

# This outputs transfers_in and transfers_out, and they are already wrong as for StSOL, they give the 
# net amount of stSOL
def _transfers_new ( balance_changes): 
    transfers_in = []
    transfers_out = []

    print ("_transfers_new START : balance_changes= \n", balance_changes)
    for account_address, (currency, amount_change) in balance_changes.items():
        if amount_change > 0:
            transfers_in.append((amount_change, currency, "", account_address))
        elif amount_change < 0:
            transfers_out.append((-amount_change, currency, account_address, ""))
    print ("_transfers_new END : transfers_in=\n", transfers_in)
    print ("_transfers_new END : transfers_out=\n", transfers_out)

    return transfers_in, transfers_out, []


def _balance_changes(data, wallet_accounts, mints):
    balance_changes_sol = _balance_changes_sol(data)
    balance_changes_tokens = _balance_changes_tokens(data, mints)

    print ("_balance_changes:: balance_changes_tokens=", balance_changes_tokens)

    balance_changes = dict(balance_changes_sol)
    balance_changes.update(dict(balance_changes_tokens))

    balance_changes_wallet = {k: v for (k, v) in balance_changes.items() if k in wallet_accounts}
    return balance_changes, balance_changes_wallet


def _balance_changes_tokens(data, mints):
    account_keys = [row["pubkey"] for row in data["result"]["transaction"]["message"]["accountKeys"]]

    post_token_balances = data["result"]["meta"]["postTokenBalances"]
    pre_token_balances = data["result"]["meta"]["preTokenBalances"]

    a = {}
    b = {}
    balance_changes = {}
    for row in pre_token_balances:
        account_address, currency_a, amount_a, _ = _row_to_amount_currency(row, account_keys, mints)
        a[account_address] = (currency_a, amount_a)

    for row in post_token_balances:
        account_address, currency_b, amount_b, decimals = _row_to_amount_currency(row, account_keys, mints)
        b[account_address] = (currency_b, amount_b)

        # calculate change in balance
        currency_a, amount_a = a.get(account_address, (currency_b, 0.0))
        amount_change = round(amount_b - amount_a, decimals)

        # add to result
        balance_changes[account_address] = (currency_a, amount_change)

    # Handle case where post_token_balance doesn't exist for token (aka zero balance)
    for row in pre_token_balances:
        account_address, currency_a, amount_a, _ = _row_to_amount_currency(row, account_keys, mints)
        if account_address not in balance_changes:
            balance_changes[account_address] = (currency_a, -amount_a)

    return balance_changes


def _row_to_amount_currency(row, account_keys, mints):
    account_address = account_keys[row["accountIndex"]]
    mint = row["mint"]
    amount = row["uiTokenAmount"]["uiAmount"]
    decimals = row["uiTokenAmount"]["decimals"]
    if not amount:
        amount = 0.0
    currency = mints[mint]["currency"] if mint in mints else mint

    return account_address, currency, amount, decimals


def _balance_changes_sol(data):
    account_keys = [row["pubkey"] for row in data["result"]["transaction"]["message"]["accountKeys"]]

    post_balances_sol = data["result"]["meta"]["postBalances"]
    pre_balances_sol = data["result"]["meta"]["preBalances"]

    balance_changes = {}
    for i, account_address in enumerate(account_keys):
        amount = (float(post_balances_sol[i]) - float(pre_balances_sol[i])) / BILLION
        amount = round(amount, 9)
        if amount != 0:
            balance_changes[account_address] = (CURRENCY_SOL, amount)

    return balance_changes


def _wallet_accounts(txid, wallet_address, instructions, inner):
    token_accounts = RpcAPI.fetch_token_accounts(wallet_address)
    accounts_wallet = set(token_accounts.keys())

    accounts_instruction = _instruction_accounts(txid, wallet_address, instructions, inner)

    accounts = set(accounts_instruction)
    accounts = accounts.union(accounts_wallet)
    return accounts


def _instruction_types(instructions):
    out = []
    for instruction in instructions:
        parsed = instruction.get("parsed", None)
        instruction_type = parsed.get("type", None) if (parsed and type(parsed) is dict) else None
        program = instruction.get("program")
        out.append((instruction_type, program))
    return out


def _input_accounts(instructions):
    out = []
    for instruction in instructions:
        if "accounts" in instruction:
            out.append(instruction["accounts"])
    return out


def _mints(data, wallet_address):
    """ Returns
    account_to_mints: dict of <account_address> -> <mint_address>
    mints: dict of <mint_address> -> { "currency" : <ticker>, "decimals" : <decimals> }
    """

    # ## Get mints of wallet token accounts
    token_accounts = RpcAPI.fetch_token_accounts(wallet_address)
    out = dict(token_accounts)

    # ## Get mints of accounts found in preTokenBalances and postTokenBalances

    # Get account addresses
    accounts = [d["pubkey"] for d in data["result"]["transaction"]["message"]["accountKeys"]]

    # Get accounts of mints found in preTokenBalances and postTokenBalances
    mintlist = list(data["result"]["meta"]["preTokenBalances"])
    mintlist.extend(list(data["result"]["meta"]["postTokenBalances"]))
    for info in mintlist:
        account_index = info["accountIndex"]
        mint = info["mint"]
        decimals = info["uiTokenAmount"]["decimals"]

        account = accounts[account_index]
        out[account] = {
            "mint": mint,
            "decimals": decimals
        }

    # ## Repackage output format
    account_to_mint = {}
    mints = {}
    for account_address, info in out.items():
        mint = info["mint"]
        decimals = info["decimals"]

        account_to_mint[account_address] = mint
        mints[mint] = {
            "currency": Tickers.get(mint),
            "decimals": decimals
        }

    # Add wallet_address
    account_to_mint[wallet_address] = MINT_SOL
    mints[MINT_SOL] = {
        "currency": CURRENCY_SOL,
        "decimals": 9
    }

    return account_to_mint, mints


def _extract_inner_instructions(data):
    if "innerInstructions" not in data["result"]["meta"]:
        return []
    inner_instructions = data["result"]["meta"]["innerInstructions"]
    if inner_instructions is None:
        return []

    out = []
    for instructions_dict in inner_instructions:
        if "instructions" in instructions_dict:
            out.extend(instructions_dict["instructions"])

    return out


def _inner_parsed(inner_instructions):
    out = {}
    if not inner_instructions:
        return out

    for elem in inner_instructions:
        if "parsed" in elem:
            parsed = elem["parsed"]
            info = parsed["info"]
            type = parsed["type"]

            if type not in out:
                out[type] = []
            out[type].append(info)

    return out


def _instruction_accounts(txid, wallet_address, instructions, inner):
    accounts = set()
    accounts.add(wallet_address)
    instrs = instructions[:] + inner[:]

    # Add associated accounts from Instructions
    for instruction in instrs:
        if "parsed" in instruction:
            parsed = instruction["parsed"]
            if type(parsed) is dict:
                # if wallet associated with source
                if parsed.get("type") in ["initializeAccount", "approve", "transfer", "transferChecked"]:
                    info = parsed["info"]

                    # Grab set of addresses associated with source
                    keys = ["authority", "source", "newAccount", "owner", "account"]
                    addresses_source = set([info.get(k) for k in keys if k in info])
                    # Don't include token program address
                    addresses_source = set([x for x in addresses_source if not x.startswith("Token")])

                    if accounts.intersection(addresses_source):
                        accounts = accounts.union(addresses_source)

                # if wallet associated with destination
                if parsed.get("type") == "closeAccount":
                    info = parsed["info"]
                    account = info["account"]
                    destination = info["destination"]

                    if destination == wallet_address:
                        accounts.add(account)

    return accounts


def _transfers_instruction(txinfo, instructions):
    """ Returns transfers using information from instructions data (alternative method instead of balance changes) """
    txid = txinfo.txid
    account_to_mint = txinfo.account_to_mint
    wallet_accounts = txinfo.wallet_accounts
    wallet_address = txinfo.wallet_address

    transfers_in = []
    transfers_out = []
    transfers_unknown = []

    for i, instruction in enumerate(instructions):
        if "parsed" in instruction:
            parsed = instruction["parsed"]
            if type(parsed) is dict and parsed.get("type") == "transfer":
                info = parsed["info"]

                amount_string = info.get("amount", None)
                lamports = info.get("lamports", None)
                token_amount = info.get("tokenAmount", None)
                authority = info.get("authority", None)
                source = info.get("source", None)
                destination = info.get("destination", None)

                # self transfer
                if source and source == destination:
                    continue
                if amount_string == "0":
                    continue

                # Determine amount
                if amount_string is not None:
                    pass
                elif lamports is not None:
                    amount_string = lamports
                elif token_amount is not None:
                    amount_string = token_amount["amount"]

                # Determine mint
                if lamports:
                    mint = MINT_SOL
                elif source in account_to_mint and account_to_mint[source] != MINT_SOL:
                    mint = account_to_mint[source]
                elif destination in account_to_mint and account_to_mint[destination] != MINT_SOL:
                    mint = account_to_mint[destination]
                else:
                    mint = MINT_SOL

                # Determine amount, currency
                amount, currency = util_sol.amount_currency(txinfo, amount_string, mint)

                # Determine direction of transfer
                if source in wallet_accounts:
                    transfers_out.append((amount, currency, wallet_address, destination))
                elif authority in wallet_accounts:
                    transfers_out.append((amount, currency, wallet_address, destination))
                elif destination in wallet_accounts:
                    transfers_in.append((amount, currency, source, destination))
                else:
                    logging.error("Unable to determine direction for info: %s", info)
                    transfers_unknown.append((amount, currency, source, destination))

    return transfers_in, transfers_out, transfers_unknown

def _transfers_instruction_new(txinfo, instructions):
    """ Returns transfers using information from instructions data (alternative method instead of balance changes) """
    txid = txinfo.txid
    account_to_mint = txinfo.account_to_mint
    wallet_accounts = txinfo.wallet_accounts
    wallet_address = txinfo.wallet_address

    transfers_in = []
    transfers_out = []
    transfers_unknown = []

    print ("_transfers_instruction_new : \n", "txinfo = \n", txinfo ,"\n", "instructions : \n", instructions,"\n")
    for i, instruction in enumerate(instructions):
        if "parsed" in instruction:
            parsed = instruction["parsed"]
            if type(parsed) is dict and parsed.get("type") == "transfer":
                info = parsed["info"]

                amount_string = info.get("amount", None)
                lamports = info.get("lamports", None)
                token_amount = info.get("tokenAmount", None)
                authority = info.get("authority", None)
                source = info.get("source", None)
                destination = info.get("destination", None)
                print ("--------------------------------\n")
                print ("_transfers_instruction_new : \n", "amount_string = ", amount_string ,"\n", "token_amount = ", token_amount, "\n", ", authority = ", authority, "\n", "source = ", source, "\n", "destination = ", destination, "\n")

                # self transfer
                if source and source == destination:
                    print ( "_transfers_instruction_new : source and source == destination : \n", "source=", source, "\n", "destination=" , destination, "\n, : CONTINUE")
                    continue
                if amount_string == "0":
                    print ( "_transfers_instruction_new : amount_string == 0 CONTINUE")
                    continue

                # Determine amount
                if amount_string is not None:
                    pass
                elif lamports is not None:
                    amount_string = lamports
                elif token_amount is not None:
                    amount_string = token_amount["amount"]

                # Determine mint
                if lamports:
                    mint = MINT_SOL
                elif source in account_to_mint and account_to_mint[source] != MINT_SOL:
                    mint = account_to_mint[source]
                elif destination in account_to_mint and account_to_mint[destination] != MINT_SOL:
                    mint = account_to_mint[destination]
                else:
                    mint = MINT_SOL

                # Determine amount, currency
                amount, currency = util_sol.amount_currency(txinfo, amount_string, mint)
                print ( "_transfers_instruction_new : amount = ", amount, ", currency = ",currency)

                # Determine direction of transfer
                if source in wallet_accounts:
                    transfers_out.append((amount, currency, wallet_address, destination))
                    print ( "_transfers_instruction_new : ",i,".1 :  amount = ", amount, ", currency = ",currency, ", wallet_address = ", wallet_address, ", destination = ", destination, ", -->transfers_out")
                elif authority in wallet_accounts:
                    transfers_out.append((amount, currency, wallet_address, destination))
                    print ( "_transfers_instruction_new : ",i,".2 : amount = ", amount, ", currency = ",currency, ", wallet_address = ", wallet_address, ", destination = ", destination, ", -->transfers_out")
                elif destination in wallet_accounts:
                    transfers_in.append((amount, currency, source, destination))
                    print ( "_transfers_instruction_new : ",i,".3 : amount = ", amount, ", currency = ",currency, ", wallet_address = ", wallet_address, ", destination = ", destination, ", -->transfers_in")
                else:
                    logging.error("Unable to determine direction for info: %s", info)
                    print ( "_transfers_instruction_new : ",i,".4 : amount = ", amount, ", currency = ",currency, ", wallet_address = ", wallet_address, ", destination = ", destination, ", -->transfers_out")
                    print ( "_transfers_instruction_new : UNKNOWN unable to determine direction for info: %s", info)
                    print ( "_transfers_instruction_new : appending amount=", amount, "currency=", currency, ",source=", source, ", destination=", destination,"\n UNKNOWN unable to determine direction for info: %s", info)
                    transfers_unknown.append((amount, currency, source, destination))

    return transfers_in, transfers_out, transfers_unknown



def _extract_mint_to(instructions, wallet_address):
    try:
        for instruction in instructions:
            parsed = instruction.get("parsed", None)
            if parsed and parsed.get("type") == "mintTo":
                info = parsed["info"]
                amount = info["amount"]
                mint = info["mint"]

                return amount, mint

    except Exception:
        pass
    return None, None


def _add_mint_to_as_transfers(txinfo, net_transfers_in):
    """ Adds 'mintTo' instructions as transfers if found """

    # Extract any "mintTo" from instructions
    mint_amount_string, mint = _extract_mint_to(txinfo.instructions, txinfo.wallet_address)

    # Extract any "mintTo" from inner instructions
    if not mint:
        mint_amount_string, mint = _extract_mint_to(txinfo.inner, txinfo.wallet_address)

    if mint_amount_string and mint:
        mints_transfers_in = [x[1] for x in net_transfers_in]
        amount, currency = util_sol.amount_currency(txinfo, mint_amount_string, mint)

        if mint in mints_transfers_in:
            # Mint transaction already reflected as inbound transfer.  Do nothing
            pass
        else:
            net_transfers_in.append((amount, currency, "", ""))


def _transfers_net(txinfo, transfers, mint_to=False):
    """ Combines like currencies and removes fees from transfers lists """
    transfers_in, transfers_out, transfers_unknown = transfers

    # Sum up net transfer by currency, into a dict
    net_amounts = {}
    for amount, currency, source, destination in transfers_in:
        if currency not in net_amounts:
            net_amounts[currency] = 0
        net_amounts[currency] += amount
    for amount, currency, source, destination in transfers_out:
        if currency not in net_amounts:
            net_amounts[currency] = 0
        net_amounts[currency] -= amount

    # Convert dict into two lists of transactions, net_transfers_in and net_transfers_out
    net_transfers_in = []
    net_transfers_out = []
    for currency, amount in net_amounts.items():
        if amount < 0:
            source, destination = _get_source_destination(currency, False, transfers_in, transfers_out)
            net_transfers_out.append((-amount, currency, source, destination))
        elif amount > 0:
            source, destination = _get_source_destination(currency, True, transfers_in, transfers_out)
            net_transfers_in.append((amount, currency, source, destination))
        else:
            continue

    # Add any nft "mintTo" instruction as net transfer in (where applicable)
    if mint_to:
        _add_mint_to_as_transfers(txinfo, net_transfers_in)

    net_transfers_in, net_transfers_out, fee = util_sol.detect_fees(transfers_in, transfers_out)

    return (net_transfers_in, net_transfers_out, transfers_unknown), fee

def _transfers_net_new(txinfo, transfers, balance_changes_all, mint_to=False):
    """ Combines like currencies and removes fees from transfers lists """
    transfers_in, transfers_out, transfers_unknown = transfers

    print ("--------\n")
    print ("_transfers_net_new:: transfers = ", transfers, "\n------")
    print ("_transfers_net_new:: transfers_in = ", transfers_in, "\n------")
    print ("_transfers_net_new:: transfers_out = ", transfers_out, "\n------")
    print ("_transfers_net_new:: transfers_unknown = ", transfers_unknown, "\n------")
    print ("_transfers_net_new:: balance_changes_all= ", balance_changes_all)
    # Sum up net transfer by currency, into a dict
    net_amounts = {}
    net_transfers_in = []
    net_transfers_out = []

    # Populate net_amounts map, and ensure unicity of coin in the map 
    for amount, currency, source, destination in transfers_in:
        print ("--- \n_transfers_net_new::for loop : transfers_in : testing currency = ", currency, ", amount = ", amount, "\n") 
        if currency not in net_amounts: # Ensure unicity of coin in map
            net_amounts[currency] = 0
        net_amounts[currency] += amount # Ensure unicity of coin in map
        print ("--- \n_transfers_net_new::for loop : transfers_in : currency = ", currency, ", adding +amount = ", amount, "\n") 
    for amount, currency, source, destination in transfers_out:
        print ("--- \n_transfers_net_new::for loop : transfers_out : testing currency = ", currency, ", amount = ", amount, "\n") 
        if currency not in net_amounts:
            net_amounts[currency] = 0
        net_amounts[currency] -= amount
        print ("--- \n_transfers_net_new::for loop : currency = ", currency, ", substracting -amount = ", -amount, "\n") 

    print ("_transfers_net_new::net_amounts : ", net_amounts, "\n")
    # print ("_transfers_net_new::net_amounts.dict() : ", net_amounts.dict(), "\n")
    print ("_transfers_net_new::net_amounts.items() : ", net_amounts.items(), "\n")

    # Case 1 : we have SOL and one pair of currencies, which means this is a regular trade, not an atomic one 
    if len ( net_amounts.items()) >= 3 or len ( net_amounts.items()) == 1:  # len = 1 -> transfers, len >=3 : regular trade, len == 2 : atomic trade
        # Convert dict into two lists of transactions, net_transfers_in and net_transfers_out
        # THIS IS THE PART WHICH is BUGGED for atomic trades : for atomic trades, the balance_changes_wallet gives only one NET value, there is no pair "stSOL/stSOL"... Therefore there is only
        # one net amount which is the P&L, but the "nominal" of the trade does not show, as the cumulated sum makes it fungible. In that case, we need to hack the code and 
        # extract the balance_changes_all which has the initial BUY/SELL on stSOL.

        for currency, amount in net_amounts.items():
            print ("_transfers_net_new:: currency = ", currency,", amount = ", amount, "\n")
            if amount < 0:
                source, destination = _get_source_destination_new(currency, False, transfers_in, transfers_out)
                net_transfers_out.append((-amount, currency, source, destination))
            elif amount > 0:
                source, destination = _get_source_destination_new(currency, True, transfers_in, transfers_out)
                net_transfers_in.append((amount, currency, source, destination))
            else:
                continue
    # Case 2 : we have SOL and one currency only, which means this is an atomic trade 
    elif len ( net_amounts.items()) == 2:
        # Pseudo code : 
        # if the net_amount.items() excluding SOL has only element (or including SOL has only 2 elements)
        # this means we have an atomic trade.
        # In that case, we should get the net_transfers_in and net_transfers_out from the original balance_changes_all WHERE THE 2 AMOUNTS ARE DIFFERENT FROM the NET AMOUNT

        # Step #1 : look for the currency and the net amount of the atomic trade
        # Step #2 : look in balance_changes_all which change/line corresponds to the currency AND which are not egal to the net amount
        # Step #3 : populate net_transfers_out and net_transfers_in accordingly

        # Step #1 : look for the currency and the net amount of the atomic trade
        # --------

        atomic_currency = ""
        atomic_net_amount = 0 
        for currency, amount in net_amounts.items():
            print ("_transfers_net_new:: Testing for atomic trade : currency = ", currency,", amount = ", amount, "\n")
            if currency != "SOL" and amount != 0:
                print ("_transfers_net_new:: Found atomic trade : currency = ", currency,", amount = ", amount, "\n")
                # We found the atomic trade with its currency : atomic_currency
                atomic_currency = currency
                atomic_net_amount = amount
                break

        # Let's look for the atomic trade
        transfers_in, transfers_out, transfers_unknown = transfers 

        # Step #2 : look in balance_changes_all which change/line corresponds to the currency AND which are not egal to the net amount
        # --------
        for k, v in balance_changes_all.items():
            source = ""
            destination = ""
            print("_transfers_net_new : listing all elements of balance_changes_all :", k, ",", v,"\n")
            iter_currency, iter_amount = v
            # Step #3 : populate net_transfers_out and net_transfers_in accordingly
            # Now populate the net_transfers_out and net_transfers_in like any other pair
            if iter_currency == atomic_currency and iter_amount != atomic_net_amount: 
                # print ("_transfers_net_new:: atomic trade found : iter_currency = ", iter_currency,", atomic_net_amount = ", atomic_net_amount,", iter_amount = ", iter_amount, "\n")
                if iter_amount < 0:
                    # Warning : signs have been inverted from normal convention : the blockchain seems to invert the signs for atomic trades. TO BE VERIFIED 
                    transfers_in=[(-iter_amount, iter_currency,'',k)]
                    # Do not need to use _get_source_destination() anymore, we just "hard code" the source wallet
                    source = k 
                    print ("_transfers_net_new:: atomic trade found : iter_currency = ", iter_currency,", transfers_out= ", iter_amount, "\n")
                elif amount > 0:
                    # Warning : signs have been inverted from normal convention : the blockchain seems to invert the signs for atomic trades. TO BE VERIFIED 
                    transfers_out=[(iter_amount, iter_currency,k,'')]
                    # Do not need to use _get_source_destination() anymore, we just "hard code" the destination wallet
                    destination = k
                    print ("_transfers_net_new:: atomic trade found : iter_currency = ", iter_currency,", transfers_in ", iter_amount, "\n")
                else:
                    continue
                # Do not need to use _get_source_destination() anymore, we just "hard code" the wallets
                # We append one single atomic trade with source and destination (as opposed to 2 trades for, say, BTC/USD)
                # source, destination = _get_source_destination_new(currency, False, transfers_in, transfers_out)
                net_transfers_in.append((iter_amount, iter_currency, source, destination))
 

    # Add any nft "mintTo" instruction as net transfer in (where applicable)
    if mint_to:
        _add_mint_to_as_transfers(txinfo, net_transfers_in)

    net_transfers_in, net_transfers_out, fee = util_sol.detect_fees(transfers_in, transfers_out)

    return (net_transfers_in, net_transfers_out, transfers_unknown), fee




def _get_source_destination(currency, is_transfer_in, transfers_in, transfers_out):
    if is_transfer_in:
        for amt, cur, source, destination in transfers_in:
            if cur == currency:
                return source, destination
    else:
        for amt, cur, source, destination in transfers_out:
            if cur == currency:
                return source, destination

    raise Exception("Bad condition in _get_source_destination()")


def _get_source_destination_new(currency, is_transfer_in, transfers_in, transfers_out):
    if is_transfer_in:
        print ("_get_source_destination_new::is_transfer_in\n")
        for amt, cur, source, destination in transfers_in:
            print ("_get_source_destination_new::is_transfer_in, cur = ", cur, ", currency=", currency, ", source=", source, ",destination=", destination,"\n")
            if cur == currency:
                print ("_get_source_destination_new::currency = ", cur,", source=", source,", destination = ", destination, ", transfers_in\n")
                return source, destination
    else:
        print ("_get_source_destination_new::is_transfer_out\n")
        for amt, cur, source, destination in transfers_out:
            print ("_get_source_destination_new::is_transfer_in, cur = ", cur, ", currency=", currency, ", source=", source, ",destination=", destination,"\n")
            if cur == currency:
                print ("_get_source_destination_new::currency = ", cur,", source=", source,", destination = ", destination, ", transfers_out\n")
                return source, destination

    raise Exception("Bad condition in _get_source_destination_new()")



def _log_messages(txid, data):
    log_instructions = []
    log = []
    log_messages = data["result"]["meta"]["logMessages"]
    if log_messages is None:
        log_messages = []

    for line in log_messages:
        match = re.search("^Program log: Instruction: (.*)", line)
        if match:
            instruction = match.group(1)
            log_instructions.append(instruction)

        match = re.search("^Program log: (.*)", line)
        if match:
            line = match.group(1)
            log.append(line)

    log_string = "\n".join(log)
    return log_instructions, log, log_string
