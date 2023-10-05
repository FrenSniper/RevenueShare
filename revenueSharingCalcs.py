from concurrent.futures import ThreadPoolExecutor
import requests
import time
import pandas as pd
import json
from web3 import Web3
from requests import get
import os
import numpy as np
import math
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dotenv import load_dotenv


def getBlockAtTime(time):
    etherscanAPI = f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={math.floor(time)}&closest=before&apikey={etherscan_API}"
    block = get(etherscanAPI).json()["result"]
    return block


def getLatestBlock():
    # Get latest block number
    baseBlockURL = f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={math.floor(time.time())}&closest=before&apikey={etherscan_API}"
    block = get(baseBlockURL).json()["result"]
    return block


def apiCall(block):
    # Get all transfer events from a block
    x = pd.DataFrame()
    try:
        events_list = contract_instance.events.Transfer.get_logs(fromBlock=block, toBlock=block + 49)
    except:
        time.sleep(2)
        events_list = contract_instance.events.Transfer.get_logs(fromBlock=block, toBlock=block + 49)
    if len(events_list) > 0:
        for event in events_list:
            eventDict = dict(event["args"])
            eventDict["blockNumber"] = block + 100
            y = pd.DataFrame(eventDict, index=[0])
            x = pd.concat([y, x], ignore_index=False, axis=0)
    else:
        x = pd.DataFrame({"blockNumber": block, "from": np.nan, "to": np.nan, "value": np.nan}, index=[0])
    return x


def hashMultiProcess(start_block):
    # Uses multithreading to speed up the process of getting all transactions. Unable to pull all transactions at once, so need to iterate through blocks for events
    block = range(start_block, int(getLatestBlock()), 50)
    dfMain = pd.DataFrame()
    with ThreadPoolExecutor(max_workers=50) as executor:
        for i in executor.map(apiCall, block):
            if len(i) > 0:
                dfMain = pd.concat([dfMain, i], ignore_index=True, axis=0)
    return dfMain


def updateTransactions():
    # Update current database of blockchain transactions
    try:
        df1 = pd.read_csv("fsnipe_transactions.csv", index_col=False)
        print("Transaction database found, starting from block " + str(df1["blockNumber"].max() + 1))
        df2 = hashMultiProcess(int(df1["blockNumber"].max() + 1))
        df1 = pd.concat([df1, df2], ignore_index=True, axis=0)
    except FileNotFoundError:
        print("Transaction database not found, starting from block 18143456")
        df1 = hashMultiProcess(18143456)
    df1.to_csv("fsnipe_transactions.csv", index=False)
    df1["value"] = df1["value"].astype(float) / 1e18
    return df1


def getTokenHolders():
    url = "https://rpc.ankr.com/multichain/79258ce7f7ee046decc3b5292a24eb4bf7c910d7e39b691384c7ce0cfb839a01/?ankr_getTokenHolders="
    payload = {
        "jsonrpc": "2.0",
        "method": "ankr_getTokenHolders",
        "params": {
            "blockchain": "eth",
            "contractAddress": "0x987041FB536942bBc686ad7DBc7046D277881FeE"
        },
        "id": 1
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()["result"]["holders"]


def getBotFees(start):
    url = "https://rpc.ankr.com/multichain/79258ce7f7ee046decc3b5292a24eb4bf7c910d7e39b691384c7ce0cfb839a01/?ankr_getTransactionsByAddress="
    payload = {
        "jsonrpc": "2.0",
        "method": "ankr_getTransactionsByAddress",
        "params": {
            "address": ["0xE42974a809BE4C0316E08178af45D55617842f3B"],
            "blockchain": ["base"],
            "fromTimestamp": start
        },
        "id": 1
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    df = pd.DataFrame(response.json()["result"]["transactions"])
    df = df[~df["from"].str.lower().isin(
        ["0x987041fb536942bbc686ad7dbc7046d277881fee", "0xE42974a809BE4C0316E08178af45D55617842f3B",
         "0x092d5fD00AF855D37013F1D8eeC9c8Da2a6c6203"])][["value", "timestamp", "blockNumber"]]
    df["value"] = df["value"].apply(lambda x: Web3.to_int(hexstr=x))
    df["blockNumber"] = df["blockNumber"].apply(lambda x: Web3.to_int(hexstr=x))
    df["timestamp"] = df["timestamp"].apply(lambda x: Web3.to_int(hexstr=x))
    return df


def getTax(start):
    taxQuery = DuneClient.from_env().run_query_dataframe(QueryBase(name="ETH Tax", query_id=3082363,
                                                                   params=[QueryParameter.number_type(
                                                                       name="start_time_unix", value=start)]))
    taxQuery = taxQuery.rename(columns={"eth_tax_share_revenue": "value"})
    return taxQuery


pd.set_option('display.max_columns', None)
if __name__ == '__main__':

    # Load API keys
    load_dotenv()
    etherscan_API = os.getenv('ETHERSCAN_API')
    rpc_url = os.getenv('RPC_URL')


    # Initiate contract instance of $FSNIPE
    w3 = Web3(
        Web3.HTTPProvider(rpc_url))
    contract_instance = w3.eth.contract(address=Web3.to_checksum_address("0x987041FB536942bBc686ad7DBc7046D277881FeE"),
                                        abi=json.loads(
                                            get("https://api.etherscan.io/api?module=contract&action=getabi"
                                                "&address=0x987041FB536942bBc686ad7DBc7046D277881FeE"
                                                f"&apikey={etherscan_API}")
                                            .json()["result"]))
    # Update transaction database
    df = updateTransactions()

    # Get timestamps between last snapshot and now to query balance, need to work out how to program in automatically
    # selecting start block for revenue share period later. Will be running every 6 hours (21600 seconds)

    last_snapshot = 18237782
    blockInfo = f"https://api.etherscan.io/api?module=block&action=getblockreward&blockno={last_snapshot}&apikey={etherscan_API}"
    timestampStartBlock = get(blockInfo).json()["result"]["timeStamp"]

    timestampEndBlock = int(timestampStartBlock) + (21600 * 4 * 14)  # 6 Hours * 4 (1 day) * 14 (2 weeks)
    if timestampEndBlock > time.time():
        timestampEndBlock = math.floor(time.time())
    timestampRange = range(int(timestampStartBlock), timestampEndBlock, 21600)

    # Create dictionary of snapshot in unix time & block number
    blockDict = {}
    for t in timestampRange:
        blockDict[t] = getBlockAtTime(t)
    snapshotBlocks = list(blockDict.values())

    # Exclude wallets from revenue share (liquidity pool, airdrop contract, tax contract etc)
    excluded_wallets = ["0x6412fc4746ede2b06604f6d80feff915830ad517", "0x74e6a65c6463f8cad09e76d8850d7d945e5881cc",
                        "0x987041fb536942bbc686ad7dbc7046d277881fee"]
    holders = pd.DataFrame(getTokenHolders())
    holders = holders[holders["balance"].astype(float) > 2499][['holderAddress', 'balance']]
    holders = holders[~holders["holderAddress"].isin(excluded_wallets)]
    holdersEligible = holders["holderAddress"].to_list()

    for wallet in holdersEligible:
        # Filter transactions to only include transactions from wallet
        dfWallet = df[(df["from"].str.lower() == wallet) | (df["to"].str.lower() == wallet)]
        dfWallet.loc[dfWallet['from'].str.lower() == wallet, ['value']] *= -1
        dfWallet = dfWallet.sort_values(by=["blockNumber"], ascending=True)
        dfWallet = dfWallet.reset_index(drop=True)[["blockNumber", "value"]]
        dfWallet["cumSum"] = dfWallet["value"].cumsum()

        for block in snapshotBlocks:
            df_closest = dfWallet.loc[dfWallet['blockNumber'] <= int(block)]
            if df_closest.empty:
                holders.loc[holders['holderAddress'] == wallet, block] = 0
            else:
                token_balance = \
                    dfWallet.loc[dfWallet['blockNumber'] == df_closest["blockNumber"].max()]["cumSum"].values[0]
                holders.loc[holders['holderAddress'] == wallet, block] = token_balance

    for block in snapshotBlocks:
        holders[f"{block}"] = holders[f"{block}"] / holders[f"{block}"].sum()

    bot_fees_df = getBotFees(timestampStartBlock)

    # Pull taxes from Dune (https://dune.com/queries/3082363) start_time_unix variable to be start of snapshot period

    tax_df = getTax(timestampStartBlock)

    revenue_df = pd.concat([bot_fees_df, tax_df], ignore_index=True, axis=0).sort_values(by=["timestamp"], ascending=True)

    blockRangeLoop = range(0, len(snapshotBlocks), 1)

    for iteration in blockRangeLoop:
        if iteration == 0:
            continue
        revenue_sum = revenue_df.loc[(revenue_df['timestamp'] >= int(timestampRange[iteration-1])) &
                                     (revenue_df['timestamp'] < int(timestampRange[iteration]))]
        revenue = revenue_sum["value"].sum()
        block = snapshotBlocks[iteration]
        holders[f"{block}"] = holders[f"{block}"].apply(lambda x: x * revenue)

    holders['total'] = holders[snapshotBlocks].sum(axis=1)
    holders[["holderAddress", "total"]].set_index("holderAddress", drop=True).to_json(
        f"revShareCalcs_" + snapshotBlocks[-1] + ".json")
