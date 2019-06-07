import requests
import mysql.connector
import time
from web3 import Web3
import math

DEBUG = True

def debug_print(_msg = "message"):
    if DEBUG:
        print("RUN-TIME MESSAGE ::: %s \n\n" % _msg)


PROVIDER = Web3.HTTPProvider("https://mainnet.infura.io/v3/27f94aaf0b9b468fae7c869394b23ed0")
web3 = Web3(PROVIDER)

ETHERSCAN_KEY_0 = "WDDC73CFRBBFBX6E2A7ZPG48SHQJZJ3DKJ"
ETHERSCAN_KEY_1 = "EN8N9CKSDH6Z3AW5XEGKJ3PHBJNGNBHB7Q"
ETHERSCAN_KEY_2 = "WDDC73CFRBBFBX6E2A7ZPG48SHQJZJ3DKJ"

ETHERSCAN_API = "https://api.etherscan.io/api?module=account&action={action}&address={addr}&tag=latest&apikey={api_token}&offset=999999999"

PRICE_EP = "https://graphs2.coinmarketcap.com/currencies/ethereum/{actual}/{offset}/"
price_call_offset = 1000 #apply before milli-seconds conversion

INSERT_FRAME = """
INSERT INTO static_tokenized_set (
    address,
    outgoing_txs,
    incoming_txs,
    ttl_eth_sent,
    avg_eth_sent,
    ttl_usd_sent,
    avg_usd_sent,
    ttl_eth_recv,
    avg_eth_recv,
    ttl_usd_recv,
    avg_usd_recv,
    active_months,
    monthly_out_txs,
    monthly_incoming_txs,
    monthly_eth_sent,
    monthly_usd_sent,
    monthly_eth_recv,
    monthly_usd_recv,
    contracts_created,
    contract_txs_sent,
    incoming_avg_txs_time,
    incoming_std_txs_time,
    num_tokens_used,
    eth_balance,
    ttl_fee_sent,
    avg_fee_sent,
    num_internal_txs,
    num_external_txs,
    external_internal_ratio,
    failed_txs,
    successful_txs,
    fail_success_txs_ratio,
    mined_blocks
    ) VALUES (
        '{address}',
        {outgoing_txs},
        {incoming_txs},
        {ttl_eth_sent},
        {avg_eth_sent},
        {ttl_usd_sent},
        {avg_usd_sent},
        {ttl_eth_recv},
        {avg_eth_recv},
        {ttl_usd_recv},
        {avg_usd_recv},
        {active_months},
        {monthly_out_txs},
        {monthly_incoming_txs},
        {monthly_eth_sent},
        {monthly_usd_sent},
        {monthly_eth_recv},
        {monthly_usd_recv},
        {contracts_created},
        {contract_txs_sent},
        {incoming_avg_txs_time},
        {incoming_std_txs_time},
        {num_tokens_used},
        {eth_balance},
        {ttl_fee_sent},
        {avg_fee_sent},
        {num_internal_txs},
        {num_external_txs},
        {external_internal_ratio},
        {failed_txs},
        {successful_txs},
        {fail_success_txs_ratio},
        {mined_blocks}
    );
"""

class InsertStructCreator(object):

    def __init__(self, _addr_data):
        # This class is written the way it is because it made implementing the
        # most efficient loops for creating these values much easier as it
        # gave me a reference as I was building big weird loops.
        # TODO: Refactor this after writing production loops in
        # ... __createExternalTransactionVariables and
        # ... __createInternalTransactionVariables

        debug_print(_addr_data.get("addr"))

        self.total_trans = 0
        self.timestamp_array = []

        self.has_inner_run = False
        self.has_external_run = False

        self.addr_data = _addr_data
        self.earliest_trans = 0
        self.latest_trans = 0

        # Insert class data

        self.address = self.addr_data.get("addr") #
        self.outgoing_txs = 0 #
        self.incoming_txs = 0 #

        self.ttl_eth_sent = 0 #
        self.avg_eth_sent = 0 #

        self.ttl_usd_sent = 0 #
        self.avg_usd_sent = 0 #

        self.ttl_eth_recv = 0 #
        self.avg_eth_recv = 0 #

        self.ttl_usd_recv = 0 #
        self.avg_usd_recv = 0 #

        self.active_months = 1 #

        self.monthly_out_txs = 0 #
        self.monthly_incoming_txs = 0 #

        self.monthly_eth_sent = 0 #
        self.monthly_usd_sent = 0 #

        self.monthly_eth_recv = 0 #
        self.monthly_usd_recv = 0 #

        self.contracts_created = 0 #
        self.contract_txs_sent = 0 #

        self.incoming_avg_txs_time = 0 #
        self.incoming_std_txs_time = 0 #

        self.num_tokens_used = 0 #

        self.eth_balance = self.addr_data.get("eth_bal") #

        self.ttl_fee_sent = 0 #
        self.avg_fee_sent = 0 #

        self.num_internal_txs = len(self.addr_data.get("internal_tx_list")) #
        self.num_external_txs = len(self.addr_data.get("tx_list")) #

        self.external_internal_ratio = self.num_external_txs / float(self.num_internal_txs) if self.num_internal_txs > 0 else 0 #


        self.failed_txs = 0 #
        self.successful_txs = 0 #
        self.fail_success_txs_ratio = 0 #
        self.mined_blocks = self.addr_data.get("mined_blocks") #

    def __grabPriceAtTime(self, _time):
        time = int(_time)
        offset = time + 1000
        price_ep = PRICE_EP.format(actual = time * 1000, offset = offset * 1000)
        price = 0

        try:
            price = requests.get(price_ep).json().get("price_usd")[0][1]
        except:
            price = 0

        return price

    def __collapseTokensWithDecimals(self, _token_amount, _decimals):
        tkn_amount = int(_token_amount)
        decimals = int(_decimals) * -1
        return tkn_amount * 10**decimals

    def __determineTimeDiffSTD(self, _diff_array, _mean):
        mean_diffs = sum([d - _mean for d in _diff_array])
        sq_mean_diffs = mean_diffs**2
        return math.sqrt(sq_mean_diffs/len(_diff_array))



    def __determineTimeDiffs(self):
        if not (self.has_inner_run and self.has_external_run):
            raise IOError("Need to run inner and external to be called first.")

        diffs = []
        previous = 0
        filtered_array = list(set(self.timestamp_array))

        for timestamp in filtered_array:
            if previous == 0:
                previous = timestamp
                continue

            diffs.append(timestamp - previous)
            previous = timestamp

        self.incoming_avg_txs_time = sum(diffs) / len(diffs)
        self.incoming_std_txs_time = self.__determineTimeDiffSTD(diffs, self.incoming_avg_txs_time)





    def __determineTokenAmount(self):
        token_list = self.addr_data.get("token_tx_list")
        self.tokens_used = sum([self.__collapseTokensWithDecimals(t.get("value"), t.get("tokenDecimal")) for t in token_list])


    def __determineIfContractAndIncrement(self, _transaction):
        to = _transaction.get("to")

        if len(to) == 0 or to.startswith("0x") == False:
            return False

        to = web3.toChecksumAddress(to)

        storage = web3.eth.getCode(to)
        if storage.hex().startswith("0x6060604052") or storage.hex().startswith("0x6080604052"):
            self.contract_txs_sent += 1
            return True

        else:
            return False

    def __setTimeDistribution(self, _timestamp):
        ts = int(_timestamp)

        if self.earliest_trans == 0 or ts < self.earliest_trans:
            self.earliest_trans = ts

        if self.earliest_trans == 0 or ts > self.latest_trans:
            self.latest_trans = ts

    def __determineMonthNumber(self):
        mon_secs = 2592000
        differential = self.latest_trans  - self.earliest_trans
        self.active_months = differential / mon_secs

    def __validateRun(self,_type):
        if _type == "tx_list":
            self.has_external_run = True
        elif _type == "internal_tx_list":
            self.has_inner_run = True

    def __createTransactionVariables(self, _type): # type is: tx_list OR internal_tx_list
        """
          Fuck this fucking function.
        """

        if _type not in ["tx_list", "internal_tx_list"]:
            raise IOError("incorrect type designated.  Can only be 'tx_list' or 'internal_tx_list'")

        tx_list = self.addr_data.get(_type)

        # ========== Pre-Loop ==========
        if _type == "tx_list":
            self.num_external_txs = len(tx_list)
        elif _type == "internal_tx_list":
            self.num_internal_txs = len(tx_list)

        if len(tx_list) == 0:
            debug_print("no transactions for %s" % _type)
            self.__validateRun(_type)
            return

        self.active_months = 0

        # ========== Loop ==========
        for transaction in tx_list:
            debug_print("running TRANSACTION: %s" % transaction)
            self.__setTimeDistribution(transaction.get("timeStamp"))
            self.timestamp_array.append(int(transaction.get("timeStamp")))
            self.__determineIfContractAndIncrement(transaction)

            if len(transaction.get("contractAddress")) > 0:
                self.contracts_created += 1

            price = self.__grabPriceAtTime(transaction.get("timeStamp"))

            is_success = bool(transaction.get("isError") == "0")
            self.ttl_fee_sent += int(transaction.get("gas"))

            if is_success:
                self.successful_txs += 1
            else:
                self.failed_txs += 1


            if transaction.get("to") == self.address and is_success:
                self.incoming_txs += 1
                self.ttl_eth_recv += int(transaction.get("value"))
                self.ttl_usd_recv += int(transaction.get("value")) * price

            elif transaction.get("from") == self.address and is_success:
                self.outgoing_txs += 1
                self.ttl_eth_sent += int(transaction.get("value"))
                self.ttl_usd_sent += int(transaction.get("value")) * price


        # ========== Post Loop ==========

        self.fail_success_txs_ratio = self.failed_txs / float(self.successful_txs) if self.failed_txs > 0 else 0


        # ========== Cleanup ==========
        self.__validateRun(_type)
        debug_print("finished getting %s" % _type)

        return True

    def __generateAverages(self):
        if not (self.has_inner_run and self.has_external_run):
            raise IOError("Need to run inner and external to be called first.")

        self.__determineMonthNumber()

        ttl_trans = float(self.successful_txs + self.failed_txs)

        ttl_eth_vol = self.ttl_eth_sent + self.ttl_eth_recv

        if ttl_trans > 0:
            self.avg_eth_sent = self.ttl_eth_sent / ttl_trans
            self.avg_usd_sent = self.ttl_usd_sent / ttl_trans
            self.avg_eth_recv = self.ttl_eth_recv / ttl_trans
            self.avg_usd_recv = self.ttl_usd_recv / ttl_trans
            self.avg_fee_sent = self.ttl_fee_sent / ttl_trans

        else:
            pass

        if self.active_months > 0:
            self.monthly_eth_sent  = self.ttl_eth_sent / self.active_months
            self.monthly_usd_sent   = self.ttl_usd_sent / self.active_months
            self.monthly_eth_recv = self.ttl_eth_recv / self.active_months
            self.monthly_usd_recv = self.ttl_usd_recv / self.active_months
            self.monthly_out_txs = self.outgoing_txs / self.active_months
            self.monthly_incoming_txs = self.incoming_txs / self.active_months

        self.__determineTokenAmount()


    def createTransactionVariables(self):
        """
        This fucking function is going to be kind of nutso,
        there are probably better ways of doing this but
        I want explict and self-validating structures popping
        out of this thing while only looping through the structures once
        """
        debug_print("creating tx_list vars")
        self.__createTransactionVariables("tx_list")

        debug_print("creating internal tx_list vars")
        self.__createTransactionVariables("internal_tx_list")

        debug_print("creating generate averages")
        self.__generateAverages()

        debug_print("creating time diffs")
        self.__determineTimeDiffs()


    def assembleStruct(self):
        return {
          "address" : self.address,
          "outgoing_txs" : self.outgoing_txs,
          "incoming_txs" : self.incoming_txs,
          "ttl_eth_sent" : self.ttl_eth_sent,
          "avg_eth_sent" : self.avg_eth_sent,
          "ttl_usd_sent" : self.ttl_usd_sent,
          "avg_usd_sent" : self.avg_usd_sent,
          "ttl_eth_recv" : self.ttl_eth_recv,
          "avg_eth_recv" : self.avg_eth_recv,
          "ttl_usd_recv" : self.ttl_usd_recv,
          "avg_usd_recv" : self.avg_usd_recv,
          "active_months": self.active_months,
          "monthly_out_txs" : self.monthly_out_txs,
          "monthly_incoming_txs" : self.monthly_incoming_txs,
          "monthly_eth_sent" : self.monthly_eth_sent,
          "monthly_usd_sent" : self.monthly_usd_sent,
          "monthly_eth_recv" : self.monthly_eth_recv,
          "monthly_usd_recv" : self.monthly_usd_recv,
          "contracts_created": self.contracts_created,
          "contract_txs_sent": self.contract_txs_sent,
          "incoming_avg_txs_time" : self.incoming_avg_txs_time,
          "incoming_std_txs_time" : self.incoming_std_txs_time,
          "num_tokens_used" : self.num_tokens_used,
          "eth_balance" : self.eth_balance,
          "ttl_fee_sent" : self.ttl_fee_sent,
          "avg_fee_sent" : self.avg_fee_sent,
          "num_internal_txs" : self.num_internal_txs,
          "num_external_txs" : self.num_external_txs,
          "external_internal_ratio" : self.external_internal_ratio,
          "failed_txs" : self.failed_txs,
          "successful_txs" : self.successful_txs,
          "fail_success_txs_ratio" : self.fail_success_txs_ratio,
          "mined_blocks" : len(self.mined_blocks)
        }

    def dump(self):
        self.createTransactionVariables()
        return self.assembleStruct()



class StaticClassifier(object):
    def __init__(self, _db_name):
        self.db_name = _db_name

    def makeGetRequestToAPI(self, _ep):
        resp = requests.get(_ep)
        return resp.json()

    def parseResponseForInsert(self,_response_json):
        pass

    def getAddressBalance(self,_addr):
        ep = ETHERSCAN_API.format(action = "balance", addr = _addr, api_token = ETHERSCAN_KEY_0)
        resp = self.makeGetRequestToAPI(ep)
        return {"eth_bal": resp.get("result")}

    def getNormalTransactions(self,_addr):
        ep = ETHERSCAN_API.format(action = "txlist", addr = _addr, api_token = ETHERSCAN_KEY_1)
        resp = self.makeGetRequestToAPI(ep)
        return {"tx_list" : resp.get("result")}

    def getInternalTransactions(self, _addr):
        ep = ETHERSCAN_API.format(action = "txlistinternal", addr = _addr, api_token = ETHERSCAN_KEY_2)
        resp = self.makeGetRequestToAPI(ep)
        return {"internal_tx_list" : resp.get("result")}

    def getTokenTransactions(self, _addr):
        ep = ETHERSCAN_API.format(action = "tokentx", addr = _addr, api_token = ETHERSCAN_KEY_0)
        resp = self.makeGetRequestToAPI(ep)
        return {"token_tx_list": resp.get("result")}

    def getMinedBlocks(self, _addr):
        ep = ETHERSCAN_API.format(action = "getminedblocks", addr = _addr, api_token = ETHERSCAN_KEY_1)
        resp = self.makeGetRequestToAPI(ep)
        return {"mined_blocks": resp.get("result")}

    def pullTokenListFromDB(self):
        self.my_db = mysql.connector.connect(
          host = "ec2-3-84-247-12.compute-1.amazonaws.com",
          port = 8899,
          user = "proofAdmin",
          passwd = "Whiskey1031!",
          database = self.db_name
        )

        self.my_cursor = self.my_db.cursor()

        self.my_cursor.execute("SELECT * FROM ethereum;")
        return [x[1] for x in self.my_cursor] #SAMPLE: (5992, '0x0bb4251b8c5e8acb27d6809d77a10aa1cacc5144', 1, 'scam', 'Exchange', 'gdax.us', None, None, None, None, None, None, None, None, 'Fake exchange (same as your-btc.co.uk)')

    def createAndValidateInfoDict(self, _addr):
        insert_struct = {}

        insert_struct.update(self.getAddressBalance(_addr))
        insert_struct.update(self.getNormalTransactions(_addr))
        insert_struct.update(self.getInternalTransactions(_addr))
        insert_struct.update(self.getTokenTransactions(_addr))
        insert_struct.update(self.getMinedBlocks(_addr))
        insert_struct.update({"addr" : _addr})

        return insert_struct

    def createInsertStruct(self, _addr_struct):
        StructCreator = InsertStructCreator(_addr_struct)
        return StructCreator.dump()

    def insertOneStructIntoDB(self, _insert_struct):
        self.my_db = mysql.connector.connect(
          host = "ec2-3-84-247-12.compute-1.amazonaws.com",
          port = 8899,
          user = "proofAdmin",
          passwd = "Whiskey1031!",
          database = self.db_name
        )

        self.my_cursor = self.my_db.cursor()

        insert_query = INSERT_FRAME.format(**_insert_struct)
        debug_print("insert_query %s" % insert_query)
        self.my_cursor.execute(insert_query)
        self.my_db.commit()

    def run(self):
        debug_print("Running: getting token List")
        account_list = self.pullTokenListFromDB()
        debug_print("list got!  Has %s entries" % len(account_list))

        for account in account_list:
            debug_print("...on account %s" % account)

            addr_struct = self.createAndValidateInfoDict(account)
            debug_print("got address structs")

            insert_struct = self.createInsertStruct(addr_struct)
            self.insertOneStructIntoDB(insert_struct)
        print("Finished")

SC = StaticClassifier("important_addresses")
SC.run()
