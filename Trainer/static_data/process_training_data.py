import requests
import mysql.connector
import time

ETHERSCAN_KEY_0 = "WDDC73CFRBBFBX6E2A7ZPG48SHQJZJ3DKJ"
ETHERSCAN_KEY_1 = "EN8N9CKSDH6Z3AW5XEGKJ3PHBJNGNBHB7Q"
ETHERSCAN_KEY_2 = "WDDC73CFRBBFBX6E2A7ZPG48SHQJZJ3DKJ"

ETHERSCAN_API = "https://api.etherscan.io/api?module=account&action={action}&address={addr}&tag=latest&apikey={api_token}&offset=999999999"



class InsertStructCreator(object):
    def __init__(self, _addr_data):
        self.addr_data = _addr_data

    def handle(self):
        pass

    def assembleStruct(self):
        return {
        "address" : self.addr_data.get("addr"),
        "outgoing_txs",
        "incoming_txs",
        "ttl_eth_sent",
        "avg_eth_sent",
        "ttl_usd_sent",
        "avg_usd_sent",
        "ttl_eth_recv",
        "avg_eth_recv",
        "ttl_usd_recv",
        "avg_usd_recv",
        "active_months",
        "monthly_out_txs",
        "monthly_incoming_txs",
        "monthly_eth_sent",
        "monthly_usd_sent",
        "monthly_eth_recv",
        "monthly_usd_recv",
        "contracts_created",
        "contract_txs_sent",
        "incoming_avg_txs_time",
        "incoming_std_txs_time",
        "num_tokens_used",
        "eth_balance" : self.addr_data.get("eth_bal"),
        "ttl_fee_sent",
        "avg_fee_sent",
        "num_internal_txs" : len(self.addr_data.get("internal_tx_list")),
        "num_external_txs" : len(self.addr_data.get("tx_list")),
        "external_internal_ratio",
        "failed_txs",
        "successful_txs",
        "fail_success_txs_ratio",
        "mined_blocks" : self.addr_data.get("mined_blocks")
        }

    def dump(self):
        pass



class StaticClassifier(object):
    def __init__(self, _db_name):
        self.my_db = mysql.connector.connect(
          host = "ec2-3-84-247-12.compute-1.amazonaws.com",
          port = 8899,
          user = "proofAdmin",
          passwd = "Whiskey1031!",
          database = _db_name
        )

        self.my_cursor = self.my_db.cursor()
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
        self.my_cursor.execute("SELECT * FROM ethereum;")
        return [x[1] for x in self.my_cursor] #SAMPLE: (5992, '0x0bb4251b8c5e8acb27d6809d77a10aa1cacc5144', 1, 'scam', 'Exchange', 'gdax.us', None, None, None, None, None, None, None, None, 'Fake exchange (same as your-btc.co.uk)')

    def createAndValidateInfoDict(self, _addr):
        insert_struct = {}

        insert_struct.update(self.getAddressBalance())
        insert_struct.update(self.getNormalTransactions())
        insert_struct.update(self.getInternalTransactions())
        insert_struct.update(self.getTokenTransactions())
        insert_struct.update(self.getMinedBlocks())
        insert_struct.update({"addr" : _addr})

        return insert_struct

    def createInsertStruct(self, _addr_struct):
        StructCreator = InsertStructCreator(_addr_struct)
        return StructCreator.dump()

SC = StaticClassifier("important_addresses")
SC.pullTokenListFromDB()
