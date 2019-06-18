from web3 import Web3
from db import MongoHandler
from idx_recorder import LocalIndexer
import time

DB_URI = "localhost"
DB_PORT = 27017
DB_USER = ""
DB_PASS = ""

MAX_THREADS = 5

PROVIDER = Web3.HTTPProvider("https://mainnet.infura.io/v3/d2bda2c2e7d0463ab1dd077566fb2e3f")

SAMPLE_OUTPUT = """
AttributeDict({'difficulty': 17179869184, 'extraData': HexBytes('0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa'), 'gasLimit': 5000, 'gasUsed': 0, 'hash': HexBytes('0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3'), 'logsBloom': HexBytes('0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'miner': '0x0000000000000000000000000000000000000000', 'mixHash': HexBytes('0x0000000000000000000000000000000000000000000000000000000000000000'), 'nonce': HexBytes('0x0000000000000042'), 'number': 0, 'parentHash': HexBytes('0x0000000000000000000000000000000000000000000000000000000000000000'), 'receiptsRoot': HexBytes('0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'), 'sha3Uncles': HexBytes('0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347'), 'size': 540, 'stateRoot': HexBytes('0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544'), 'timestamp': 0, 'totalDifficulty': 17179869184, 'transactions': [], 'transactionsRoot': HexBytes('0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'), 'uncles': []})
"""

class EthereumCrawler(object):

    def __init__(self, _mode, _name="ethereum_store", _provider = PROVIDER):
        self.name = _name
        self.db_handler = MongoHandler(_name, _uri = DB_URI, _port = DB_PORT, _user = DB_USER, _pw=DB_PASS)
        self.web3 = Web3(PROVIDER)
        self.init_height = self.web3.eth.blockNumber
        self.indexer = LocalIndexer(_name)
        self.mode = _mode

    def getLatestHeight(self):
        self.init_height = self.web3.eth.blockNumber

    def filterForHex(self, _field, _key = ""):
        try:
            return _field.hex()
        except:
            return str(_field) if not _key == "number" else _field

    def filterReceivedBlock(self, _block):
        if 'difficulty' not in list(_block.keys()):
            return None

        return {k : self.filterForHex(v, k) for k,v in _block.items()}

    def grabBlockFromChain(self, _block_num):
        return self.web3.eth.getBlock(_block_num) or None

    def insertBlockIntoDB(self, _filt_block_struct):
        return self.db_handler.insertOne(self.mode, _filt_block_struct)

    def getHistoricals(self):
        ref = self.indexer.getRecord().get("last_block") if self.indexer.getRecord() else 0
        end = int(self.web3.eth.blockNumber)

        for _i in range(ref, end):
            print("GRABBING_BLOCK: %s" % _i)
            raw_block = self.grabBlockFromChain(_i)
            print("Block %s Received ::: \n %s" % (_i, raw_block))

            insert_id = self.insertBlockIntoDB(self.filterReceivedBlock(raw_block)).inserted_id if raw_block else None

            print("INSERTED TO: %s" % insert_id)
            self.indexer.newRecord({"last_block" : _i, "insert_id" : str(insert_id)})

        self.indexer.newRecord({"last_block":end})


    def runOngoingRecentBlocksScrape(self):
        prev_block = None
        cur_block = None

        while True:
            cur_block = self.getLatestHeight()

            if prev_block == cur_block:
                time.sleep(1)
                continue

            block = self.grabBlockFromChain(cur_block)

            if not block: continue
            self.insertBlockIntoDB(self.filterReceivedBlock(block))

            prev_block = cur_block

def runHist(Crawler):
    Crawler.getHistoricals()

def runOngoing(Crawler):
    Crawler.runOngoingRecentBlocksScrape()

if __name__ == "__main__":
    ECrawler = EthereumCrawler("blocks")
    runHist(ECrawler)
    runOngoing(ECrawler)
