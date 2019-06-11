from web3 import Web3
from db import MongoHandler
from idx_recorder import LocalIndexer
import time

DB_URI = "ec2-3-84-247-12.compute-1.amazonaws.com"
DB_PORT = 6677
DB = "ethereum_store"
main_collection = "transactions"
collection_2 = "address_mappings"

DB_USER = "proofAdmin"
DB_PASS = "Whiskey1031!"

MAX_THREADS = 5

PROVIDER = Web3.HTTPProvider("https://mainnet.infura.io/v3/f72e74c2859345519365c084c67fa49b")

SAMPLE_OUTPUT = """
AttributeDict({
    'blockHash': '0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd',
    'blockNumber': 46147,
    'from': '0xa1e4380a3b1f749673e270229993ee55f35663b4',
    'gas': 21000,
    'gasPrice': 50000000000000,
    'hash': '0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060',
    'input': '0x',
    'nonce': 0,
    'to': '0x5df9b87991262f6ba471f09758cde1c0fc1de734',
    'transactionIndex': 0,
    'value': 31337,
})"""

SAMPLE_OUTPUT_2 = """
AttributeDict({'difficulty': 17179869184, 'extraData': HexBytes('0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa'), 'gasLimit': 5000, 'gasUsed': 0, 'hash': HexBytes('0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3'), 'logsBloom': HexBytes('0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'), 'miner': '0x0000000000000000000000000000000000000000', 'mixHash': HexBytes('0x0000000000000000000000000000000000000000000000000000000000000000'), 'nonce': HexBytes('0x0000000000000042'), 'number': 0, 'parentHash': HexBytes('0x0000000000000000000000000000000000000000000000000000000000000000'), 'receiptsRoot': HexBytes('0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'), 'sha3Uncles': HexBytes('0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347'), 'size': 540, 'stateRoot': HexBytes('0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544'), 'timestamp': 0, 'totalDifficulty': 17179869184, 'transactions': [], 'transactionsRoot': HexBytes('0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'), 'uncles': []})
"""

class EthTransactionCrawler(object):

    def __init__(self, _mode, _name="ethereum_store", _provider = PROVIDER):
        self.name = _name #DB name
        self.db_handler = MongoHandler(_name, _uri = DB_URI, _port = DB_PORT, _user = DB_USER, _pw=DB_PASS)
        self.web3 = Web3(PROVIDER)
        self.init_height = self.web3.eth.blockNumber
        self.indexer = LocalIndexer(_mode)
        self.mode = _mode # Primary Collection

    def dictionarifyAndStringifyBlockStruct(self, _block_attr_dict):
        block_dict = _block_attr_dict #TODO: Create Field Validator
        print(block_dict)
        for key in block_dict.keys():
            try:
                block_dict[key] = block_dict[key].hex()
            except:
                pass

            try:
                block_dict[key] = [i.hex() for i in block_dict[key]]
            except:
                pass

            if key == "timestamp":
                pass
            elif type(block_dict[key]) == int:
                block_dict[key] = str(block_dict[key])
            else:
                pass

        return block_dict


    def getTransactions(self, _block_dict):
        transaction_array = _block_dict.get("transactions")
        if not transaction_array:
            return False

        transaction_info_array = [self.dictionarifyAndStringifyBlockStruct(dict(self.web3.eth.getTransaction(transaction))) for transaction in transaction_array]

        return transaction_info_array


    def recordTransactionMap(self, _block_dict):
        transaction_infos = self.getTransactions(_block_dict)
        # Ensure transactions exist
        if not transaction_infos: return False
        db_handler.refreshClient()
        insert_id = self.db_handler.insertMany(self.mode, transaction_infos)
        return insert_id

    def getMasterBlockList(self):
        block_iterator = self.db_handler.pullFullCollection("blocks")
        return block_iterator

    def setIndexer(self):
        ref = self.indexer.getRecord().get("last_transaction") if self.indexer.getRecord() else 0
        if not ref:
            self.indexer.newRecord({'last_transaction' : 0})
            return 0

        else:
            return ref

    def run(self):
        iterator = self.getMasterBlockList()
        cur_ref = self.setIndexer()
        idx = cur_ref

        for block_struct in iterator:
            if block_struct.get("number") < cur_ref:
                continue

            block_dict = self.dictionarifyAndStringifyBlockStruct(block_struct)
            insert_id = self.recordTransactionMap(block_dict)
            if not insert_id: pass

            idx+=1
            self.indexer.newRecord({'last_transaction' : idx})

            print(block_dict)



if __name__ == "__main__":
    ETC = EthTransactionCrawler(main_collection)
    ETC.run()
