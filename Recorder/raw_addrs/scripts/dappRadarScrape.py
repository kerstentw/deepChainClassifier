import requests
import json
import mysql.connector
import db_info
import urllib.parse

API_LISTING_ENDPOINT = "https://dappradar.com/api/xchain/dapps/theRest"
API_CONTRACT_ENDPOINT = "https://dappradar.com/api/dapp/{id_num}"
DB = "important_addresses"
LABEL = "dapp"
CHAIN_ID = 1
 #if eth in protocols
this_db = mysql.connector.connect(**db_info.createDatabaseParams(DB))
db_cursor = this_db.cursor()

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

def getDappList():
    resp = requests.get(API_LISTING_ENDPOINT, headers = headers)
    print (resp.content)
    return resp.json()

def getDappInfo(_id):
    resp = requests.get(API_CONTRACT_ENDPOINT.format(id_num=_id), headers = headers)
    print (resp.content)
    return resp.json()

def filterDappListForEthOnly(_dapp_list):
    _dapp_list = _dapp_list.get("data").get("list")
    return [dapp for dapp in _dapp_list if "eth" in dapp.get("protocols")]

def filterDappListStructForID(_dapp_struct):
    return _dapp_struct.get("id")

def filterDappInfoForContractInfo(_dapp_info_struct):
    return _dapp_info_struct.get("data").get("contracts") if _dapp_info_struct.get("data") else None

def formatInsertString(address, chain_id, label, tag_1, tag_2, notes):
    return """
            INSERT INTO ethereum (`address`, `chain_id`, `label`, `tag_1`, `tag_2`, `notes`)
            VALUES ('%s',%s,'%s','%s','%s','%s');
          """ % (address, chain_id, label, tag_1, tag_2, notes)



def submitToDB(_insert_string):
    db_cursor.execute(_insert_string)
    this_db.commit()

def insertAddressesFromListIntoDB(_contract_array, _info):
    for contract in _contract_array:
        insert_struct = {
          "address" : contract.get("address"),
          "chain_id" : CHAIN_ID,
          "label" : LABEL,
          "tag_1" : _info.get("category"),
          "tag_2" : contract.get("title"),
          "notes" : urllib.parse.quote(json.dumps(_info))
        }

        insert_string = formatInsertString(**insert_struct)

        submitToDB(insert_string)


def main():
    id_list = [filterDappListStructForID(i) for i in filterDappListForEthOnly(getDappList())]
    for _id in id_list:
        dapp_info = getDappInfo(_id)
        contract_data = dapp_info.get("data")

        if not contract_data:
            pass

        insertAddressesFromListIntoDB(contract_data.get("contracts"), contract_data.get("info"))

main()
