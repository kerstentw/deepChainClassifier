import json
import db_info
import requests
import mysql.connector

SAMPLE = """
'0x0bb4251b8c5e8acb27d6809d77a10aa1cacc5144':
    {
      'id': 6604,
      'name': 'gdax.us',
      'url': 'http://gdax.us',
      'coin': 'ETH',
      'category': 'Scamming',
      'subcategory': 'Exchange',
      'description': 'Fake exchange (same as your-btc.co.uk)',
      'addresses': ['1gj9tymgjcfsnjprkdcaugemewix2ysbdl',
                    '0x0bb4251b8c5e8acb27d6809d77a10aa1cacc5144',
                    'qzh7l8neh3v868uzu74qpyat4gjz2rdv0yhjktqcp2'],
      'reporter': 'MyCrypto',
      'ip': '217.23.6.10',
      'nameservers': ['1a7ea920.bitcoin-dns.hosting',
                      'c358ea2d.bitcoin-dns.hosting',
                      'a8332f3a.bitcoin-dns.hosting',
                      'ad636824.bitcoin-dns.hosting'],
      'status': 'Offline'}

      """

DB = "important_addresses"

db_conn = mysql.connector.connect(
  host = "ec2-3-84-247-12.compute-1.amazonaws.com",
  port = 8899,
  user = "proofAdmin",
  passwd = "Whiskey1031!",
  database = DB
)

db_cursor = db_conn.cursor()

API_EP = "https://etherscamdb.info/api/addresses/"
SOURCE = "../ethereum/scams/etherscamdb/scams.json"

LABEL = "scam"


def updateSource():
    pass

def determineIfETH(_addr):
    return True if _addr.startswith("0x") else False

def createQueryString(_addr, _scam_struct):
    insert_struct = {
      "address" : _addr,
      "chain_id" : 1,
      "label" : "scam",
      "tag_1" : _scam_struct.get("subcategory"),
      "tag_2" : _scam_struct.get("name"),
      "notes" : _scam_struct.get("description").replace('\'','"') if _scam_struct.get("description") else None
    }

    return db_info.ETH_INSERT_FRAME.format(**insert_struct)

def main():
    with open(SOURCE, "r") as my_fil:
        scam_table = json.loads(my_fil.read()).get("result")
        scam_list = scam_table.keys()

    for address in scam_list:
        if not determineIfETH(address):
            continue

        scam_struct = scam_table[address]
        q_string = createQueryString(address, scam_struct)

        db_cursor.execute(q_string)
        db_conn.commit()

main()
