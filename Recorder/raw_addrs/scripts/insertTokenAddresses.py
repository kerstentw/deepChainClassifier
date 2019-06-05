import sys
import os
import mysql.connector
import yaml
import json
import urllib.parse

SAMPLE = {'addr': '0xffe8196bc259e8dedc544d935786aa4709ec3e64',
          'decimals': 18,
          'description': 'HDG is a utility token (for trading fees payment and buyback) powering our upcoming platform. Our main selling point are Crypto Traded Indices - crypto ETF equivalents (tradable index funds) and indexed derivatives.',
          'links': [{'Bitcointalk': 'https://bitcointalk.org/index.php?topic=2142867'}, {'Blog': 'https://medium.com/@hedgetoken'}, {'CoinMarketCap': 'hedge'}, {'Email': 'mailto:info@hedge-crypto.com'}, {'Facebook': 'https://www.facebook.com/hedgetoken/'}, {'Github': 'https://github.com/HedgeToken/Hedge-Token'}, {'Reddit': 'https://www.reddit.com/r/HedgeToken/'}, {'Telegram': 'https://t.me/joinchat/F5fglQ7DVPV2wi4JK8Mw9A'}, {'Twitter': 'https://twitter.com/hedgetoken'}, {'Website': 'https://www.hedge-crypto.com/'}, {'Website': 'https://www.hedge-crypto.com'}, {'Whitepaper': 'https://www.hedge-crypto.com/wp-content/uploads/2017/08/Hedgetoken_Whitepaper_2017.pdf'}],
           'name': 'Hedge',
           'symbol': 'HDG'}

GLOBAL_CHAIN_ID = 1

DB = "important_addresses"
INSERT_FRAME = "INSERT INTO ethereum (`address`, `chain_id`, `label`, `tag_1`, `tag_2`, `notes`) VALUES ('%s',%s,'%s','%s','%s','%s');"

db_conn = mysql.connector.connect(
  host = "ec2-3-84-247-12.compute-1.amazonaws.com",
  port = 8899,
  user = "proofAdmin",
  passwd = "Whiskey1031!",
  database = DB
)

db_connection = db_conn.cursor()

YAMLS_PATH = "../ethereum/ercTokens"
tokens_list = os.listdir(YAMLS_PATH)

def insertToDatabase(token_struct):
    tag_one = urllib.parse.quote(token_struct.get("symbol").encode("unicode-escape")) if token_struct.get("symbol").isalnum() else token_struct.get("name")
    tag_two = token_struct.get("name")
    label = "token"
    notes = urllib.parse.quote(json.dumps(token_struct))
    address = token_struct.get("addr")
    chain_id = GLOBAL_CHAIN_ID

    insert_query = INSERT_FRAME % (address, chain_id, label, tag_one, tag_two, notes)
    print("QUERY::: %s" % insert_query)

    db_connection.execute(insert_query)
    db_conn.commit()


for token in tokens_list[370:]:
    base = os.path.join(YAMLS_PATH, token)
    with open(base, "r") as my_token:
        token_struct = yaml.load(my_token)
        insertToDatabase(token_struct)
