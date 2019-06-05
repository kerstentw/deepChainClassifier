
def createDatabaseParams(_db):
    return {
      "host" : "ec2-3-84-247-12.compute-1.amazonaws.com",
      "port" : 8899,
      "user" : "proofAdmin",
      "passwd" : "Whiskey1031!",
      "database" : _db
    }


DB = "important_addresses"


BTC_INSERT_FRAME = "INSERT INTO bitcoin (`address`, `chain_id`, `label`, `tag_1`, `tag_2`, `notes`) VALUES ('{address}',{chain_id},'{label}','{tag_1}','{tag_2}','{notes}');"
ETH_INSERT_FRAME = "INSERT INTO ethereum (`address`, `chain_id`, `label`, `tag_1`, `tag_2`, `notes`) VALUES ('{address}',{chain_id},'{label}','{tag_1}','{tag_2}','{notes}');"
