import mysql.connector

CREATE_TABLE_STRING = """
CREATE TABLE IF NOT EXISTS {table_name} ({comma_joined_field_names})ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

INSERT_STRING = """
INSERT INTO {table_name} ({comma_joined_field_names}) VALUES ({comma_joined_values});
"""

FORMS = {
  int: "TEXT",
  list: "TEXT",
  str: "TEXT",
  float: "TEXT"
}

def filterForms(_type):
    return FORMS.get(_type)

class MySQLBasicInserter(object):
    def __init__(self, _db_name, _db_uri, _port, _db_user = "", _db_password = ""):
        self.db = mysql.connector.connect(
          host = _db_uri,
          port = _port,
          user = _db_user,
          passwd = _db_password,
          database = _db_name
        )
        self.cursor = self.db.cursor()

    @classmethod
    def createTableArrayFromDict(self,_insert_dictionary, _pk = "", _create_id_pk = True, _special_fields = None):
        shallow_dict = _insert_dictionary.copy()
        return_array = [(k,filterForms(type(v))) for k,v in shallow_dict.items()]

        if _create_id_pk:
            return_array.extend([("id", "INT UNIQUE AUTO_INCREMENT"), ("","PRIMARY KEY(id)")])

        if _special_fields and len(_special_fields) > 0:
            return_array.extend(_special_fields)

        return return_array


    @classmethod
    def createTableQueryString(self, _table_name, _array):
        """
        Sample Array = [("id", "INT"), ("name", "VARCHAR(123)")]
        """

        field_names = ",".join(["%s %s"% (_key, _type) for _key,_type in _array])
        print("FIELD NAMES: %s" % field_names)

        return CREATE_TABLE_STRING.format(table_name = _table_name, comma_joined_field_names = field_names)

    @classmethod
    def createInsertStringFromDictionary(self, _table_name, _insert_dictionary):
        dict_items = _insert_dictionary.items()
        key_insert = ",".join([str(item[0]) for item in dict_items])
        values_insert = ",".join(['"%s"' % str(item[1]) for item in dict_items])

        return INSERT_STRING.format(table_name = _table_name, comma_joined_field_names = key_insert, comma_joined_values = values_insert)

    def createTable(self, _table_name, _array):
        q_string = self.createTableQueryString(_table_name, _array)
        self.cursor.execute(q_string)
        self.db.commit()


    def insertIntoTable(self, _table_name, _insert_dictionary):
        q_string = self.createInsertStringFromDictionary(_table_name, _insert_dictionary)
        print(q_string)
        stat = self.cursor.execute(q_string)
        self.db.commit()
