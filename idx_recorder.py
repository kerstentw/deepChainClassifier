import os
import json
# Global State Managers

TARGET_FIL = "{chain_id}_last_block.json"
RECORD_DIR = "./cache"


class LocalIndexer(object):
    def __doc__(self):
        return """
          This Module logs and retrieves JSON Objects from the local file system
          The default Recordation directory is <module_root>/cache.

          Utilizes a pretty standard getter/setter around a file object
        """

    def doesFileExist(self, _target):
        fil_list = os.listdir(RECORD_DIR)

        return True if _target in fil_list else False

    def __init__(self, _label):
        self.target = TARGET_FIL.format(chain_id = _label)
        self.full_path = os.path.join(RECORD_DIR, self.target)
        self.fil_mode = "r" if self.doesFileExist(self.target) else "w"

        self.files_contents = json.loads(open(self.full_path, self.fil_mode).read()) if self.fil_mode == "r" else {}

    def getRecord(self):
        return self.files_contents

    def newRecord(self, _dict_content):
        self.fil_mode = "w"

        with open(self.full_path, self.fil_mode) as my_fil:
            my_fil.write(json.dumps(_dict_content))

        with open(self.full_path, "r") as my_fil:
            self.files_contents = json.loads(my_fil.read())
