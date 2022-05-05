from pylibs.staticdata.checker.log_changes import  LogChanges
from pylibs.staticdata.checker.update_static_in_db import UpdateStaticInDB
from pathlib import Path
import json
home = str(Path.home())
#lg = LogChanges()
um = UpdateStaticInDB()
#filepath = um.get_path_latest_static_in_portoshared()
#clean_data_new = um.load_latest_json(filepath)

#{_id:12308}
um.pull_all_unprocessed_changes()
#um.remove_current_static_from_db(True, True, True, True)
#um.remove_changes_history_from_db()

#um.push_latest_static_file_into_db(clean_data_new, True, True, True, True)


# um2 = UpdateStaticInDB()

#dump = um2.load_static_from_db_with_filers({'active': True})
#um2.dump_staticdata_from_db(dump)
