from pylibs.staticdata.checker.book_checker import BookChecker
from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.checker.log_changes import LogChanges
from pylibs.staticdata.checker.log_types import LogTypes
from pylibs.staticdata.generator.runner import runner as runner2
from datetime import datetime
from pprint import pprint
import json
from pathlib import Path
import glob, time
import os

def runner():

    lg = LogChanges()
    home = str(Path.home())
    time_date_stamp = time.strftime("%Y%m%d_%H%M%S")

    # static_filename = home + '/Static/StaticData/static_file_' + time_date_stamp + '.json'

    exchanges_to_update = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 16, 17, 18,
        19, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33,
        34, 35, 36, 37, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48,
        49, 50, 51, 52, 53, 54, 55
        ]

    #refuse all changes
    # um.auto_refuse_all_unprocessed_changes_in_db()

    #map new and latest data from db and run_generator
    # data_old = um.load_static_from_db_by_id()
    data_old = load_latest_static()
    data_new = runner2(exchanges_to_update)

    checks = BookChecker(data_old, data_new)

    # Check missing books
    print(str(datetime.utcnow().isoformat()))
    missing_books = checks.check_missing_books()
    print('Missing books:', str(missing_books.keys()))
    lg.log_missing_books(missing_books)

    # Check added books
    added_books = checks.check_added_books()
    print('Added books:', str(added_books.keys()))
    lg.log_added_books(added_books)

    # Check added book_fields
    added_book_fields = checks.check_added_book_fields()
    print('Added book_fields:', added_book_fields.keys())
    #pprint(added_book_fields)
    lg.log_added_book_fields(added_book_fields)

    # Substracted book_fields
    substracted_book_fields = checks.check_substracted_book_fields(exception_key='_id')
    print('Substracted book_fields:', substracted_book_fields.keys())
    #pprint(substracted_book_fields)
    lg.log_substracted_book_fields(substracted_book_fields)

    # Modified book_fields
    modified_book_field = checks.check_modified_book_fields()
    print('Modified book_fields:')
    print(modified_book_field.keys())
    lg.log_modified_book_fields(modified_book_field)

    # Extra statics
    extra_statics = checks.check_all_extra_statics_changes()
    print('Extra statics:')
    pprint(extra_statics)
    lg.log_extra_statics(extra_statics)

    print(str(datetime.utcnow().isoformat()))

    # static_dump = um.construct_static_data_for_push(data_new)
    # lg.dump_log(log_filename)
    # Dumps the logs of the checker in a file
    lg.dump_formatted_logs()

    # Dump the staticdata into the db. Dumps in a file instead
    # um.dump_staticdata(static_dump, static_filename)


def load_latest_static():
    latest_staticData = json.load(open(get_path_latest_static_file(), "r"))
    return latest_staticData

def get_path_latest_static_file():
    try:
        home = str(Path.home())
        folder_path = os.path.join(home, 'Static', 'StaticData')
        file_type = '/*json'
        return str(max(glob.iglob(folder_path + file_type), key=os.path.getctime))
    except ValueError:
        pass
    except TypeError:
        pass

def main(*args):
    ''' This is the main runner to feed persist the data on a set of days for a given config
    @:param args should contain at least one date, one config number and one s3_region
    '''

    args = list(*args)

    arg_count = len(args)
    runner()