import openpyxl
import json
import os
from pathlib import Path

def excel_to_dict(excel_path, sheet_name, headers=[]):
    def _index_to_col(index):
        return chr(ord('@') + index)

    wb = openpyxl.load_workbook(excel_path)
    sheet = wb.get_sheet_by_name(sheet_name)
    result_dict = {}
    for row in range(2, sheet.max_row +1):
        line = dict()
        for header in headers:
            cell_value = sheet[_index_to_col(headers.index(header)+1) + str(row)].value
            if isinstance(cell_value, str):
                cell_value = cell_value.encode('utf-8').decode('ascii', 'ignore')
                cell_value = cell_value.strip()
            elif type(cell_value) is int:
                cell_value = str(cell_value)
            elif cell_value is None:
                cell_value = ''
            # if header =='exchange':
            #     continue
            line[header] = cell_value
        result_dict[line['exchange']] = {k:v for k,v in line.items() if k != 'exchange'}
    return result_dict

def convert_api_param_to_json():
    dict_to_dump = excel_to_dict('/home/vic/pylibs/SystematicCeFi/DerivativeArbitrage/Runtime/configs/static_params.xlsx', 'api', ['exchange', 'key', 'comment'])
    with open(os.path.join(Path.home(), '.cache', 'setyvault', 'api_keys.json'), 'w') as fp:
        json.dump(dict_to_dump, fp, indent=4)

