import json, os
from  constants import *
import datetime
with open(os.path.join(os.getcwd(),"data","data.json"),"r") as f:
    data=json.load(f)

def formatData(data):
    new_data=[]
    for ts in data:
        row=data[ts]
        ts=int(ts)
        date=datetime.datetime.fromtimestamp(ts)
        new_row = {
            "timestamp":ts,
            "date":str(date),
            "glp_price":row["glp_price"],
            }
        for add in row["weights"]:
            token = add2name[add]
            new_row[token+"_weight"]=row["weights"][add]
        for add in row["prices"]:
            token = add2name[add]
            new_row[token+"_price"]=row["prices"][add]
        for token in row["liquidations"]:
            new_row[token+"_liq"]=row["liquidations"][token]
        for token in row["targets"]:
            new_row[token+"_targets"]=row["targets"][token]
        for token in row["current"]:
            new_row[token+"_current"]=row["current"][token]
        for token in row["update"]:
            new_row[token+"_update"]=row["update"][token]
        new_data.append(new_row)
    return new_data
def formatDataLong(data):
    # each observation has its row
    new_data=[]
    for ts in data:
        row=data[ts]
        ts=int(ts)
        date=datetime.datetime.fromtimestamp(ts)
        new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"price",
            "token":"glp",
            "value":row["glp_price"]
            }
        new_data.append(new_row)
        for add in row["weights"]:
            token = add2name[add]
            new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"weight",
            "token":token,
            "value":row["weights"][add]
            }
            new_data.append(new_row)

        for add in row["prices"]:
            token = add2name[add]
            new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"price",
            "token":token,
            "value":row["prices"][add]
            }
            new_data.append(new_row)

        for token in row["liquidations"]:
            new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"liquidationPrice",
            "token":token,
            "value":row["liquidations"][token]
            }
            new_data.append(new_row)
        for token in row["targets"]:
            new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"target",
            "token":token,
            "value":row["targets"][token]
            }
            new_data.append(new_row)        
        for token in row["current"]:
            new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"current",
            "token":token,
            "value":row["current"][token]
            }
            new_data.append(new_row)          
        for token in row["update"]:
            new_row = {
            "timestamp":ts,
            "date":str(date),
            "kind":"update",
            "token":token,
            "value":row["update"][token]
            }
            new_data.append(new_row)          
    return new_data