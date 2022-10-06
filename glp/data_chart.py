import json
from  constants import *
from data_parse import *
import datetime
import pandas as pd
# testing some viz lib
import seaborn as sns
import altair as alt
import matplotlib.pyplot as plt
import numpy as np

def savePnlChart(pnlOnly=False,removeArtefacts=True):
    with open("data/data.json") as f:
        data=json.load(f)
    dataWide=formatData(data)
    dataLong=formatDataLong(data)
    df=pd.DataFrame(dataWide)
    dfL=pd.DataFrame(dataLong)
    df.to_csv("data.csv")
    dfL["date"]=dfL["timestamp"].apply(lambda x: datetime.datetime.fromtimestamp(x))
    a=dfL.pivot(index=["timestamp","token"],columns="kind",values="value").sort_index()
    a=a.reset_index()
    a["currentNominal"]=a["current"]/a["price"]
    #a["currentNominal"]=a["current"]/(a["price"]**2)

    sub=a.query("token=='glp'").copy()
    sub["diff"]=sub["currentNominal"].diff()
    rebalances=sub[sub["diff"]!=0]["timestamp"].values
    rebalances_date=[datetime.datetime.fromtimestamp(x) for x in rebalances]
    dfL["token"]=dfL["token"].apply(lambda x:x.lower())
    tokens=list(dfL["token"].value_counts().index)
    tokens.remove("sum")
    tokens.remove("mim")
    tokens.remove("usdc")
    tokens.remove("usdc_e")
    ratepersec=1.09**(1/(365*86400))-1
    ratepersec
    (1+ratepersec)**(86400*365)
    plt.title("Pnl vs normalized prices")
    kind="current"
    sub = dfL.query("kind==@kind and token=='sum'").copy()
    if removeArtefacts:
        diff = np.concatenate([[0],np.diff(sub["value"])])
        a=pd.Series(np.abs(diff)/sub["value"])
        sub["diff"] = a
        mean = a.mean()
        std = a.std()
        threshold = mean+4*std
        sub2 = sub.query("diff<@threshold")
    else:
        sub2=sub
    ax=sns.lineplot(data=sub2,x="date",y="value",label='PnL Strategy')
    print(pnlOnly)
    if not pnlOnly:
        for token in tokens:
            sub = dfL.query("kind=='price' and token==@token").copy()
            sub["value"]=sub["value"]/sub["value"].iloc[0]*1000
            sub["token"]="glp Normalized"
            sns.lineplot(data=sub,x="date",y="value",ax=ax,label=token+" Normalized")
        sub = dfL.query("kind=='price' and token=='usdc'").copy()
        sub["timediff"]=sub["timestamp"]-sub["timestamp"].iloc[0]
        sub["value"]=sub["timediff"].apply(lambda x:1000*(1+ratepersec)**x)
        sns.lineplot(data=sub,x="date",y="value",ax=ax,label="Cricri index at 9% ")
    for x in rebalances_date:
        ax.axvline(x, 0,0.17)
    ax.get_figure().set_size_inches(15,5)
    plt.savefig('pnl.png',dpi=300)
    plt.close()
