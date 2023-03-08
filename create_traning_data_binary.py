from os import stat
import pandas as pd
import numpy as np
import datetime as dt
from tqdm import tqdm

def main(method,raw_data):
    def fill_duration(x):
        if action in x['Event']:
            return round(float(x['x1']))
        else:
            return 0

    def fill_code(x):
        if code in x['Code']:
            return 1
        else:
            return 0

    def correctSignals(x):
        if x["AC000"] == 0:
            for i in range(2,len(x)):
                x[i] = 0
        return x


    use_cols = ["Station","Timestamp","Event","Code","x1"]
    type_use_cols = {"Station":int,"Timestamp":str,"Event":str,"Code":str,"x1":str}
    valid_car_codes = ["1111110000","1111110011","1111110013","1111110016","1111110018","1111110021", "1111110022","1111110023","1111110026","1111110028"]
    threshold = 600
    stations =[7220,7230,7240]

    df = pd.read_csv(raw_data,usecols=use_cols,dtype=type_use_cols)

    # *************************** 1 - Data  transormations ***************************
    df["Timestamp"]=df["Timestamp"].astype('datetime64[ns]')
    df["Timestamp"] = (df["Timestamp"]-dt.datetime(1970,1,1)).dt.total_seconds()
    df["Timestamp"] = df["Timestamp"].apply(lambda x: round(x))
    df["Event"]=df["Event"].apply(lambda x: x.replace(" ",""))
    df["x1"]=df["x1"].apply(lambda x: str(x).replace(",","."))



    for station in stations:
        print("Working on Station ",station, method)
        # *************************** 2 - Filter  Station ***************************
        is_station = df.Station == station
        data = df[is_station]
        # *************************** 3 - Filter  car codes ***************************
        data["Code"] = data["Code"].astype(str)
        data["Code"] = data["Code"].apply(lambda x: x if x in valid_car_codes else "unknown")

        # *************************** 4 - Filter  actions ***************************
        actions_events = data["Event"].unique()
        actions = []
        for element in actions_events:
            if 'AC' in element:
                actions.append(element)
        is_action = data.Event.isin(actions)
        df_actions = data[is_action]

        min_time = df_actions["Timestamp"].min()
        max_time = df_actions["Timestamp"].max()
        to_append = []
        for timestamp in tqdm(range(int(min_time+1),int(max_time))):
            if timestamp not in df_actions["Timestamp"].values:
                to_append.append(['',timestamp,'','',''])
        df_to_append = pd.DataFrame(to_append, columns=df_actions.columns)
        df_actions = pd.concat([df_actions,df_to_append], ignore_index = True, axis = 0)
        df_actions=df_actions.sort_values(by=['Timestamp'])
        for action in actions:
            df_actions[action] = df_actions.apply(fill_duration, axis=1)
        df_actions=df_actions.reset_index(drop=True)
        print("all actions are: ",actions)

         # *************************** 5 - impute  missing timestamps ***************************
        for action in actions:
            print("Working on action ",action)
            idx_action = df_actions.index[df_actions['Event'] == action].tolist() # search for action indexes
            begin_actions = [df_actions.iloc[i]['Timestamp']- df_actions.iloc[i][action] for i in idx_action]
            begin_actions = [ i if i>min_time else min_time for i in begin_actions] # search for beginning of the actions indexes
            idx_begin_actions = [df_actions.index[df_actions['Timestamp'] == i].tolist()[0] for i in begin_actions] # search the indexes of beginning of the actions
            if not(len(idx_action) == len(begin_actions) == len(idx_begin_actions)):
                print("idx_action, begin_actions and idx_begin_actions have different lengths")
                break
            for i_action, i_begin in zip(idx_action,idx_begin_actions):
                current_duration = df_actions.iloc[i_action][action]
                if current_duration>threshold:
                    df_actions.loc[i_begin+1:i_action,action] = 0
                else:
                    if method == "constant":
                        df_actions.loc[i_begin+1:i_action,action] = current_duration
                    elif method == "increment":
                        for ind, value in enumerate(range(i_begin+1,i_action),1):df_actions.at[value,action] = ind
                    elif method == "binary":
                        df_actions.loc[i_begin+1:i_action,action] = 1


        df_actions = df_actions.drop(columns=['Event','x1'],axis=1)
        df_actions = df_actions.drop_duplicates(subset=['Timestamp'])
        df_actions = df_actions.reset_index(drop=True)
        idx_codes  = df_actions.index[df_actions['Code'] !=""].tolist()
        #impute car codes
        for i,idx in reversed(list(enumerate(idx_codes))):
            current_code = df_actions.iloc[idx]['Code']
            df_actions.loc[idx_codes[i-1]+1:idx,'Code'] = current_code
        codes=df_actions["Code"].unique()

        for code in codes:
            df_actions[code] = df_actions.apply(fill_code, axis=1)

        if "unknown" in df_actions.columns:
            df_actions = df_actions.drop(columns=["Station","Code","unknown"], axis = 1)
        else:
            df_actions = df_actions.drop(columns=["Station","Code"], axis = 1)
        filename = "datasets/training_data_"+method+"_"+str(station)+"_without_outliers.csv"

        #columns = np.concatenate([["Timestamp"], np.sort(actions)])
        #columns = np.concatenate([columns,codes])
        #df_actions = df_actions[columns]
        #set all cols to 0 when AC000 = 0
        df_actions = df_actions.apply(correctSignals, axis=1)
        df_actions.to_csv(filename,index=False)

if __name__ == "__main__":
    methods = ["binary"]
    raw_data = "raw_data.csv"
    for method in methods:
        main(method,raw_data)