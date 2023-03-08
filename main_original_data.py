import numpy as np
import pandas as pd
from datetime import datetime
import read_csv
import data_dictionary_final


format = '%Y-%m-%d %H:%M:%S.%f'

event_action = {}
def dictionary_of_events():
    dictionary = data_dictionary_final.main()

    for index_dic in range(len(dictionary.staion_list)):
        if dictionary.staion_list[index_dic].first_part_station_name == 'K7U1G13_2530':
            for index_action in range (len(dictionary.staion_list[index_dic].action_list)):
                event_action[(dictionary.staion_list[index_dic].action_list[index_action].action)]\
                    = {'start': (dictionary.staion_list[index_dic].action_list[index_action].start_event),\
                       'end': (dictionary.staion_list[index_dic].action_list[index_action].end_event)}

            break
    return event_action


def event_action_value(start_event):
    not_valid_action = None
    key1_list = []
    value1_list =[]

    for key1, value1 in event_action.items():
        not_found = False
        for key2, value2 in value1.items():
            try:
                if key2 == "start" and value2 == start_event:
                    key1_list.append(key1)
                    value1_list.append(value1['end'])
                elif key1 == list(event_action)[-1]:
                    return key1_list, value1_list

            except Exception as e:
                not_found = True
                break

        if not_found:
            return None

class FileHnadler:

    def __init__(self, data):
        self.data = data
        self.action_data_list = []
        #self.action_df = pd.DataFrame()

    def data_from_read_csv(self):

        for data_index in range(2):#(len (self.data)-1):
            data_frame_1 = self.data[data_index]
            len_dataframe_1 = len(data_frame_1)
            data_frame_2 = self.data[data_index +1]
            data_set = pd.concat([data_frame_1, data_frame_2])
            data_set = data_set.reset_index().rename(columns={'index': 'original index'})
            self.data_to_process(data_set, len_dataframe_1)

        action_df = pd.concat(self.action_data_list)

        action_df.to_csv('action_df', encoding='utf-8', index=False)
        print(action_df)
    def data_to_process(self, data_set, len_dataframe_1):
        # Create the dictionary to json
        dictionary_of_events()

        raw_datafram = data_set
        len_of_data = len_dataframe_1

        raw_datafram.to_csv('two_chank', encoding='utf-8', index=False)

        for index in range(len_of_data):
            flag = 0
            if index not in Row.compleated_row:
                row_object = RowBuilders(index, raw_datafram.loc[index], flag=0)
                row_object.split_row_data()
                f"Start time, {Row.start_time} start event {Row.s_event}"

                index1 = index
                for index1 in range(index + 1, len(raw_datafram)):
                    flag = 1
                    if Row.s_event == raw_datafram.loc[index1, 'Event']:
                        break
                    if raw_datafram.loc[index1, 'Event'] in Row.e_event_list:
                        Row.e_event = raw_datafram.loc[index1, 'Event']
                        action_potion = Row.e_event_list.index(raw_datafram.loc[index1, 'Event'])
                        Row.action = Row.action_list[action_potion]
                        row_object = RowBuilders(index1, raw_datafram.loc[index1], flag=1)
                        row_object.split_row_data()

            Row.reset(Row)

        Results.result_dataframe.sort_values(by='Timestamp', ascending=True, inplace=True)
        #Results.result_dataframe.to_csv('action_data_2', encoding='utf-8', index=False)
        self.action_data_list.append(Results.result_dataframe)
        Results.result_dataframe = None

class Row:
    compleated_row = []
    start_time = None
    end_time = None
    s_event = None
    e_event = None
    e_event_list = None
    station = None
    robot_name = None
    action = None
    action_list = None
    index = None
    UniqueID = None
    Code = None
    Robot_name = None
    Time_diff: datetime = None
    action_name = None
    s_index = None
    e_index = None
    second = None

    def reset(self):
        self.start_time = None
        self.end_time = None
        self.s_event = None
        self.e_event_list = None
        self.e_event = None
        self.station = None
        self.robot_name = None
        self.action = None
        self.action_list = None
        self.index = None
        self.UniqueID = None
        self.Code = None
        self.Robot_name = None
        self.Time_diff = None
        self.action_name = None
        self.s_index = None
        self.end_time = None
        self.second = None

class Results:
    result_dataframe = pd.DataFrame(
        columns=["S_index","E_Index", "Station", "Timestamp", "Action",  "UniqueID", "Code", "Robot_name",
                 "Time_diff"])

class RowBuilders:
    def __init__(self, index, rows, flag):
        self.rows = rows
        self.index = index
        self.flag = flag
        self.df_new_row = None

    def split_row_data(self):
        Row.station = self.rows.loc['Station']

        if self.flag == 0:
            Row.s_event = self.rows.loc['Event']
            ret_val = event_action_value(Row.s_event)
            if ret_val is not None:
                Row.action_list, Row.e_event_list = ret_val
                Row.start_time = self.rows.loc['Timestamp']
                Row.s_index =self.rows.loc['original index']
        elif self.flag == 1:
            Row.end_time = self.rows.loc['Timestamp']
            Row.index = self.index
            Row.e_index = self.rows.loc['original index']
            Row.station = self.rows.loc['Station']
            Row.UniqueID = self.rows.loc['UniqueID']
            Row.Code = self.rows.loc['Code']
            Row.Robot_name = self.rows.loc['Robot_name']
            Row.action_name = Row.action

            E_time: datetime = datetime.strptime(Row.end_time, format)
            S_time: datetime = datetime.strptime(Row.start_time, format)
            Row.Time_diff = E_time - S_time
            Row.second = (Row.Time_diff.seconds)

            #print(Row.index ,':','E_event:',Row.e_event, 'S_event:', Row.s_event, 'S_Time', Row.start_time , 'E_Time', Row.end_time, 'time_def:', Row.Time_def)
            df = pd.DataFrame()
            new_row = {"S_index":Row.s_index, "E_Index": Row.e_index, "Station": Row.station, "Timestamp": Row.end_time, "Action": Row.action_name,

                       "UniqueID": Row.UniqueID, "Code": Row.Code, "Robot_name": Row.Robot_name,
                       "Time_diff": Row.second}
            df_dictionary = pd.DataFrame([new_row])
            Results.result_dataframe = pd.concat([Results.result_dataframe, df_dictionary], ignore_index=True)

            #Row.reset(Row)
        Row.robot_name = self.rows.loc['Robot_name']


def main():

    receive_data = read_csv.main()
    list_of_chank = receive_data

    data_to_process = FileHnadler(list_of_chank)
    data_to_process.data_from_read_csv()

if __name__ == '__main__':
    main()


