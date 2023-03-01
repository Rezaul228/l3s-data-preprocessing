import numpy as np
import pandas as pd
from datetime import datetime
import read_csv
import data_dictionary_final


format = '%Y-%m-%d %H:%M:%S.%f'

#read_csv.main()

event_action = {}
def dictionary_of_events():
    dictionary = data_dictionary_final.main()

    for index_dic in range(len(dictionary.staion_list)):
        if dictionary.staion_list[index_dic].first_part_station_name == 'K7U1G13_2530':
            print(type((dictionary.staion_list[index_dic])))

            for index_action in range (len(dictionary.staion_list[index_dic].action_list)):
                event_action[(dictionary.staion_list[index_dic].action_list[index_action].action)]\
                    = {'start': (dictionary.staion_list[index_dic].action_list[index_action].start_event),\
                       'end': (dictionary.staion_list[index_dic].action_list[index_action].end_event)}
                #event_action_1[(dictionary.staion_list[index_dic].

            break
    #print(event_action_1)

    return event_action








def event_action_value(start_event):
    not_valid_action = None
    for key1, value1 in event_action.items():
        #
        # print(type(i))
        not_found = False
        for key2, value2 in value1.items():
            # print(type(y))
            try:
                if key2 == "start" and value2 == start_event:
                    # print(type(y))
                    # print('key', key2 , 'value', value2)
                    # print('action', key1, 'end', value1['end'] )
                    # print((key1))
                    # if key1 != None:

                    return key1, value1['end']
            except Exception as e:
                not_found = True
                break

        if not_found:
            return None


class FileHnadler:

    def __init__(self, data):
        self.data = data



    def data_from_read_csv(self):


        data_frame_1 = self.data[0]
        len_dataframe_1 = len(data_frame_1)
        data_frame_2 = self.data[1]
        #paired_dataframe = [data_frame_1, data_frame_2]
        data_set = pd.concat([data_frame_1, data_frame_2])
        #print(type(data_set))
        #print(data_set)
        #data_set.to_csv('imputed_data_0', encoding='utf-8', index=False)
        return data_set, len_dataframe_1


        # Finding car category in the cycle (sequence)
        # new_data_set_cat = []
        # data_set_cat = []
        # for chank_index in range(len(chank)):
        #     data = (chank[chank_index])
        #     cat_arr = np.array(pd.Categorical(data['Code']).categories)
        #     # print(type(cat_arr))
        #     cat_arr_list = cat_arr.tolist()
        #     data_set_cat = data_set_cat + cat_arr_list
        # [new_data_set_cat.append(item) for item in data_set_cat if item not in new_data_set_cat]
        # print(len(new_data_set_cat))




class Row:
    compleated_row = []

    start_time = None
    end_time = None
    s_event = None
    e_event = None
    station = None
    robot_name = None
    action = None
    index = None
    #Unknown1 = None
    UniqueID = None
    Code = None
    Robot_name = None
    Time_def: datetime = None
    action_name = None
    s_index = None

    def reset(self):
        self.start_time = None
        self.end_time = None
        self.s_event = None
        self.e_event = None
        self.station = None
        self.robot_name = None
        self.action = None
        self.index = None
        #self.Unknown1 = None
        self.UniqueID = None
        self.Code = None
        self.Robot_name = None
        self.Time_def = None
        self.action_name = None
        self.s_index = None





class Results:
    result_dataframe = pd.DataFrame(
        columns=["S_index","E_Index", "Station", "Timestamp", "Action",  "UniqueID", "Code", "Robot_name",
                 "Time_def"])


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
            # if event_action_value(Row.s_event) != None:
            ret_val = event_action_value(Row.s_event)
            if ret_val is not None:
                Row.action, Row.e_event = ret_val  # event_action_value(Row.s_event)
                Row.start_time = self.rows.loc['Timestamp']

                Row.s_index =self.index
                print('S_Index:',self.index)

        elif self.flag == 1:
            #Row.compleated_row.append(self.index)
            Row.end_time = self.rows.loc['Timestamp']
            Row.index = self.index
            Row.station = self.rows.loc['Station']
            #Row.Unknown1 = self.rows.loc['Unknown1']
            Row.UniqueID = self.rows.loc['UniqueID']
            Row.Code = self.rows.loc['Code']
            Row.Robot_name = self.rows.loc['Robot_name']
            E_time: datetime = datetime.strptime(Row.end_time, format)
            S_time: datetime = datetime.strptime(Row.start_time, format)
            #print(datetime)
            Row.Time_def = E_time - S_time
            Row.action_name = Row.action #Row.robot_name + Row.action
            print(Row.index ,':','E_event:',Row.e_event, 'S_event:', Row.s_event, 'S_Time', Row.start_time , 'E_Time', Row.end_time, 'time_def:', Row.Time_def)
            df = pd.DataFrame()
            new_row = {"S_index":Row.s_index, "E_Index": Row.index, "Station": Row.station, "Timestamp": Row.end_time, "Action": Row.action_name,

                       "UniqueID": Row.UniqueID, "Code": Row.Code, "Robot_name": Row.Robot_name,
                       "Time_def": Row.Time_def}

            df_dictionary = pd.DataFrame([new_row])
            Results.result_dataframe = pd.concat([Results.result_dataframe, df_dictionary], ignore_index=True)

            Row.reset(Row)



        Row.robot_name = self.rows.loc['Robot_name']

        # Call the function to extract the action and end event
        # print('end',self.e_event)
        # extracted_row = Row(self.row_index,self.start_end_time,self.event,self.robot_name)


def main():

    # Create the dictionary to json
    dictionary_of_events()

    # Data receive from read_csv.py
    receive_data = read_csv.main()
    list_of_chank = receive_data

    data_to_process = FileHnadler(list_of_chank)
    process_data = data_to_process.data_from_read_csv()
    #raw_datafram.to_csv('file_name_noise_removed_duplicate', encoding='utf-8', index=False)
    raw_datafram = process_data[0]
    len_of_data = process_data[1]
    print(len_of_data)
    print(raw_datafram)
    raw_datafram.to_csv('two_chank', encoding='utf-8', index=False)

    for index in range(len_of_data):
        #print(raw_datafram)
        flag = 0
        if index not in Row.compleated_row:

             row_object = RowBuilders(index, raw_datafram.loc[index], flag=0)
             row_object.split_row_data()
             f"Start time, {Row.start_time} start event {Row.s_event}"

             index1 = index
             for index1 in range(index+1, len(raw_datafram)):
                flag = 1
                if Row.s_event == raw_datafram.loc[index1, 'Event']:
                    break

                if raw_datafram.loc[index1, 'Event'] == Row.e_event:
                    # print(raw_datafram.loc[index1, 'Event'])
                    row_object = RowBuilders(index1, raw_datafram.loc[index1], flag=1)
                    row_object.split_row_data()
                    # f"End time, {Row.end_time} End event, {Row.e_event}"

                    break
        else:
            continue


    Results.result_dataframe.sort_values(by='Timestamp', ascending=True, inplace=True)
    Results.result_dataframe.to_csv('action_data_2', encoding='utf-8', index=False)
    print(Results.result_dataframe)



if __name__ == '__main__':
    main()
