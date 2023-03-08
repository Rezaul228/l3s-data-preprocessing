# Import libraries
import glob
import pandas as pd
import numpy as np
import re
import data_dictionary_final
from datetime import datetime, timedelta

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
    for key1, value1 in event_action.items():
        not_found = False
        for key2, value2 in value1.items():
            # print(type(y))
            try:
                if key2 == "start" and value2 == start_event:
                    return key1, value1['end'], 1
                elif key2 == "end" and value2 == start_event:
                    return key1, value1['start'], 2

            except Exception as e:
                not_found = True
                break

        if not_found:
            return None

result_dataframe = pd.DataFrame(
        columns=["Index", "Station", "Timestamp", "Event",  "UniqueID", "Code", "Robot_name"])

class FileHnadler:

    def __init__(self, csv_files):
        self.csv_files = csv_files
        self.df_list = None
        self.big_df = pd.DataFrame()
        self.temporary_df = pd.DataFrame
        self.data_chank = []
        self.entire_chank = pd.DataFrame()
        self.event_list = []
        self.imputed_data = []
        #Index has been completed
        self.done_index = []

    # Remove unidentified events by dictionary
    def remove_noise(self):
        dictionary = data_dictionary_final.main()

        for index_dic in range(len(dictionary.staion_list)):
            if dictionary.staion_list[index_dic].first_part_station_name == 'K7U1G13_2530':
                for index_action in range(len(dictionary.staion_list[index_dic].action_list)):
                    self.event_list.append(dictionary.staion_list[index_dic].action_list[index_action].start_event)
                    self.event_list.append(dictionary.staion_list[index_dic].action_list[index_action].end_event)

                break

        event_cat_in_data = np.array(pd.Categorical(self.big_df['Event']).categories)
        event_cat_in_dic = np.array(pd.Categorical(self.event_list).categories)
        event_not_in_data =(list(set(event_cat_in_data).difference(set(event_cat_in_dic))))

        for event in event_not_in_data:
            self.big_df.drop(self.big_df[self.big_df['Event'] == event].index, inplace=True)

        # Remove duplicate row from data
        self.big_df = self.big_df.drop_duplicates()
        return self.big_df

    #Read each CSV file into DataFrame
    def read_file(self):
        self.df_list = (pd.read_table(file,\
                                delim_whitespace=True, names=["Station", 'Timestamp', "Time", "Event", "Event_code", "UniqueID", "Code",\
                                                              "Robot_name"], dtype={"Station": str, 'Timestamp': str, "Time": str, "Event": str, \
                                                             "Event_code": str, "UniqueID": str,"Code": str, "Robot_name": str, }) \
                                                                for file in self.csv_files)

        for chank_df in self.df_list:

            self.temporary_df = pd.concat([chank_df], join='outer', ignore_index=True)

            pd.set_option('display.max_columns', None)
            #print(self.temporary_df)
            self.big_df = self.big_df.append(self.temporary_df, ignore_index=True)

        self.big_df["Event"] = self.big_df['Event'].astype(str) + " " + self.big_df["Event_code"]
        self.big_df["Timestamp"] = self.big_df['Timestamp'].astype(str) + " " + self.big_df["Time"]
        self.big_df.drop(columns=['Time', 'Event_code'], inplace=True)#

        for index, row in self.big_df.iterrows():
            if (row['Event']) == 'EV 256':
                self.big_df = self.big_df.drop(self.big_df.index[0:index + 1])
                break

        self.big_df = self.remove_noise()
        self.big_df.sort_values(by='Timestamp', ascending=True, inplace=True)
        self.big_df = self.big_df.reset_index()
        self.big_df.drop(['index'], axis=1, inplace=True)
        # Entire data make to csv
        self.big_df.to_csv('data_set', encoding='utf-8', index=True)
        return self.big_df

    def event_data_cycle(self):
        start = None
        end = None
        self.entire_chank = self.read_file()
        self.entire_chank = self.entire_chank.reset_index(drop=True)

        cat_arr = np.array(pd.Categorical(self.entire_chank['UniqueID']).categories)
        #print(len(cat_arr))

        for index, row in self.entire_chank.iterrows():
            if index <= len(self.entire_chank):
                if (row['Event']) == 'EV 001':
                    start = index
                if (row['Event']) == 'EV 256':
                    end = index
                    if end != None:
                        data = self.entire_chank.iloc[start:end+1]
                        # Unique ID and car code inserting in column
                        unique_id_of_col = (data.iloc[-1]['UniqueID'])
                        car_code_of_col = (data.iloc[-1]['Code'])
                        data.loc[data['UniqueID'] != unique_id_of_col, 'UniqueID'] = unique_id_of_col
                        data.loc[data['Code'] != car_code_of_col, 'Code'] = car_code_of_col

                        self.data_chank.append(data)
                        end = None
                    else:
                        break
            else:
                break
        return self.data_chank

    def new_row_insert(self,data_to_impute):
        data = data_to_impute.reset_index()
        data.drop(['index'], axis=1, inplace=True )

        for row_index in range(len(data) - 1, -1, -1):
            if row_index not in self.done_index:
                action, value, e_type = event_action_value(data.iloc[row_index]['Event'])
                if e_type == 1:
                    date_time = datetime.strptime((data.iloc[row_index]['Timestamp']), '%Y-%m-%d %H:%M:%S.%f')
                    date_time = date_time + timedelta(milliseconds=10)

                    new_row = {"Index": row_index + 1, "Station": (data.iloc[row_index]['Station']), "Timestamp": str(date_time),
                               "Event": value,

                               "UniqueID": (data.iloc[row_index]['UniqueID']), "Code": (data.iloc[row_index]['Code']), "Robot_name": (data.iloc[row_index]['Robot_name'])
                               }
                    df_new_row = pd.DataFrame([new_row])

                    # Insert new row in dataframe
                    for i in range(len(data), row_index + 1, -1):
                        data.loc[i] = data.loc[i - 1]
                    data.loc[row_index + 1] = df_new_row.iloc[0]
                else:
                    for end_ev_index in range (row_index-1, -1, -1):
                       if data.iloc[end_ev_index]['Event'] == data.iloc[row_index]['Event']:
                            date_time = datetime.strptime((data.iloc[row_index]['Timestamp']), '%Y-%m-%d %H:%M:%S.%f')
                            date_time = date_time - timedelta(milliseconds=10)

                            new_row = {"Index": row_index , "Station": (data.iloc[row_index]['Station']),
                                       "Timestamp": str(date_time),
                                       "Event": value,

                                       "UniqueID": (data.iloc[row_index]['UniqueID']), "Code": (data.iloc[row_index]['Code']),
                                       "Robot_name": (data.iloc[row_index]['Robot_name'])
                                       }
                            df_new_row = pd.DataFrame([new_row])

                            # Insert new row in dataframe
                            for i in range(len(data), row_index, -1):
                                data.loc[i] = data.loc[i - 1]
                            data.loc[row_index] = df_new_row.iloc[0]
                            break
                       elif data.iloc[end_ev_index]['Event'] == value:
                           self.done_index.append(end_ev_index)
                           break
                       elif end_ev_index == 0 and data.iloc[end_ev_index]['Event'] != data.iloc[row_index]['Event'] and data.iloc[end_ev_index]['Event'] != value:
                           date_time = datetime.strptime((data.iloc[row_index]['Timestamp']), '%Y-%m-%d %H:%M:%S.%f')
                           date_time = date_time - timedelta(milliseconds=10)

                           new_row = {"Index": row_index, "Station": (data.iloc[row_index]['Station']),
                                       "Timestamp": str(date_time),
                                       "Event": value,

                                       "UniqueID": (data.iloc[row_index]['UniqueID']), "Code": (data.iloc[row_index]['Code']),
                                       "Robot_name": (data.iloc[row_index]['Robot_name'])
                                       }
                           df_new_row = pd.DataFrame([new_row])

                            # Insert new row in dataframe
                           for i in range(len(data), row_index, -1):
                               data.loc[i] = data.loc[i - 1]
                           data.loc[row_index] = df_new_row.iloc[0]

        return data

def main():
    # Get CSV files list from a folder
    path = '/home/rezaul/Developments/l3s_tadgan/Data_preprocessing/data'
    csv_files = glob.glob(path + "/*.evdata")

    # Function for sorting the file list according to the number in the file name
    def extract_number(list_of_file):
        pattern = re.search(r'\d{8}', list_of_file)
        return int(pattern[0])

    csv_files.sort(key=extract_number)

    data_set = FileHnadler(csv_files)
    pd.set_option('display.max_columns', None)
    return data_set.event_data_cycle()


if __name__ == '__main__':
    main()
