import read_csv
import data_dictionary_final
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import difflib
from datetime import datetime, time

class DataVisualization:


    def date_diff_in_seconds(dt2, dt1):
        timedelta = dt2 - dt1
        return timedelta.days * 24 * 3600 + timedelta.seconds

    def dhms_from_seconds(seconds):
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        return (days, hours, minutes, seconds)

    def chank_len(self, chank):

        # Finding car category in the cycle (sequence)
        new_data_set_cat = []
        data_set_cat = []
        for chank_index in range(len(chank)):
            data=(chank[chank_index])
            cat_arr = np.array(pd.Categorical(data['Code']).categories)
            #print(type(cat_arr))
            cat_arr_list = cat_arr.tolist()
            data_set_cat = data_set_cat + cat_arr_list
        [new_data_set_cat.append(item) for item in data_set_cat if item not in new_data_set_cat]
        print(len(new_data_set_cat))

        # Line graph for same type of car's (event)
        for cat_index in range(len(new_data_set_cat)):

            all_chank_len = []
            for index in range(len(chank)):
                if ((chank[index]['Code'].values[-1:])[0]) == (new_data_set_cat[cat_index]):
                    print(((chank[index]['Code'].values[-1:])[0]))
                    chank_len = len(chank[index])
                    all_chank_len.append(chank_len)
            plt.figure(figsize=(10, 5))
            plt.plot(all_chank_len)
            plt.xlabel("Number of cycle (EV 001 to EV 256)")
            # set label name of x-axis title


            plt.ylabel("Number of Events")
            plt.title("Comparison between number of events in different cycle . Car category: {} ".format(new_data_set_cat[cat_index]))
            plt.savefig('/home/rezaul/Developments/l3s_tadgan/Data_preprocessing/fig/events_{}.png'.format(new_data_set_cat[cat_index]), dpi=400)
            plt.show()

    def chank_comparison(self, chank):

        # Finding car category in the cycle (sequence)
        new_data_set_cat = []
        data_set_cat = []
        for chank_index in range(len(chank)):
            data = (chank[chank_index])
            cat_arr = np.array(pd.Categorical(data['Code']).categories)
            # print(type(cat_arr))
            cat_arr_list = cat_arr.tolist()
            data_set_cat = data_set_cat + cat_arr_list
        [new_data_set_cat.append(item) for item in data_set_cat if item not in new_data_set_cat]
        print(len(new_data_set_cat))

        # Heat map for same type of car
        for cat_index in range(len(new_data_set_cat)):
            list_com = []
            for index_df1 in range(100):
                if ((chank[index_df1]['Code'].values[-1:])[0]) == (new_data_set_cat[cat_index]):
                    df1 = chank[index_df1]["Event"]
                    products_list_df1 = df1.values.tolist()
                    #print(products_list_df1)
                    count = 0
                    for index_df2 in range(100):
                        if ((chank[index_df2]['Code'].values[-1:])[0]) == (new_data_set_cat[cat_index]):
                            df2 = chank[index_df2]["Event"]
                            products_list_df2 = df2.values.tolist()
                            #print(products_list_df2)
                            sm = difflib.SequenceMatcher(None, products_list_df1, products_list_df2)
                            similarity = sm.ratio()
                            list_com.append(similarity)
                            count = count +1
                    #print(list_com)
            my_array = np.array(list_com)
            data =my_array.reshape(count,count)

            data = pd.DataFrame(data=data[0:, 0:],
                            index = [i for i in range(data.shape[0])],columns = [str(i) for i in range(data.shape[1])])


            yticks = data.index
            keptticks = yticks[::int(len(yticks) / 10)]
            yticks = ['' for y in yticks]
            yticks[::int(len(yticks) / 10)] = keptticks

            xticks = data.columns
            keptticks = xticks[::int(len(xticks) / 10)]
            xticks = ['' for y in xticks]
            xticks[::int(len(xticks) / 10)] = keptticks


            sns.set(rc={'figure.figsize': (30, 20)})
            sns.set(font_scale=2.3)
            sns.heatmap(data, linewidth=0, yticklabels=yticks, xticklabels=xticks)

            plt.yticks(rotation=0)
            #plt.rcParams['font.size'] = 30
            plt.title("Comparison between events of cycle. Car category: {} ".format(new_data_set_cat[cat_index]))
            plt.savefig('/home/rezaul/Developments/l3s_tadgan/Data_preprocessing/fig/Cycle_comparison_{}.png'.format(new_data_set_cat[cat_index]), dpi=400)
            plt.show()

    def total_time_of_cycle(self, chank):
        format = '%Y-%m-%d %H:%M:%S.%f'
        # Finding car category in the cycle (sequence)
        new_data_set_cat = []
        data_set_cat = []
        for chank_index in range(len(chank)):
            data = (chank[chank_index])
            cat_arr = np.array(pd.Categorical(data['Code']).categories)
            # print(type(cat_arr))
            cat_arr_list = cat_arr.tolist()
            data_set_cat = data_set_cat + cat_arr_list
        [new_data_set_cat.append(item) for item in data_set_cat if item not in new_data_set_cat]
        print(len(new_data_set_cat))


        for cat_index in range(len(new_data_set_cat)):
            time_of_cycle = []
            for index_df1 in range(100):
                if ((chank[index_df1]['Code'].values[-1:])[0]) == (new_data_set_cat[cat_index]):
                    start_time_stamp = ((chank[index_df1]['Timestamp'].values[:1])[0])
                    print(((chank[index_df1]['Timestamp'].values[:1])[0]))
                    end_time_stamp =((chank[index_df1]['Timestamp'].values[-1:])[0])
                    start_time = datetime.strptime(start_time_stamp, format)
                    end_time = datetime.strptime(end_time_stamp, format)
                    time_dif = end_time - start_time

                    time_of_cycle.append(time_dif.seconds)
                    '''if len((time_dif.split('.')[0]).split(':')) == 3:
                        x =['0'] +(time_dif.split('.')[0]).split(':')
                        print(x)
                    '''
                    #dd, hh, mm, ss = DataVisualization.dhms_from_seconds(DataVisualization.date_diff_in_seconds(end_time - start_time))
                    #try:
                    #    time_def_in_sec = int(dd) * 86400 + int(hh) * 3600 + int(mm) * 60 + int(ss)
                    #except:
                    #    pass


            print('Average time car {}: '.format((new_data_set_cat[cat_index])),(sum(time_of_cycle)/len(time_of_cycle)))
            squares = [i for i in range(len(time_of_cycle))]
            #def addlabels(x, y):
             #   for i in range(len(x)):
              #      plt.text(i, y[i] // 2, y[i], ha='center')

            plt.figure(figsize=(10, 5))
            # plt.bar(category, list_for_graph, color='red')
            plt.bar(squares, time_of_cycle)
            #addlabels(squares, time_of_cycle)
            plt.title("Time comparison between cycle of same category's car")
            plt.xlabel("Number of cycle. Car category: {} ".format(new_data_set_cat[cat_index]))
            #plt.xlabel("Number of cycle same category()")
            plt.ylabel("Time(second)")
            plt.savefig('/home/rezaul/Developments/l3s_tadgan/Data_preprocessing/fig/car_category_{}.png'.format(new_data_set_cat[cat_index]), dpi=400)

            plt.show()




    def dictionary_data_vis(self):
        list_for_graph = []
        event_categories = []
        dictionary = data_dictionary_final.main()

        # Event categories in the data set
        raw_datafram = read_csv.main()
        entire_chank = pd.concat(raw_datafram)
        #print(entire_chank)

        cat_arr = np.array(pd.Categorical(entire_chank['Event']).categories)
        list_for_graph.append(len(cat_arr))
        category_in_dataset = len(cat_arr)
        #print(entire_chank.head(10))

        for station_index in range(len(dictionary.staion_list)):
            if dictionary.staion_list[station_index].first_part_station_name == 'K7U1G13_2530':
                #print('station', station_index, 'Name', dictionary.staion_list[station_index].first_part_station_name)
                for action_index in range(len(dictionary.staion_list[station_index].action_list)):

                    start = (dictionary.staion_list[station_index].action_list[action_index].start_event)
                    end = (dictionary.staion_list[station_index].action_list[action_index].end_event)
                    if start not in event_categories:
                        event_categories.append(dictionary.staion_list[station_index].action_list[action_index].start_event)
                    if end not in event_categories:
                        event_categories.append(dictionary.staion_list[station_index].action_list[action_index].end_event)
                    print('Action:', dictionary.staion_list[station_index].action_list[action_index].action,\
                          'Start Event:', dictionary.staion_list[station_index].action_list[action_index].start_event, \
                          'End Event:', dictionary.staion_list[station_index].action_list[action_index].end_event)
                list_for_graph.append(len(event_categories))
                category_in_dictionary = len(event_categories)

                print('Unique value',list(set(cat_arr).difference(set(event_categories))))
                unique_category_in_dataset = len(list(set(cat_arr).difference(set(event_categories))))
                list_for_graph.append(unique_category_in_dataset)

        category = ['Types of events in dataset', 'Types of events in dictionary', 'Unique types of events in dataset']


        # Ber plot of the event category
        def addlabels(x, y):
            for i in range(len(x)):
                plt.text(i, y[i] // 2, y[i], ha='center')


        plt.figure(figsize=(10, 5))
        #plt.bar(category, list_for_graph, color='red')
        plt.bar(category, list_for_graph)
        addlabels(category, list_for_graph)
        plt.title("Events difference between dataset and dictionary")
        plt.xlabel("")
        plt.ylabel("Number of events")
        plt.show()







def main():
    raw_datafram = read_csv.main()
    chank = raw_datafram

    line_graph = DataVisualization()
    #line_graph.chank_len(chank)
    #line_graph.chank_comparison(chank)
    #line_graph.dictionary_data_vis()
    line_graph.total_time_of_cycle(chank)




if __name__ == '__main__':
    main()

# pd.set_option('display.max_columns', None)
        #print(raw_datafram)

        #for index in range(len(raw_datafram)):
        # for index, row in raw_datafram.iterrows():
        #     #if index == 5000: break
        #     if (row['Event']) == 'EV 256':
        #         print('index',index, ' ' 'value' , row['Event'])
        #         new_dataframe = raw_datafram.drop(raw_datafram.index[0:index+1])
        #         print(new_dataframe.head(5))
        #         print('.....................................')
        #         print(raw_datafram.head(5))
        #         break