import re


class All_station:
    def __init__(self):
        self.staion_list = []

class Station:
    def __init__(self):
        self.action_list = []

        self.first_part_station_name = None
        self.station_name_extension = None
        self.station_name_middle = None

    def station_name_extractor(self, line):
        station_name_first_part = None
        #print(line)

        station_pattern = re.compile(r'K\d\w{4}')
        station_matches = station_pattern.finditer(line)
        for match in station_matches:
            station_name_first_part = match.group(0)

        station_name_first_part += line.split(' ')[0].split('.')[1]
        #print(line.split(' ')[0].split('.')[1])
        #station_name_first_part += line.split(' ')[1].split('.')[0]
        self.station_name_middle = line.split(' ')[1].split('.')[0]
        #print(self.station_name_middle)
        station_name_first_part += '_'
        #print(line.split(' ')[1].split('.')[1])
        station_name_first_part += line.split(' ')[1].split('.')[1]

        self.first_part_station_name = station_name_first_part
        #print('Station_name_first_part: ',station_name_first_part)
        if len((line.split(' ')[1].split('.'))) == 3:

            self.station_name_extension = (line.split(' ')[1].split('.')[2])
            #print('Station_name_extension: ', self.station_name_extension)
        else: self.station_name_extension = None
        #print('Station_name_extension: ', self.station_name_extension)

class Dictionary_event_data:

    def __init__(self):
        #self.path = path
        self.line = None
        self.single_station = None
        self.all_station = All_station()
        self.station_counter = 0


    def read_event_file(self, path):
        action_list = []
        with open(path) as file:
            action_count = 0
            count = 0
            for line in file:
                self.line = line.strip()
                #print('count', count)
                count += 1

                action = None



                if (self.line.startswith('[ACTION')) == True:
                    #print('Single Station:', self.line)
                    if self.station_counter == 0:
                        self.single_station = Station()
                        self.station_counter += 1
                    else:
                        self.all_station.staion_list.append(self.single_station)
                        self.single_station = Station()
                        self.station_counter += 1
                        action_count = 1

                        #reset_data = Single_station()
                        #reset_data.reset()

                elif (self.line.startswith('BMS=')) == True:
                    self.single_station.station_name_extractor(self.line)
                    #print('Station name:', self.line)

                elif (self.line.startswith('ACTION=')) == True:
                    action = Action_details()
                    action.each_action_line(self.line, action_count)
                    self.single_station.action_list.append(action)
                    action_count += 1
                    #print('Actions of Station:', self.line)
                    #print('-----------------')

            # for the last group
            self.all_station.staion_list.append(self.single_station)

        return self.all_station


# Each pair of events along with the corresponding action
class Action_details:

    def __init__(self):
        self.action = None
        self.start_event = None
        self.end_event = None

    def each_action_line(self, line, action_count):
        #print('Line from class', line)
        pattern = re.compile(r'EV\s\d{3}')
        matches = pattern.finditer(line)

        count = 0
        for match in matches:
            event = match.group(0)
                # Remove white space from events
            if count == 0:
                self.start_event = ' '.join(event.split())

                count = 1
            else: self.end_event = ' '.join(event.split())

        #self.action = 'AC' + str(action_count)
        self.action = 'AC' + ' ' + str(format(action_count, "03d"))
        #print('Start event: ', self.start_event)
        #print('End event: ', self.end_event)
        #print('Action: ',self.action)



def main():
    #path = '/home/rezaul/Developments/l3s_tadgan/Data_preprocessing/event_data_dictionary.txt'
    path = './CTSS_TOOL_EventDef.txt'
    event_data_obj = Dictionary_event_data()
    dictionary = event_data_obj.read_event_file(path)

    #for i in range(len(dictionary.staion_list)):
        #print(i , 'F' '  ',dictionary.staion_list[i].first_part_station_name)
        #print(i, 'M' '  ', dictionary.staion_list[i].station_name_middle)

        #print(i, 'S' '  ',dictionary.staion_list[i].station_name_extension)
        #if dictionary.staion_list[i].first_part_station_name =='K7MABT13_7240':
        #    print('station', i, 'Name', dictionary.staion_list[i].first_part_station_name)

    #print(dictionary.staion_list[0].action_list[0].action)
    #print(dictionary.staion_list[0].first_part_station_name)
    return dictionary
    #'K7MABT13_7240'



    #print(event_data_obj.single_station.action_list[2].action)
          #all_station.dictionary.single_station.all_actions[2].action)



if __name__ == '__main__':
    main()