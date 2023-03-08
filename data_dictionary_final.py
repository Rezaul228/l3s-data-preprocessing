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
        self.station_name_middle = line.split(' ')[1].split('.')[0]
        station_name_first_part += '_'
        station_name_first_part += line.split(' ')[1].split('.')[1]

        self.first_part_station_name = station_name_first_part
        if len((line.split(' ')[1].split('.'))) == 3:
            self.station_name_extension = (line.split(' ')[1].split('.')[2])
        else: self.station_name_extension = None

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
                count += 1

                action = None
                if (self.line.startswith('[ACTION')) == True:
                    if self.station_counter == 0:
                        self.single_station = Station()
                        self.station_counter += 1
                    else:
                        self.all_station.staion_list.append(self.single_station)
                        self.single_station = Station()
                        self.station_counter += 1
                        action_count = 1

                elif (self.line.startswith('BMS=')) == True:
                    self.single_station.station_name_extractor(self.line)

                elif (self.line.startswith('ACTION=')) == True:
                    action = Action_details()
                    action.each_action_line(self.line, action_count)
                    self.single_station.action_list.append(action)
                    action_count += 1

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
        self.action = 'AC' + ' ' + str(format(action_count, "03d"))

def main():
    #path = '/home/rezaul/Developments/l3s_tadgan/Data_preprocessing/event_data_dictionary.txt'
    path = './CTSS_TOOL_EventDef.txt'
    event_data_obj = Dictionary_event_data()
    dictionary = event_data_obj.read_event_file(path)
    return dictionary

if __name__ == '__main__':
    main()