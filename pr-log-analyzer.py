import sys
import datetime
import csv

traceId_to_time_dic = dict()


def check_input_param():
    params = sys.argv
    if params.__len__() != 2:
        print("Error: no parameters. Please, add packet-retrieval's log file as a parameter")
        sys.exit(2)


def parse_start_session_line(line):
    trace_id = parse_trace_id_in_start_line(line)
    session_start_date_time = parse_datetime(line)
    datetime_dict = {"start_time": session_start_date_time, "end_time": ""}
    traceId_to_time_dic[trace_id] = datetime_dict


def parse_trace_id_in_start_line(line):
    trace_id_start_index = 0
    trace_id_last_index = 0
    if line.__contains__('Get historical'):
        trace_id_start_index = line.index("with traceId") + "with traceId ".__len__()
        trace_id_last_index = trace_id_start_index + 24
    else:
        trace_id_start_index = line.index("with traceId: ") + "with traceId: ".__len__()
        trace_id_last_index = trace_id_start_index + 24
    trace_id = line[trace_id_start_index: trace_id_last_index]
    return trace_id


def parse_datetime(line):
    year = int(line[9: 9 + 4])
    month = int(line[14: 16])
    day = int(line[17: 19])
    hour = int(line[20:22])
    minute = int(line[23:25])
    sec = int(line[26:28])
    millisecond = int(line[29: 32] + '000')
    return datetime.datetime(year, month, day, hour, minute, sec, millisecond)


def parse_last_session_line(line):
    #trace_id = ''
    if line.__contains__('PacketsServiceOnlineImpl(109) FN:stopTrace'):
        trace_id = line.split(' ')[8]
    else:
        trace_id = line.split(' ')[14]
    if trace_id == '':
        return
    end_session_datetime = parse_datetime(line)
    times_dic = dict()
    try:
        times_dic = traceId_to_time_dic[trace_id]
    except Exception as e:
        print("Unexpected error. Reason:", e.__cause__)

    times_dic.update({"end_time": end_session_datetime})


def parse_line(line):
    if line.__contains__("R:<<<<<<<<<<<<< Get"):
        parse_start_session_line(line)
    if line.__contains__('=========Send stop message') or line.__contains__('PacketsServiceOnlineImpl(109) FN:stopTrace'):
        parse_last_session_line(line)


def read_file():
    params = sys.argv
    try:
        file = open(params[1])
        print("file name:", file.name)

        lines = file.readlines()
        print("lines number in file:", lines.__len__())

        for line in lines:
            first_symbol = line[0]
            if first_symbol != 'L':
                continue
            parse_line(line)
    except FileNotFoundError:
        print("Error. Could not open file", params[1])
        sys.exit(2)


def create_data_for_csv():
    result = []
    for item in traceId_to_time_dic.items():
        # new_list = [item[0], item[1].get("start_time"), item[1].get("end_time")]
        try:
            start_time = int(item[1].get("start_time").timestamp()*1000)
            end_time = int(item[1].get("end_time").timestamp()*1000)
            duration = end_time - start_time
            new_list = [item[0], str(start_time), str(end_time), str(duration)]
            result.append(new_list)
        except Exception as e:
            print('Unexpected error. Reason:', e.__cause__)
    return result


def create_csv():
    header = ['trace id', 'start_time', 'end_time', 'duration']
    data = create_data_for_csv()
    with open('output.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


def main():
    check_input_param()
    read_file()
    create_csv()
    print('lines in output files:', traceId_to_time_dic.values().__len__())


if __name__ == '__main__':
    main()
