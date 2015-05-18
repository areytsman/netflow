#!/usr/local/bin/python3.3
__author__ = 'reytsman'
import multiprocessing as mp
import os
import sys
import bz2


def reed_data_from_flows(path):
    dictionary_in = {}
    dictionary_out = {}
    global this_file
    if not os.path.exists(path):
        print("File " + path + " not found")
        sys.exit()
    try:
        this_file = bz2.open(path, mode='rt')
    except:
        print('Cannot open ' + path)
    # 0 SRC_AS,
    # 1 DST_AS,
    # 2 SRC_IP,
    # 3 DST_IP,
    # 4 SRC_PORT,
    # 5 DST_PORT,
    # 6 PROTOCOL,
    # 7 TOS,
    # 8 TIMESTAMP_START,
    # 9 TIMESTAMP_END,
    # 10 PACKETS,
    # 11 BYTES
    for line in this_file:
        line = line.split(',')
        if len(line) > 10:
            if line[1] == '31484':
                dictionary_in.setdefault(str(line[0]), 0)
                dictionary_in[str(line[0])] += int(line[11])
            elif line[0] == '31484':
                dictionary_out.setdefault(str(line[1]), 0)
                dictionary_out[str(line[1])] += int(line[11])

    this_file.close()

    print("File " + path + " processed.")
    return dictionary_in, dictionary_out


def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        result = func(*args)
        output.put(result)


if __name__ == '__main__':
    #path = 'C:\\!heroten\\kazc\\2014-12-10\\'
    paths = ['C:\\!heroten\\kazc\\2014-12-10\\','C:\\!heroten\\kazc\\2014-12-11\\']
    file_paths = []
    for path in paths:
        dir_files = os.listdir(path)
        for file in dir_files:
            file_paths.append(path+file)

    # num_processes = mp.cpu_count()
    num_processes = 2
    tasks = [(reed_data_from_flows, (file,)) for file in file_paths]

    # Create queues
    task_queue = mp.Queue()
    done_queue = mp.Queue()

    # Submit tasks
    for task in tasks:
        task_queue.put(task)

    # Start worker processes
    for i in range(num_processes):
        mp.Process(target=worker, args=(task_queue, done_queue)).start()

    # Get results
    results = []
    for i in range(len(tasks)):
        results.append(done_queue.get())

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    dictionary_in = {}
    dictionary_out = {}
    for result in results:
        for item in result[0].items():
            dictionary_in.setdefault(item[0], 0)
            dictionary_in[item[0]] += item[1]
        for item in result[1].items():
            dictionary_out.setdefault(item[0], 0)
            dictionary_out[item[0]] += item[1]

    if len(dictionary_in) == 0 or len(dictionary_out) == 0:
        print("Dictionary is empty")
        sys.exit()

    in_list = dictionary_in.items()
    in_list = [list(i) for i in in_list]
    out_list = dictionary_out.items()
    out_list = [list(i) for i in out_list]

    in_summary = sum(dictionary_in.values())
    out_summary = sum(dictionary_out.values())

    for item in in_list:
        item[1] = (item[1] / in_summary) * 100
    for item in out_list:
        item[1] = (item[1] / out_summary) * 100

    in_list.sort(key=lambda item: item[1], reverse=True)
    out_list.sort(key=lambda item: item[1], reverse=True)

    print("Lists sorted.\n")

    print("IN")
    print("SRC_AS".rjust(10),"\tBytes")
    sum_in = 0
    sum_out = 0
    for item in in_list:
        if item[1] > 0.2:
            print(str(item[0]).rjust(10),"%.2f"%(item[1]),sep='\t',end='%\n')
            sum_in += item[1]
    print('Other'.rjust(10),"%.2f"%(100-sum_in), sep='\t',end='%\n')

    print("==================================================================================")
    print("OUT")
    print("DST_AS".rjust(10),"\tBytes")
    for item in out_list:
        if item[1] > 0.2:
            print(str(item[0]).rjust(10),"%.2f"%(item[1]),sep='\t',end='%\n')
            sum_out += item[1]
    print('Other'.rjust(10),"%.2f"%(100-sum_out), sep='\t',end='%\n')