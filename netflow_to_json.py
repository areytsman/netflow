#!/usr/local/bin/python3.3
__author__ = 'reytsman'
import multiprocessing as mp
import os
import sys
import bz2
import json

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
    filename = path.split('/')
    tempfile_in = open('/tmp/netflow/' + filename[-1] + '.in', 'w')
    tempfile_out = open('/tmp/netflow/' + filename[-1] + '.out', 'w')
    json.dump(dictionary_in,tempfile_in)
    json.dump(dictionary_out,tempfile_out)
    print("File " + path + " processed.")
    #return dictionary_in, dictionary_out


def worker(input, output):
    for func, args in iter(input.get, 'STOP'):
        result = func(*args)
        output.put(result)


if __name__ == '__main__':
    #path = 'C:\\!heroten\\kazc\\2014-12-10\\'
    paths = ['/mnt/data/nfacct/2015/2015-05/2015-05-08/',
			'/mnt/data/nfacct/2015/2015-05/2015-05-09/',
			'/mnt/data/nfacct/2015/2015-05/2015-05-10/',
			'/mnt/data/nfacct/2015/2015-05/2015-05-11/',
			'/mnt/data/nfacct/2015/2015-05/2015-05-12/',
			'/mnt/data/nfacct/2015/2015-05/2015-05-13/',]
    file_paths = []
    for path in paths:
        dir_files = os.listdir(path)
        for file in dir_files:
            file_paths.append(path+file)

    # num_processes = mp.cpu_count()
    num_processes = 3
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
