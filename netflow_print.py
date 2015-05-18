#!/usr/local/bin/python3.3
__author__ = 'reytsman'
import multiprocessing as mp
import os
import sys
import bz2
import json


def reed_data_from_flows(path):
    global this_file
    if not os.path.exists(path):
        print("File " + path + " not found")
        sys.exit()
    try:
        this_file = open(path, mode='rt')
    except:
        print('Cannot open ' + path)

    dictionary = json.load(this_file)

    this_file.close()
    return dictionary


def dict_split(dict1, dict2):
    for item in dict2.items():
        dict1.setdefault(item[0], 0)
        dict1[item[0]] += item[1]

    return dict1



# path = 'C:\\!heroten\\kazc\\2014-12-10\\'
# paths = ['/tmp/netflow/']
paths = ['C:\\!heroten\\kazc\\111\\']
file_paths = []
for path in paths:
    dir_files = os.listdir(path)
    for file in dir_files:
        file_paths.append(path + file)

dictionary_in = {}
dictionary_out = {}

for file in file_paths:
    if file.split('.')[-1] == 'in':
        dict_split(dictionary_in, reed_data_from_flows(file))
    elif file.split('.')[-1] == 'out':
        dict_split(dictionary_out, reed_data_from_flows(file))

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
print("SRC_AS".rjust(10), "\tBytes")
sum_in = 0
sum_out = 0
for item in in_list:
    if item[1] > 0.2:
        print(str(item[0]).rjust(10), "%.2f" % (item[1]), sep='\t', end='%\n')
        sum_in += item[1]
print('Other'.rjust(10), "%.2f" % (100 - sum_in), sep='\t', end='%\n')

print("==================================================================================")
print("OUT")
print("DST_AS".rjust(10), "\tBytes")
for item in out_list:
    if item[1] > 0.2:
        print(str(item[0]).rjust(10), "%.2f" % (item[1]), sep='\t', end='%\n')
        sum_out += item[1]
print('Other'.rjust(10), "%.2f" % (100 - sum_out), sep='\t', end='%\n')