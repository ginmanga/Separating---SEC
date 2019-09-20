import pandas as pd
import os, sys, copy
from collections import Counter # Counter counts the number of occurrences of each item
from itertools import tee, count

def write_file(path_file, filename, data, write_type):
    """Writes all data to file"""
    #first tells if it writtes or appends
    #write_type 'w' or 'a'
    #print(data)
    path_to_2 = os.path.join(path_file, filename)
    with open(path_to_2, write_type) as file:
        file.writelines('\t'.join(i) + '\n' for i in data)
    file.close()

def write_file_direct(path_file, gvkey, fyear, type_doc, type_sec, f):
    """Writes individual docs to files"""
    print("HEERRRRRE")
    dir_to_save = os.path.abspath(path_file + "\\" + gvkey  + "\\" + fyear)
    filename = type_doc + '_' + type_sec + '.txt'

    dir_to_file = os.path.join(dir_to_save, filename)
    if os.path.exists(dir_to_file):
        filename = type_doc + '_' + type_sec + '.txt'

    if not os.path.exists(dir_to_save):
        os.makedirs(dir_to_save)

    with open(dir_to_file, 'w') as file:
        file.writelines(i for i in f)
    return None


def sep_docs(new_list, path_to_save):
    """Separates documents from files"""
    for i, item in enumerate(new_list):
        try:
            fhand = open(item[0], encoding = "utf8")
            start = int(float(new_list[i][5]))
            end =  int(float(new_list[i][6]))-1
            lines = fhand.readlines()[start:end]
            print(start)
            gvkey =  str(new_list[i][1])
            fyear = str(new_list[i][2])
            type_doc = new_list[i][3]
            type_sec = new_list[i][4]
            write_file_direct(path_to_save, gvkey, fyear, type_doc, type_sec, lines)
        except:
            continue


def uniquify(seq, suffs = count(1)):
    """Make all the items unique by adding a suffix (1, 2, etc).

    `seq` is mutable sequence of strings.
    `suffs` is an optional alternative suffix iterable.
    """
    not_unique = [k for k,v in Counter(seq).items() if v>1] # so we have: ['name', 'zip']
    # suffix generator dict - e.g., {'name': <my_gen>, 'zip': <my_gen>}
    suff_gens = dict(zip(not_unique, tee(suffs, len(not_unique))))
    for idx,s in enumerate(seq):
        try:
            suffix = str(next(suff_gens[s]))
        except KeyError:
            # s was unique
            continue
        else:
            seq[idx] += suffix

def folder_loop(path):
    """Loops through contents of a folder
    saves file path, keep only text files"""
    req_paths = []
    for path, dirs, files in os.walk(path):
        req_paths.extend([os.path.join(path, i) for i in files])
    req_paths_doc = [i for i in req_paths if os.path.splitext(i)[1] == ".doc" or os.path.splitext(i)[1] == ".DOC"]
    req_paths = [i for i in req_paths if os.path.splitext(i)[1] == ".txt" or os.path.splitext(i)[1] == ".TXT"]
    return req_paths, req_paths_doc


def splitall(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts

def first_line(path):
    """Go through each file and record file line where the first document starts
    make a list with path and line start number"""
    req_paths, req_paths_doc = folder_loop(path)  # creates lists of paths
    first_line_list = []
    for filename in req_paths:
        doct_found = 0
        fhand = open(filename, encoding="utf8")
        [start, end] = [0, 0]
        text = []
        doc_count = 0
        line_count = 0
        doc_type_known = False
        doc_type = ''
        file_info = [filename, '']
        doc_info = [filename, '', '', '']
        doc_data = []
        start_docu = ["of", "DOCUMENTS"]
        line_count = 0

        for line in fhand:
            if all(f in line for f in start_docu):
                check = [word for word in line.strip().split() if word not in start_docu]
                if len(check) == 2 and all(s.isdigit for s in check):
                    #print(filename)
                    #print(line)
                    #print(line_count)
                    file_info[1] = str(line_count)
                    #print(file_info)
                    break
            line_count += 1
        first_line_list.append(file_info)
    #print(first_line_list)
    return first_line_list


def sep_files(directory_sample, path_to_save):
    """Takes file in directory_sample and separates 10-K and AR and saves them into path_Save
    saves by gvkey and fyear"""
    directory_firstline = "C:/Users/Panqiao/Documents/Research/SEC Online - 05042017/All - SS/first_line.txt"
    sample = pd.read_csv(directory_sample, sep=",")
    firstline = pd.read_csv(directory_firstline, sep="\t")  # line where files start need to add to line start

    sample_small = sample[['gvkey', 'datadate', 'fyear', 'doc_type', "sec_type", 'path',
                           'line_start', 'line_end']]
    firstline = firstline[['path', 'first_line']]
    gvkey_list = sample_small['gvkey'].tolist()
    fyear_list = sample_small['fyear'].tolist()

    doc_type_list = [i.replace('[', '').replace(']', '')
                         .replace("\'", "").replace(" ", "").split(",")
                     for i in sample_small['doc_type']]

    for i in doc_type_list:
        uniquify(i, (f'_{x!s}' for x in range(1, 100)))

    sec_type_list = [i.replace('[', '').replace(']', '')
                         .replace("\'", "").replace(" ", "").split(",")
                     for i in sample_small['sec_type']]

    path_list2 = [i.replace('[', '').replace(']', '')
                      .replace("\'", "").split(",")
                  for i in sample_small['path']]

    line_start_list = [i.replace('[', '').replace(']', '')
                           .replace("\'", "").replace(" ", "").split(",")
                       for i in sample_small['line_start']]

    line_end_list = [i.replace('[', '').replace(']', '')
                         .replace("\'", "").replace(" ", "").split(",")
                     for i in sample_small['line_end']]

    firstline_line = firstline['first_line'].tolist()
    firstline_path = firstline['path'].tolist()
    firstline_path2 = [os.path.abspath(i) for i in firstline_path]
    firstline_list = []

    for i, item in enumerate(firstline_path):
        firstline_list.append([item, firstline_line[i]])

    new_list = []
    for i, item in enumerate(path_list2):
        for j, item_2 in enumerate(item):
            new_list.append([item_2.strip(), gvkey_list[i], fyear_list[i],
                             doc_type_list[i][j], sec_type_list[i][j],
                             line_start_list[i][j], line_end_list[i][j]])

    for i, item in enumerate(new_list):
        """Add doc_line start to list"""
        try:
            a = os.path.abspath(item[0])
            aa = firstline_path2.index(a)
            new_list[i].append(firstline_line[aa])
        except:
            continue
    sep_docs(new_list, path_to_save)


def separate_paths(paths):
    """Takes path and """

    new_list = []
    path1 = [[i] for i in paths]
    path2 = [[os.path.splitext(i)[0]] for i in paths]

    for i, item in enumerate(path1):
        a = splitall(path2[i][0])[8:11]
        a.append(item[0])
        new_list.append(a)
    return new_list

def collapse_list(a, x, y, header = 0):
    #a list to collapse
    #x group of variables used to collapase, indexes
    #y list of columns to keep
    # a collapsed list collpased with a 10K and AR path if any
    counter_1 = 0
    counter_2 = 0
    accumulator_text = []
    accumulator_doc = []
    accumulator_path = []
    if header == 0:
        collapsed_list = []
    else:
        collapsed_list = [[header[i] for i in y]]

    for i, item in enumerate(a):
        if counter_2 == 0:
            accumulator_text.append([a[i][ii] for ii in x])
            list_to_accumulate_doc_type = a[i][y[0]]
            list_to_accumulate_path = a[i][y[1]]

        if counter_2 > 0:
            list_to_check = [a[i][ii] for ii in x]
            list_to_accumulate_doc_type = a[i][y[0]]
            list_to_accumulate_path = a[i][y[1]]
            if list_to_check == accumulator_text[-1]:
                accumulator_text.append([a[i][ii] for ii in x])
                accumulator_doc.append(list_to_accumulate_doc_type)
                accumulator_path.append(list_to_accumulate_path)

            if list_to_check != accumulator_text[-1]:
                b = [accumulator_text[-1]]
                b.append(accumulator_doc)
                b.append(accumulator_path)
                collapsed_list.append(b)
                accumulator_text = [list_to_check]
                accumulator_doc = [list_to_accumulate_doc_type]
                accumulator_path = [list_to_accumulate_path]

        counter_2 += 1
    return collapsed_list

def order_path(c):
    """Takes a list with a gvkey and date and several paths and organizes them based on doc type """
    newnew_list = copy.deepcopy(c)
    for i, item in enumerate(c):
        d_type = []
        s_type = []
        if len(item[1]) > 1:
            for j in item[1]:
                a = j.split("_")
                d_type.append(a[0])
                s_type.append(a[-1])

            s_set = set(s_type)
            d_set = set(d_type)

            if len(s_set) == 1 and len(d_set) == 2 and len(item[1]) == 2:
                None
            if len(s_set) == 1 and len(d_set) == 2 and len(item[1]) > 2:
                i_want = d_type.index('AR')
                newnew_list[i][1] = [item[1][0]]
                newnew_list[i][1].extend([item[1][i_want]])
                newnew_list[i][2] = [item[2][0]]
                newnew_list[i][2].extend([item[2][i_want]])

            #if len(s_set) > 1 and len(d_set) == 2 and len(item[1]) > 2:
            if len(s_set) > 1 and len(item[1]) > 2:
                #print(item[1])
                #print(s_type)
                #print(d_type)
                mix = []
                for f, item2 in enumerate(d_type):
                    mixx = item2+s_type[f]
                    mix.append(mixx)

                try:
                    i_want = mix.index('AROLD')
                except:
                    i_want = 1000
                try:
                    in_want = mix.index('ARNEW')
                except:
                    in_want = 1000
                try:
                    i_want_1 = mix.index('10KOLD')
                except:
                    i_want_1 = 1000
                try:
                    in_want_1 = mix.index('10KNEW')
                except:
                    in_want_1 =1000
                print(i_want_1,in_want_1)
                if i_want_1 < 1000 and i_want < 1000:
                    newnew_list[i][1] = [item[1][i_want_1]]
                    newnew_list[i][1].extend([item[1][i_want]])
                    newnew_list[i][2] = [item[2][i_want_1]]
                    newnew_list[i][2].extend([item[2][i_want]])

                if i_want_1 == 1000 and i_want < 1000 and in_want_1 < 1000:
                    newnew_list[i][1] = [item[1][i_want]]
                    newnew_list[i][1].extend([item[1][in_want_1]])
                    newnew_list[i][2] = [item[2][i_want]]
                    newnew_list[i][2].extend([item[2][in_want_1]])

                if i_want_1 < 1000 and i_want == 1000 and in_want < 1000:
                    newnew_list[i][1] = [item[1][i_want_1]]
                    newnew_list[i][1].extend([item[1][in_want]])
                    newnew_list[i][2] = [item[2][i_want_1]]
                    newnew_list[i][2].extend([item[2][in_want]])

                if i_want_1 < 1000 and  in_want_1 < 1000 and i_want == 1000 and in_want == 1000:
                    newnew_list[i][1] = [item[1][i_want_1]]
                    newnew_list[i][1].extend([item[1][in_want_1]])
                    newnew_list[i][2] = [item[2][i_want_1]]
                    newnew_list[i][2].extend([item[2][in_want_1]])

                if i_want_1 == 1000 and  in_want_1 == 1000 and i_want < 1000 and in_want < 1000:
                    newnew_list[i][1] = [item[1][i_want]]
                    newnew_list[i][1].extend([item[1][in_want]])
                    newnew_list[i][2] = [item[2][i_want]]
                    newnew_list[i][2].extend([item[2][in_want]])
    return newnew_list

