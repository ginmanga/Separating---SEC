# take a list of gkeys and other company varibles and the SEC filine information, and develop functions to retrieve
# the individual files and important parts of the files
import pandas as pd
import os
import Collect_Files_Funcs as cf
import re
import codecs
import copy, unicodedata


#first go trhough docs again an get file line for first doc

directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
path = os.path.abspath(directory)
path = os.path.abspath(path)
#for i in path:
    #print(i)

a = cf.first_line(path)
########################
#No need to run anymore#
#directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
#cf.write_file(directory, "first_line.txt", a, 'w')

#now collect the documents from the files

directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/test/test.csv"
directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/junk.csv"
directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/ig_all.csv"
path_to_save = r'C:\Users\Panqiao\Documents\Research\SECO - DATA - COLLECTION\Individual Docs\Investment Grade'




#Now write script to separate Management discussion, consolidated balance sheet and notes..
directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/junk.csv"

directory_junk = r"C:\Users\Panqiao\Documents\Research\SECO - DATA - COLLECTION\Individual Docs\junk" #cf.sep_files(directory_sample, path_to_save)
paths = cf.folder_loop(directory_junk)[0]
sample = pd.read_csv(directory_sample, sep=",")


def separate_paths(paths):
    """Takes path and """

    new_list = []
    path1 = [[i] for i in paths]
    path2 = [[os.path.splitext(i)[0]] for i in paths]

    for i, item in enumerate(path1):
        a = cf.splitall(path2[i][0])[8:11]
        a.append(item[0])
        new_list.append(a)
    return new_list


for i in separate_paths(paths):
    print(i[0:3], i[2].split("_"))

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
                #print(accumulator_list)

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

#c = collapse_list(new_list, x = [0,1], y = [2,3])

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

new_path = cf.separate_paths(paths)

collapse_list = cf.collapse_list(new_path, x = [0,1], y = [2,3])

new_list = cf.order_path(collapse_list)

def parse_tabelu(tabelu):
    mngd = 'MANAGEMENT\'S DISCUSSION AND ANALYSIS'
    bs = 'CONS. BALANCE SHEETS'
    ntfs = 'NOTES TO FINANCIAL STATEMENTS'
    sch9 = 'SCH IX'
    find_all = [0, 0, 0]
    find_all_lines = [0, 0, 0]
    sch9_line = -1
    for i, item in enumerate(tabelu):
        if item.find(mngd) != -1:
            find_all[0] = 1
            find_all_lines[0] = i#item.find(mngd)
        if item.find(bs) != -1:
            find_all[1] = 1
            find_all_lines[1] = i#item.find(bs)
        if item.find(ntfs) != -1:
            find_all[2] = 1
            find_all_lines[2] = i#item.find(ntfs)
        if item.find(sch9) != -1:
            sch9_line = i #item.find(sch9)
    #if find_all ==  [1, 1, 1]:
        #print("found all")
    #if sch9_line > -1:
        #print("found Schedule IX")
    return find_all, find_all_lines, sch9_line

def est_pag_numbs(a, last_line, tabelu, lines):
    """find relation between page numbering and table content number"""
    # a = line on table of contents for item 7
    #normed_last = unicodedata.normalize('NFKD', tabelu[last_line]).replace('\n', '')
    normed_mgt = unicodedata.normalize('NFKD', tabelu[a]).replace('\n', '').split()[-1]
    bspags = normed_mgt.split('-')
    #print('HHHHHHHEEEEE')
    #print(unicodedata.normalize('NFKD', tabelu[a]).replace('\n', '').split())
    #print(bspags)
    #print(last_line.split().index('[*1]'))
    #print(bspags)
    try:
        last_line.split().index('[*1]')
        #for i in lines:

    except:
        print(last_line.split())
    #if last_line.split())[0]
    #last_line.split()


def find_tc(lines, type_doc, sec_type):
    page_terms = ['[*1]','[HARDCOPY PAGE 1]', '[HARDCOPY PAGE H1]']
    line_start = 0
    line_end = 0
    found_none = 0
    for i, item in enumerate(lines):
        if item.find("TABLE OF CONTENTS") is not -1:
            line_start = i
        if any(n in item for n in page_terms):
            tabelu = lines[line_start:i+1]
            last_line = item
            #print("LAAAAAAA")
            #print(last_line)
            break
    find_all, find_all_lines, sch9_line = parse_tabelu(tabelu)
    print(find_all)
    if find_all[0] == 0: #== [0, 0, 0]:
        print("TROOOOOOOOOOOOOOOOOOOOOOOOOOOO")
        est_pag_numbs(find_all_lines[0], last_line, tabelu, lines)
        found_none = 1
    if find_all[0] == 1:
        None
        #est_pag_numbs(find_all_lines[0], last_line, tabelu, lines)
    if sch9_line > -1:
        normed = unicodedata.normalize('NFKD', tabelu[sch9_line]).replace('\n','')
        sch9_page = normed.split()[-1]
        #print
        #print(tabelu[sch9_line].split('SCH IX'))
        #print(tabelu[-1])
    #if find_all == [1, 1, 1]:



for i, item in enumerate(new_list):
    #print(item[1], len(item[1]))
    print(item)
    if len(item[1]) == 1:
        #print(item)
        type_doc = item[1][0].split('_')[0]
        type_sec = item[1][0].split('_')[-1]
        #print(type_doc, type_sec)
        #fhand = open(item[2][0]).readlines()
        lines = open(item[2][0]).readlines()
        find_tc(lines, type_doc, type_sec)
        #if type_doc == "10K":
            #lines = fhand.readlines()
            #for i in lines:
                #print(lines)
        #if type_doc == "AR":
            #print("that")
        #print(type_doc)
    if len(item[1]) == 2:
        type_doc_1 = item[1][0].split('_')[0]
        type_doc_2 = item[1][1].split('_')[0]

#now separate the individual components