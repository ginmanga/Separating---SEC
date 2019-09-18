# take a list of gkeys and other company varibles and the SEC filine information, and develop functions to retrieve
# the individual files and important parts of the files
import pandas as pd
import os
import Collect_Files_Funcs as cf
import re
import codecs
import copy


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

directory_junk = r"C:\Users\Panqiao\Documents\Research\SECO - DATA - COLLECTION\Individual Docs\junk"
#cf.sep_files(directory_sample, path_to_save)


#Now write script to separate Management discussion, consolidated balance sheet and notes..
directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/junk.csv"
paths = cf.folder_loop(directory_junk)[0]
sample = pd.read_csv(directory_sample, sep=",")

sample_small = sample[['gvkey', 'datadate', 'fyear', 'doc_type', "sec_type"]]
gvkey_list = sample_small['gvkey'].tolist()
fyear_list = sample_small['fyear'].tolist()

sec_type_list = [i.replace('[', '').replace(']', '')
                     .replace("\'", "").replace(" ", "").split(",")
                 for i in sample_small['sec_type']]

doc_type_list = [i.replace('[', '').replace(']', '')
                     .replace("\'", "").replace(" ", "").split(",")
                 for i in sample_small['doc_type']]
new_list = []
for i, item in enumerate(gvkey_list):
    new_list.append([item, fyear_list[i], doc_type_list[i], sec_type_list[i]])

for i in paths:
    #print(i.split("C:\Users\\Panqiao\\Documents\\Research\\SECO - DATA - COLLECTION\\Individual Docs\\junk\\""))
    print(os.path.splitext(i))

cf.splitall(paths[0])[8:11]
print(os.path.splitext(paths[0])[0])
path1 = [[i] for i in paths]
path2 = [[os.path.splitext(i)[0]] for i in paths]
#for i, item in enumerate(paths)
path2[0][0]

new_list = []
for i, item in enumerate(path1):
    a = cf.splitall(path2[i][0])[8:11]
    a.append(item[0])
    new_list.append(a)

for i in new_list:
    print(i[0:3], i[2].split("_"))

def collapse_list(a, x, y, header = 0):
    #a list to collapse
    #x group of variables used to collapase, indexes
    #y list of columns to keep
    # a collapsed list collpased, date from the first row and rest from the last
    counter_1 = 0
    counter_2 = 0
    accumulator_text = []
    accumulator_doc = []
    accumulator_path = []
    if header == 0:
        collapsed_list = []
    else:
        collapsed_list = [[header[i] for i in y]]
    #for i in range(len(a)):
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
                print(accumulator_text)
                accumulator_doc.append(list_to_accumulate_doc_type)
                accumulator_path.append(list_to_accumulate_path)
                #print(accumulator_list)

            if list_to_check != accumulator_text[-1]:

                #collapse_accumulator = [accumulator_list[0][0]]
                #collapse_accumulator.extend(accumulator_list[-1][1:])
                #collapsed_list.append(accumulator_text[-1])
                #collapsed_list.extend(accumulator_doc)
                #collapsed_list.extend(accumulator_path)
                b = [accumulator_text[-1]]
                b.append(accumulator_doc)
                b.append(accumulator_path)
                collapsed_list.append(b)
                accumulator_text = [list_to_check]
                accumulator_doc = [list_to_accumulate_doc_type]
                accumulator_path = [list_to_accumulate_path]

        counter_2 += 1

    return collapsed_list

c = collapse_list(new_list, x = [0,1], y = [2,3])
for i in c[-5][1]:
    print(i.split("_"))
c[0]
newnew_list = copy.deepcopy(c)

for i, item in enumerate(c):
    d_type = []
    s_type = []
    #if len(item[1]) == 1:
    #print(item)
    if len(item[1]) > 1:
        for j in item[1]:
            a = j.split("_")
            d_type.append(a[0])
            s_type.append(a[-1])

        s_set = set(s_type)
        d_set = set(d_type)

        if len(s_set) == 1 and len(d_set) == 2 and len(item[1]) == 2:
            None
            #print("Do nothing")
        if len(s_set) == 1 and len(d_set) == 2 and len(item[1]) > 2:
            i_want = d_type.index('AR')
            #print(item[1])
            newnew_list[i][1] = [item[1][0]]
            #print(item(1))
            newnew_list[i][1].extend([item[1][i_want]])
            newnew_list[i][2] = [item[2][0]]
            newnew_list[i][2].extend([item[2][i_want]])
            #print(newnew_list[i][1])
            #print(newnew_list[i][2])
        #if len(s_set) > 1 and len(d_set) == 2 and len(item[1]) > 2:
        if len(s_set) > 1 and len(item[1]) > 2:
            #print(d_type)
            print(item[1])
            #print(item[2])
            print(s_type)
            print(d_type)
            mix = []
            for f, item2 in enumerate(d_type):
                mixx = item2+s_type[f]
                mix.append(mixx)

            print(mix)
            #print(d_set)
            #print(s_set)
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
            if i_want_1 <1000 and i_want < 1000:
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

            #print("INDEX")
            #print(i_want)
            #print([item[1][i_want]])
            #print(newnew_list[i][1])
            #print(newnew_list[i][2])
            #print(item[1])
            #newnew_list[i][1] = [item[1][0]]
            #print(item(1))
            #newnew_list[i][1].extend([item[1][i_want]])
            #newnew_list[i][2] = [item[2][0]]
            #newnew_list[i][2].extend([item[2][i_want]])
            print(newnew_list[i][1])
            print(newnew_list[i][2])






for i in new_list:
    print(i)
len(new_list)