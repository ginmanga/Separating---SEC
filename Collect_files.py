# take a list of gkeys and other company varibles and the SEC filine information, and develop functions to retrieve
# the individual files and important parts of the files
import pandas as pd
import os
import Collect_Files_Funcs as cf
import re

#first go trhough docs again an get file line for first doc

directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
path = os.path.abspath(directory)
path = os.path.abspath(path)
#for i in path:
    #print(i)

a = cf.first_line(path)

directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
cf.write_file(directory, "first_line.txt", a, 'w')

#now collect the documents from the files

directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/test/test.csv"
directory_firstline = "C:/Users/Panqiao/Documents/Research/SEC Online - 05042017/All - SS/first_line.txt"
sample = pd.read_csv(directory_sample, sep=",")
firstline = pd.read_csv(directory_firstline, sep="\t")

sample_small = sample[['gvkey', 'datadate','fyear', 'doc_type',"sec_type",'path',
                       'line_start','line_end']]

#dates_CR = CR['datadate'].tolist()
gvkey_list = sample_small['gvkey'].tolist()
fyear_list = sample_small['fyear'].tolist()
doc_type_list = sample_small['doc_type'].tolist()
doc_type_list2 = [i.replace('[','').replace(']','')
                  .replace("\'","").replace(" ","").split(",")
                  for i in sample_small['doc_type']]

doc_type_list2 = [[i.replace('[','').replace(']','')]
                  for i in sample_small['doc_type']]

sec_type_list = sample_small['sec_type'].tolist()
path_list = sample_small['path'].tolist()
line_start_list = sample_small['line_start'].tolist()
line_end_list = sample_small['line_end'].tolist()

#fixt lists

#doc_type_list = [re.sub('[',"", i) for i in doc_type_list]
for i in doc_type_list2:
    a = i[0].replace("\'","").replace(" ","").split(",")
    print(a)
    #print(i.strip('[').strip(']').strip().split(','))
    #print(type(i))

#convert TO LIST OF LISTS
list_all = [[i] for i in gvkey_list]
for i, item in enumerate(gvkey_list):
    list_all[i].append(fyear_list[i])
    list_all[i].append(doc_type_list[i])
