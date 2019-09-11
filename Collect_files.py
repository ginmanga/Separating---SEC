# take a list of gkeys and other company varibles and the SEC filine information, and develop functions to retrieve
# the individual files and important parts of the files
import pandas as pd
import os
import Collect_Files_Funcs as cf
import re
import codecs



#first go trhough docs again an get file line for first doc

directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
path = os.path.abspath(directory)
path = os.path.abspath(path)
#for i in path:
    #print(i)

a = cf.first_line(path)
#No need to run anymore#
#directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
#cf.write_file(directory, "first_line.txt", a, 'w')

#now collect the documents from the files

directory_sample = "C:/Users/Panqiao/Documents/Research/SECO - DATA - COLLECTION/test/test.csv"
directory_firstline = "C:/Users/Panqiao/Documents/Research/SEC Online - 05042017/All - SS/first_line.txt"
sample = pd.read_csv(directory_sample, sep=",")
firstline = pd.read_csv(directory_firstline, sep="\t") # line where files start need to add to line start

sample_small = sample[['gvkey', 'datadate','fyear', 'doc_type',"sec_type",'path',
                       'line_start','line_end']]
firstline = firstline[['path','first_line']]
#dates_CR = CR['datadate'].tolist()
gvkey_list = sample_small['gvkey'].tolist()
fyear_list = sample_small['fyear'].tolist()
#doc_type_list = sample_small['doc_type'].tolist()
doc_type_list = [i.replace('[','').replace(']','')
                  .replace("\'","").replace(" ","").split(",")
                  for i in sample_small['doc_type']]

sec_type_list = [i.replace('[','').replace(']','')
                  .replace("\'","").replace(" ","").split(",")
                  for i in sample_small['sec_type']]
path_list = [i.replace('[','').replace(']','')
                  .replace("\'","").replace(" ","").split(",")
                  for i in sample_small['path']]

path_list2 = [i.replace('[','').replace(']','')
                  .replace("\'","").split(",")
                  for i in sample_small['path']]


line_start_list = [i.replace('[','').replace(']','')
                  .replace("\'","").replace(" ","").split(",")
                  for i in sample_small['line_start']]
line_end_list = [i.replace('[','').replace(']','')
                  .replace("\'","").replace(" ","").split(",")
                  for i in sample_small['line_end']]

firstline_line = firstline['first_line'].tolist()
firstline_path = firstline['path'].tolist()
firstline_path2 = [os.path.abspath(i) for i in firstline_path]


firstline_list = []
for i, item in enumerate(firstline_path):
    firstline_list.append([item,firstline_line[i]])

#fixt lists

#list of things to do
# For each document, find file line start and extract document

#doc_type_list = [re.sub('[',"", i) for i in doc_type_list]
#convert TO LIST OF LISTS
#build a list of lists with all the required information per document
#first math doc_path with first line


new_list = []
for i, item in enumerate(path_list2):
    for j, item_2 in enumerate(item):
        new_list.append([item_2.strip(), gvkey_list[i], fyear_list[i],
                         doc_type_list[i][j], sec_type_list[i][j],
                         line_start_list[i][j], line_end_list[i][j]])


for i, item in enum(new_list):
    #print(i[0])
    #print(type(i[0]))
    try:
        a = i[0]
        #a = os.path.abspath(i[0])
        print(firstline_path2.index(os.path.abspath(i[0])))
        print(firstline_line[firstline_path2.index(os.path.abspath(i[0]))])
    except:
        continue

firstline_list.index('C:\\Users\\Panqiao\\Documents\\Research\\SEC Online - 05042017\\All\\1988\\AMS - December - 1988 - 1- 500.TXT')
firstline_path.index('C:\\Users\\Panqiao\\Documents\\Research\\SEC Online - 05042017\\All\\1988\\AMS - December - 1988 - 1- 500.TXT')
firstline_path[34]
type(firstline_path[34])
firstline_path.index('C:\\Users\\Panqiao\\Documents\\Research\\SEC Online - 05042017\\All\\1990\\AMS - March - 1990 - 501- 850.TXT')
firstline_path.index(r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All\1994\AMS - March - 1994 - 1 - 100.TXT')
firstline_path.index(r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All\1987\AMS - December - 1987 - 1- 500.TXT')

firstline_path2.index((os.path.abspath(new_list[2][0])))

print(len('C:\\Users\\Panqiao\\Documents\\Research\\SEC Online - 05042017\\All\\1994\\AMS - March - 1994 - 1 - 100.TXT'))
print(len(r'C:\\Users\\Panqiao\\Documents\\Research\\SEC Online - 05042017\\All\\1994\\AMS - March - 1994 - 1 - 100.TXT'))
for i in new_list:
    print(i[0])

pd.dataframe

C:\\Users\\Panqiao\\Documents\\Research\\SEC Online - 05042017\\All\\1994\\AMS - March - 1994 - 1 - 100.TXT