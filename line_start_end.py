#from id_file_data get line start and end of each document


import datetime, csv
import pandas as pd
import pandasql as ps
import numpy as np


sec_file_path = "C:/Users/Panqiao/Documents/Research/SS - All/SS/id_file_data.txt"
SEC = pd.read_csv(sec_file_path, sep="\t", usecols=[0, 1, 2, 4, 5, 7, 16, 17])

SEC_dc = SEC['doc_number'].tolist()
SEC_line = SEC['line_number'].tolist()
SEC_all = []
for i in range(len(SEC_dc)):
    SEC_all.append([SEC_dc[i], SEC_line[i]])

SEC_lines = []
for i, item in enumerate(SEC_all):
    #print(i,item)
    #if item[0] == 1:
        #print(item)
        #SEC_lines.append([item[1], SEC_all[i+1][1]])
        #print(SEC_lines)
    try:
        if SEC_all[i+1] == 1:
            SEC_lines.append([item[1], 'end'])
        if SEC_all[i+1] != 1:
            SEC_lines.append([item[1], SEC_all[i+1][1]])
    except:
        SEC_lines.append([item[1], 'end'])

line_start = [i[0] for i in SEC_lines]
line_end = [i[1] for i in SEC_lines]

SEC['line_start'] = line_start
SEC['line_end'] = line_end

SEC_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/FINAL/sec_gvkey - v2.txt'
SEC_FILES = pd.read_csv(SEC_FINAL, sep="\t") #, usecols=[0, 1, 2, 6, 7, 9, 10])
#macth on path doc_type filing_type filing_date document_date path
len(SEC_FILES)
len(SEC)
#SEC path DOC_TYPE filing_type filing_date document_date path
#SECfiles sec_type DOC_TYPE file_date doc_Date path

SEC_FILES['doc_number'] = SEC['doc_number']
SEC_FILES['line_start'] = SEC['line_start']
SEC_FILES['line_end'] = SEC['line_end']

SEC_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/FINAL/sec_gvkey - v3.txt'
SEC_FILES.to_csv(SEC_FINAL, index=False)

#no need for this, continues on bs-data

SEC['filing_date'] = pd.to_datetime(SEC['filing_date'])
SEC['document_date'] = pd.to_datetime(SEC['document_date'])

SEC_FILES = SEC_FILES.dropna(how='all')
SEC_FILES = SEC_FILES.dropna(subset=['gvkey'])

SEC_FILES = SEC_FILES.astype({'gvkey': 'int64', 'doc_Date':'int64', 'file_date':'int64'})
SEC_FILES = SEC_FILES.astype({'doc_Date':'str', 'file_date':'str'})
SEC_FILES['doc_Date'] = pd.to_datetime(SEC_FILES['doc_Date'])
SEC_FILES['file_date'] = pd.to_datetime(SEC_FILES['file_date'])

SEC_FILES = pd.merge(SEC_FILES,
                  SEC,
                  left_on=['sec_type','doc_type','file_date','doc_Date','path', 'conm'],
                  right_on = ['DOC_TYPE','filing_type','filing_date','document_date','path','COMN'], how='left')

SEC.dtypes
SEC_FILES.dtypes