# take a list of gkeys and other company varibles and the SEC filine information, and develop functions to retrieve
# the individual files and important parts of the files
import pandas as pd
import os


#first go trhough docs again an get file line for first doc

directory = r'C:\Users\Panqiao\Documents\Research\SEC Online - 05042017\All'
path = os.path.abspath(directory)
path = os.path.abspath(path)
#for i in path:
    #print(i)

def folder_loop(path):
    """Loops through contents of a folder
    saves file path, keep only text files"""
    req_paths = []
    for path, dirs, files in os.walk(path):
        req_paths.extend([os.path.join(path, i) for i in files])
    req_paths_doc = [i for i in req_paths if os.path.splitext(i)[1] == ".doc" or os.path.splitext(i)[1] == ".DOC"]
    req_paths = [i for i in req_paths if os.path.splitext(i)[1] == ".txt" or os.path.splitext(i)[1] == ".TXT"]
    return req_paths, req_paths_doc


def first_line(path):
    """Go through each file and record file line where the first document starts
    make a list with path and line start number"""
    req_paths, req_paths_doc = folder_loop(path)  # creates lists of paths
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
                    print(filename)
                    print(line)
                    print(line_count)
                    break
            line_count += 1

first_line(path)

for i in paths:
    print(i)
for i in paths_doc:
    print(i)
