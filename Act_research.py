#here we start the actual reserarch with matched file

import datetime, csv
import pandas as pd
import pandasql as ps
import numpy as np
AA_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/FINAL/final_match.txt'
CT = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/ccmxpf_linktable.sas7bdat'
CT1 = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/ccmxpf_linktable.txt'
AA_FINAL = pd.read_csv(AA_FINAL, sep=",")

def sas_to_dataframe(file_path):
    """
    Read in a SAS dataset (sas7bdat) with the proper encoding
    :param file_path: location of SAS dataset to be read in
    :return: pandas dataframe with contents of SAS dataset
    """
    return pd.read_sas(file_path, format="sas7bdat", encoding="iso-8859-1")
CR = sas_to_dataframe(CT)
CR.to_csv(CT1)