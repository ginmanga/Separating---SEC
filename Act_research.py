#here we start the actual reserarch with matched file

import datetime, csv
import pandas as pd
import pandasql as ps
import numpy as np

AA_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/FINAL/final_match_a.txt'
CT = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/ccmxpf_linktable.sas7bdat'
CT1 = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/ccmxpf_linktable.txt'
AA_FINAL = pd.read_csv(AA_FINAL, sep=",")
AA_FINAL = AA_FINAL.drop(columns=['Unnamed: 0','mdatadate','gvkeydate'])
#shorten keep only 1985-1995 how to do it?
#df = df.drop(df[df.score < 50].index)
AA_FINAL_S = AA_FINAL
AA_FINAL_S = AA_FINAL_S.drop(AA_FINAL_S[AA_FINAL_S.fyear >= 1995].index)
AA_FINAL_S = AA_FINAL_S.drop(AA_FINAL_S[AA_FINAL_S.fyear < 1985].index)
AA_FINAL_S = AA_FINAL_S.drop(columns=['oldate','LINKTYPE','LPERMNO','LINKDT','LINKENDDT','doc_Date','indfmt','consol'])
AA_FINAL_S.describe
AA_FINAL_S.dtypes

### Build one sample using fiscal year pre: 1986-1989 post: 1990-1993
AA_FINAL_S_FY = AA_FINAL_S
AA_FINAL_S_FY = AA_FINAL_S_FY.drop(AA_FINAL_S_FY[AA_FINAL_S_FY.fyear >= 1994].index)
AA_FINAL_S_FY  = AA_FINAL_S_FY .drop(AA_FINAL_S_FY [AA_FINAL_S_FY.fyear <= 1985].index)
# Keep only companies have at least one ob in pre and post
AA_FINAL_S_FY['pre'] = np.where(AA_FINAL_S_FY['fyear'] < 1990, 1, 0)
AA_FINAL_S_FY['post'] = np.where(AA_FINAL_S_FY['fyear'] >= 1990, 1, 0)
#let's do some sQL do group by gvkey an sum pre and post, and keep


def sas_to_dataframe(file_path):
    """
    Read in a SAS dataset (sas7bdat) with the proper encoding
    :param file_path: location of SAS dataset to be read in
    :return: pandas dataframe with contents of SAS dataset
    """
    return pd.read_sas(file_path, format="sas7bdat", encoding="iso-8859-1")
CR = sas_to_dataframe(CT)
CR.to_csv(CT1)