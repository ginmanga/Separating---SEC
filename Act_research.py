#here we start the actual reserarch with matched file

import datetime, csv
import pandas as pd
import pandasql as ps
import numpy as np
import SQL.py

AA_FINAL_C = 'C:/Users/Panqiao/Documents/Research/SS - All/FINAL/final_match.txt'
AA_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/FINAL/final_match_a_V1.txt'
CT = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/ccmxpf_linktable.sas7bdat'
CT1 = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/ccmxpf_linktable.txt'


AA_FINAL_COMPSTAT = pd.read_csv(AA_FINAL_C, sep=",")
AA_FINAL_COMPSTAT.describe
AA_FINAL_LINKED = pd.read_csv(AA_FINAL, sep=",")
AA_FINAL.describe

AA_FINAL = pd.read_csv(AA_FINAL, sep=",")

AA_FINAL = AA_FINAL.drop(columns=['Unnamed: 0'])
AA_FINAL_COMPSTAT = AA_FINAL.drop(columns=['Unnamed: 0'])
AA_FINAL_LINKED = AA_FINAL.drop(columns=['Unnamed: 0','mdatadate','gvkeydate'])
#shorten keep only 1985-1995 how to do it?
#df = df.drop(df[df.score < 50].index)
AA_FINAL_S = AA_FINAL
AA_FINAL_S = AA_FINAL_S.drop(AA_FINAL_S[AA_FINAL_S.fyear >= 1995].index)
AA_FINAL_S = AA_FINAL_S.drop(AA_FINAL_S[AA_FINAL_S.fyear < 1985].index)

AA_FINAL_S= AA_FINAL_S.drop(columns=['doc_Date'])
AA_FINAL_S_COMPSTAT = AA_FINAL_S.drop(columns=['doc_Date'])
AA_FINAL_S_LINKED = AA_FINAL_S.drop(columns=['oldate','LINKTYPE','LPERMNO','LINKDT','LINKENDDT','doc_Date','indfmt','consol'])

AA_FINAL_S.describe
AA_FINAL_S.dtypes

### Build one sample using fiscal year pre: 1986-1989 post: 1990-1993
AA_FINAL_S_FY = AA_FINAL_S
AA_FINAL_S_FY = AA_FINAL_S_FY.drop(AA_FINAL_S_FY[AA_FINAL_S_FY.fyear >= 1994].index)
AA_FINAL_S_FY  = AA_FINAL_S_FY.drop(AA_FINAL_S_FY [AA_FINAL_S_FY.fyear <= 1985].index)
# Keep only companies have at least one ob in pre and post

AA_FINAL_S_FY = AA_FINAL_S_FY[np.isfinite(AA_FINAL_S_FY['fyear'])]
AA_FINAL_S_FY = AA_FINAL_S_FY.astype({'fyear': 'int64'})
AA_FINAL_S_FY['pre'] = np.where(AA_FINAL_S_FY['fyear'] < 1990, 1, 0)
AA_FINAL_S_FY['post'] = np.where(AA_FINAL_S_FY['fyear'] >= 1990, 1, 0)
AA_FINAL_S_FY.drop_duplicates(keep=False,inplace=True)
AA_FINAL_S_FY['hfile'] = np.where(AA_FINAL_S_FY['doc_Date_1'].isna(), 0, 1)
#let's do some sQL do group by gvkey an sum pre and post, and keep

temp = AA_FINAL_S_FY[['gvkey','datadate', 'fyear', 'splticrm', 'spsdrm' ,'spsticrm','pre','post','hfile']]

#aggregate pre and post keep only those with at least one obs
sqlcode = '''
select gvkey,
sum(pre), sum(post)
from temp
group by gvkey
'''
sql_test = ps.sqldf(sqlcode, locals())

AA_FINAL_S_FY = pd.merge(AA_FINAL_S_FY, sql_test, left_on=['gvkey'], right_on = ['gvkey'], how='left')
#drop where pre and post are 0
temp = AA_FINAL_S_FY.copy()
temp = temp.drop(temp[temp['sum(pre)'] == 0].index)
temp = temp.drop(temp[temp['sum(post)'] == 0].index)

#drop if sum(pre) or sum(post) == 0
#handle unrated, junk and ig
#unrated splticrm spsdrm, spsticrm is nan
#junk junk
#unrated year
#temp['splticrm'].isna()
#temp['spsdrm']
#temp['spsticrm']

ig = ['AAA+','AAA','AAA-','AA+', 'AA', 'AA-','A+','A','A-','A-','BBB+','BBB','BBB-']
lig = ['BBB+','BBB','BBB-']
hjunk = ['BB+','BB','BB-']
junk = ['BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC+','CC','CC-']
default = ['C','D']
temp['unrated'] = np.where(np.all([temp['splticrm'].isna(), temp['spsdrm'].isna(), temp['spsticrm'].isna()], axis=0), 1, 0)
#temp['junk'] = np.where(temp['splticrm'] in junk, 1, 0)

temp['junk'] = [1 if x in junk else 0 for x in temp['splticrm']]
temp['hjunk'] = [1 if x in hjunk else 0 for x in temp['splticrm']]
temp['inv_grade'] = [1 if x in ig else 0 for x in temp['splticrm']]
temp['lowinv_grade'] = [1 if x in lig else 0 for x in temp['splticrm']]
temp.dtypes
temp.sum()

temp_nf = temp.copy()
indexnames= temp_nf[(temp_nf['sic'] >= 6000) & (temp_nf['sic'] < 7000)].index
temp_nf.drop(indexnames, inplace=True)
temp_nf.sum(axis=0, skipna = True)
temp_nf = temp_nf[['gvkey','datadate','hfile','pre','post','unrated','junk','inv_grade']]

#first sum pre , post, junk, inv_grade
#first groupings:
#1. unrated never rated
#2. junk at least one junk in pre and post and no inv_grade: junk
#3. Inv_grade always rated inv_grade

sqlcode = '''
select gvkey,
sum(pre), sum(post), sum(junk*pre), sum(junk*post), sum(inv_grade*pre), sum(inv_grade*post), sum(hfile*pre), sum(hfile*post)
from temp_nf
group by gvkey
'''
sql_test = ps.sqldf(sqlcode, locals())
sql_test['unrated_1'] = [1 if sql_test['sum(junk*pre)'] == 0 & sql_test['sum(junk*post)'] == 0
                             & sql_test['sum(inv_grade*post)'] == 0 & sql_test['sum(inv_grade*pre)'] == 0 else 0]

sql_test['unrated_1'] = [1 if x == 0 else 1 for x in [sql_test['sum(junk*pre)'], sql_test['sum(junk*post)'],sql_test['sum(inv_grade*post)'],
                                               sql_test['sum(inv_grade*pre)']]]

temp['unrated'] = np.where(np.all([temp['splticrm'].isna(), temp['spsdrm'].isna(), temp['spsticrm'].isna()], axis=0), 1, 0)

sql_test['unrated_1'] = np.where(np.all([sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0,
                                         sql_test['sum(inv_grade*post)'] == 0, sql_test['sum(inv_grade*pre)'] == 0], axis=0), 1, 0)

sql_test['unrated_1_hfile'] = np.where(np.all([sql_test['unrated_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0], axis=0), 1, 0)


sql_test['junk_1']= np.where(np.all([sql_test['sum(junk*pre)'] > 0, sql_test['sum(junk*post)'] > 0,
                                         sql_test['sum(inv_grade*post)'] == 0, sql_test['sum(inv_grade*pre)'] == 0], axis=0), 1, 0)

sql_test['junk_1_hfile']= np.where(np.all([sql_test['junk_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0], axis=0), 1, 0)

sql_test['inv_grade_1']= np.where(np.all([sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0,
                                         sql_test['sum(inv_grade*post)'] > 0, sql_test['sum(inv_grade*pre)'] > 0], axis=0), 1, 0)

sql_test['inv_grade_1_hfile']= np.where(np.all([sql_test['inv_grade_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0], axis=0), 1, 0)

sql_test.sum(axis=0, skipna = True)

#start calculating values compare with LR(2010)





def sas_to_dataframe(file_path):
    """
    Read in a SAS dataset (sas7bdat) with the proper encoding
    :param file_path: location of SAS dataset to be read in
    :return: pandas dataframe with contents of SAS dataset
    """
    return pd.read_sas(file_path, format="sas7bdat", encoding="iso-8859-1")
CR = sas_to_dataframe(CT)
CR.to_csv(CT1)

np.all([0,0],axis=0)
np.all([1-1.0,1],axis=0)
np.all([1,1],axis=0)
np.all([1,1],axis=0)
np.all([True,True],axis=0)
np.all([True,False],axis=0)
print(sql_test.loc[[1]])
sql_test[0]
for i in range(10):
    print(sql_test.loc[[i,'sum(inv_grade*post']])