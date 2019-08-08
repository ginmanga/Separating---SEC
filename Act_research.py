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
ratings = ['AAA+','AAA','AAA-','AA+', 'AA', 'AA-','A+','A','A-','A-','BBB+','BBB','BBB-',
           'BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC+','CC','CC-','C']

ig = ['AAA+','AAA','AAA-','AA+', 'AA', 'AA-','A+','A','A-','A-','BBB+','BBB','BBB-']
hig = ['AAA+','AAA','AAA-','AA+', 'AA', 'AA-','A+','A','A-','A-']
BBB = ['BBB+','BBB','BBB-']
hjunk = ['BB+','BB','BB-']
junk = ['BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC+','CC','CC-','C']
default = ['D']
CP = ['A-1+','A-1','A-2','A-3','B']
CP_h = ['A-1+','A-1']
CP_l = ['A-2','A-3','B']
temp['unrated'] = np.where(np.all([temp['splticrm'].isna(), temp['spsdrm'].isna(), temp['spsticrm'].isna()], axis=0), 1, 0)
#temp['junk'] = np.where(temp['splticrm'] in junk, 1, 0)

temp['rating'] = temp[['splticrm']]
temp.loc[temp['rating'].isna(),'rating'] = temp['spsdrm']

temp['rated'] = [1 if x in ratings else 0 for x in temp['rating']]
temp['junk'] = [1 if x in junk else 0 for x in temp['rating']]
temp['hjunk'] = [1 if x in hjunk else 0 for x in temp['rating']]
temp['inv_grade'] = [1 if x in ig else 0 for x in temp['rating']]
temp['lowinv_grade'] = [1 if x in BBB else 0 for x in temp['rating']]
temp['hinv_grade'] = [1 if x in hig else 0 for x in temp['rating']]
temp['CP_A'] = [1 if x in CP else 0 for x in temp['spsticrm']]
temp['CP_H'] = [1 if x in CP_h else 0 for x in temp['spsticrm']]
temp['CP_L'] = [1 if x in CP_l else 0 for x in temp['spsticrm']]
temp.dtypes
temp.sum()

temp_nf = temp.copy()
indexnames= temp_nf[(temp_nf['sic'] >= 6000) & (temp_nf['sic'] < 7000)].index
temp_nf.drop(indexnames, inplace=True)
temp_nf.sum(axis=0, skipna = True)
temp_nf = temp_nf[['gvkey','datadate','hfile','pre','post','unrated','rated',
                   'junk', 'inv_grade','hjunk','lowinv_grade','hinv_grade',
                   'CP_A','CP_H','CP_L']]

#first sum pre , post, junk, inv_grade
#first groupings:
#1. unrated never rated
#2. junk at least one junk in pre and post and no inv_grade: junk
#3. Inv_grade always rated inv_grade

sqlcode = '''
select gvkey,
sum(pre), sum(post), sum(junk*pre), sum(junk*post), sum(inv_grade*pre), sum(inv_grade*post), 
sum(hfile*pre), sum(hfile*post), sum(hjunk*pre),  sum(hjunk*post), sum(lowinv_grade), 
sum(lowinv_grade*pre), sum(lowinv_grade*post), sum(hinv_grade), sum(hinv_grade*pre), sum(hinv_grade*post),
sum(CP_A), sum(CP_A*pre), sum(CP_A*post), sum(CP_H*pre), sum(CP_H*post), sum(CP_L*pre), sum(CP_L*post),
sum(rated)
from temp_nf
group by gvkey
'''
sql_test = ps.sqldf(sqlcode, locals())

#sql_test['unrated_1'] = [1 if sql_test['sum(junk*pre)'] == 0 & sql_test['sum(junk*post)'] == 0
                             #& sql_test['sum(inv_grade*post)'] == 0 & sql_test['sum(inv_grade*pre)'] == 0 else 0]

#sql_test['unrated_1'] = [1 if x == 0 else 1 for x in [sql_test['sum(junk*pre)'], sql_test['sum(junk*post)'],sql_test['sum(inv_grade*post)'],
                                               #sql_test['sum(inv_grade*pre)']]]

#temp['unrated'] = np.where(np.all([temp['splticrm'].isna(), temp['spsdrm'].isna(), temp['spsticrm'].isna()], axis=0), 1, 0)
##### Make variables grouping into unrated, junk and high yield
sql_test['unrated_1'] = np.where(np.all([sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0,
                                         sql_test['sum(inv_grade*post)'] == 0, sql_test['sum(inv_grade*pre)'] == 0],
                                        axis=0), 1, 0)

sql_test['unrated_1_hfile'] = np.where(np.all([sql_test['unrated_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                              axis=0), 1, 0)


sql_test['junk_1']= np.where(np.all([sql_test['sum(junk*pre)'] > 0, sql_test['sum(junk*post)'] > 0,
                                         sql_test['sum(inv_grade*post)'] == 0, sql_test['sum(inv_grade*pre)'] == 0],
                                         axis=0), 1, 0)

sql_test['junk_1_hfile']= np.where(np.all([sql_test['junk_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                          axis=0), 1, 0)

sql_test['inv_grade_1']= np.where(np.all([sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0,
                                         sql_test['sum(inv_grade*post)'] > 0, sql_test['sum(inv_grade*pre)'] > 0],
                                         axis=0), 1, 0)

sql_test['inv_grade_1_hfile']= np.where(np.all([sql_test['inv_grade_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)


sql_test['hjunk_1']= np.where(np.all([sql_test['sum(hjunk*pre)'] > 0, sql_test['sum(hjunk*post)'] > 0,
                                         sql_test['sum(inv_grade*post)'] == 0, sql_test['sum(inv_grade*pre)'] == 0],
                                         axis=0), 1, 0)

sql_test['hjunk_1_hfile']= np.where(np.all([sql_test['hjunk_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)

sql_test['lowinv_grade_1']= np.where(np.all([sql_test['sum(lowinv_grade*pre)'] > 0, sql_test['sum(lowinv_grade*post)'] > 0,
                                        sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0,
                                        sql_test['sum(hinv_grade*pre)'] == 0, sql_test['sum(hinv_grade*post)'] == 0   ],
                                        axis=0), 1, 0)

sql_test['lowinv_grade_1_hfile']= np.where(np.all([sql_test['lowinv_grade_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)
###rated but not classified into any of the above groups

sql_test['rated_nc']= np.where(np.all([sql_test['sum(rated)'] > 0,
                                        sql_test['junk_1'] == 0, sql_test['inv_grade_1'] == 0],
                                        axis=0), 1, 0)

sql_test['rated_nc_hfile']=  np.where(np.all([sql_test['rated_nc'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)


#### Commercial paper ####
sql_test['CP_ALL']= np.where(np.all([sql_test['sum(CP_A)'] > 0],
                                         axis=0), 1, 0)
sql_test['CP_ALL_BOTH']=  np.where(np.all([sql_test['sum(CP_A*pre)'] > 0, sql_test['sum(CP_A*post)'] > 0,
                                         sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0],
                                         axis=0), 1, 0) #CP both, but never junk rated
sql_test['CP_H_BOTH']=  np.where(np.all([sql_test['sum(CP_H*pre)'] > 0, sql_test['sum(CP_H*post)'] > 0,
                                         sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0],
                                         axis=0), 1, 0) #CP high both, but never junk rated

sql_test['CP_L_BOTH']=  np.where(np.all([sql_test['sum(CP_L*pre)'] > 0, sql_test['sum(CP_L*post)'] > 0,
                                         sql_test['sum(junk*pre)'] == 0, sql_test['sum(junk*post)'] == 0],
                                         axis=0), 1, 0) #CP L both, but never junk rated

### have file for each

sql_test['CP_ALL_1_hfile']= np.where(np.all([sql_test['CP_ALL'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)

sql_test['CP_ALL_BOTH_1_hfile']= np.where(np.all([sql_test['CP_ALL_BOTH'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)
sql_test['CP_H_BOTH_1_hfile']= np.where(np.all([sql_test['CP_H_BOTH'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)

sql_test['CP_L_BOTH_1_hfile']= np.where(np.all([sql_test['CP_L_BOTH'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         sql_test['sum(hfile*post)'] > 0],
                                         axis=0), 1, 0)
#### End Commercial paper

sql_test['junk_1_total_obs'] = sql_test['junk_1']*sql_test['sum(pre)']+sql_test['junk_1']*sql_test['sum(post)']
sql_test['ig_1_total_obs'] = sql_test['inv_grade_1']*sql_test['sum(pre)']+sql_test['inv_grade_1']*sql_test['sum(post)']
sql_test['unrated_1_total_obs'] = sql_test['unrated_1']*sql_test['sum(pre)']+sql_test['unrated_1']*sql_test['sum(post)']
sql_test.sum(axis=0, skipna = True)

sql_test_small = sql_test.copy()
sql_test_small = sql_test_small.drop(columns=['sum(junk*pre)', 'sum(junk*post)', 'sum(inv_grade*pre)',
                                             'sum(inv_grade*post)', 'sum(hjunk*pre)', 'sum(hjunk*post)',
                                             'sum(lowinv_grade)', 'sum(lowinv_grade*pre)', 'sum(lowinv_grade*post)',
                                             'sum(hinv_grade)', 'sum(hinv_grade*pre)', 'sum(hinv_grade*post)',
                                             'sum(rated)', 'sum(CP_A)', 'sum(CP_A*pre)', 'sum(CP_A*post)',
                                             'sum(CP_H*pre)', 'sum(CP_H*post)', 'sum(CP_L*pre)', 'sum(CP_L*post)'])

sql_test_small_temp = sql_test_small.loc[sql_test_small['junk_1'] == 1]

print(sql_test_small.loc[sql_test_small['junk_1'] == 1])
obs_junk_pre = sql_test_small_temp['sum(hfile*pre)'].tolist()

#merge and save to file, save the collapsed file
sql_test_path = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/collapsed_v1.txt'
nf_path = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/not_collapsed_v1.txt'

sql_test_small.to_csv(sql_test_path)
sql_test_small_fm = sql_test_small.drop(columns=['junk_1_total_obs','ig_1_total_obs','unrated_1_total_obs'])
sql_test_small_fm = sql_test_small_fm.rename(columns = {'sum(pre)':'tot_pre','sum(post)':'tot_post','sum(hfile*pre)':'tot_hfpre',
                                             'sum(hfile*post)':'tot_hfpost'})

#first merge splticrm spsdrm spsticrm doc_Date_1, doc_type from temp to temp_nf
temp.dtypes
temp_nf.dtypes
temp['datadate'] = pd.to_datetime(temp['datadate'])
temp_nf['datadate'] = pd.to_datetime(temp_nf['datadate'])
sample_merged = pd.merge(temp_nf,
                         temp[['gvkey','datadate','splticrm','spsdrm','spsticrm','doc_Date_1','doc_type']],
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')

sample_merged_copy = pd.merge(sample_merged,
                         sql_test_small_fm ,
                         left_on=['gvkey'],
                         right_on = ['gvkey'], how='left')

sample_merged_copy.to_csv(nf_path)

indexnames= CR_N[(CR_N['sum(pre)'].isna())].index
CR_N.drop(indexnames, inplace=True)
CR_NN = CR_N[['gvkey','datadate','splticrm','unrated_1','junk_1','inv_grade_1']]

#start calculating values compare with LR(2010)

#List of variable names to keep from compustat
#CAPX : Capital expenditures
#AQC Aquisitions
#SPPE sale of property plan and equipment
#SPPIV sale of property plan and equipment and investments
#IVCH - inreace in investments
#SIV sale of investments
#AT total assets
#DLTIS :Long-term debt issuance
#DLTR Long-term debt reduction
#DLCCH - Current debt 0 changes
# SSTK:  Sale of Common and Preferred Stock
# PRSTKC: Purchase of Common and Preferred Stock
# dm: Debt Mortgages & Other Secured
# APCH: Account payable increase-decrease
# RECCH: Accounts receivable - Decrease-increase
# CHE - Cash and equivalents
# CHECH - - Cash and equivalents - Increase/Decrease
# DLTT Long term debt total
# DLC debt in current liabilities
# PRCC_F : price close fiscal #not in funda
#CSHO common shares outstanding
# pstkl :prferred stock liquidating value
# txditc :deferrred taxes and investment tax credt
# sale : net sales
# MKVALT: market value total
# ajex: adjustment factor
# xint : interest and related expense total
# ebitda: earnings before interest etc...
#
#definitions
#Net investment = (capx+aqc-sppe+ivch-siv)/(at)
#Net LT Debt Issues = (DLTIS-DLTR)/AT
#Net ST Debt Issues = DLCCH/AT
#Net equity issues = (SSTK - PRSTKC)/AT
#Net Issues of Secured Debt and mortgages = dm-dm_lag
#change in trade credit = (apch-recch)/at
#change in cash = chech/at
# total debt (TD) = DLTT + DLC
#Market Value of Assets (MVA) = prcc_f*csho+dlc+dltt-pstkl-txditc
#Market leverage = TD / MVA
#firm size = log(sale)
#market-to-book_1 = mkvalt+TD+pstkl-txditc)/at
#market-to-book_2 = prcc_f*csho+TD+pstkl-txditc)/at
#net equity issues = (csho_t-csho_t-1)*(ajex_t/ajex_t-1)*(PRCC_F_t+PRCC_F_t-1)*(ajex_t/ajex_t-1)/at
#financially distressed = 1 if EBITDA < (xint_t+xint_t_1) or 1 if EBITDA < 0.8*xint_t or 0.8*xint_t-1
# term spread = where get?
# credit spread = where?
# equity market return = CRSP annual value weithed return
#firm age =
#SP500 =
#NYSE =
#Firm age =
#operating income =
#asset turnover =
#return on equity =
#Z-score =

#sample date need: nonmissing data for book assets(at), net debt issuances, net equity issuances, investment and market to book

def sas_to_dataframe(file_path):
    """
    Read in a SAS dataset (sas7bdat) with the proper encoding
    :param file_path: location of SAS dataset to be read in
    :return: pandas dataframe with contents of SAS dataset
    """
    return pd.read_sas(file_path, format="sas7bdat", encoding="iso-8859-1")
CR = sas_to_dataframe(CT)
CR.to_csv(CT1)
