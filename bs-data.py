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
# APALCH: Account payable increase-decrease
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
# ADJEX_F: adjustment factor
# xint : interest and related expense total
# ebitda: earnings before interest etc...
# oibdp Operating Income Before Depreciation
# CEQ ­­ Common/Ordinary Equity ­ Tota
# PI - pretax income
# RE - retained earnings
# act - current assets
# lct - currewt liabilities
# PPENT PPEGT property plant and equipment
# XRD  r&d
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
#Firm age = number of years in compustat with non missing at
#operating income = oibdp/at
#asset turnover = sale/at
#return on equity = oibdp/ceq
#Z-score = 3.3*pi + sale + 1.4*re+ 1.2*(act-lct)/at
# term spread = where get? yield spread between the 1 and 10 year treasury bond
# credit spread = where? spread BB and BBB rated corp bonds
# equity market return = CRSP annual value weithed return
#SP500 = 1 firm in SP500
#NYSE = 1 firm in NYSE
#sample date need: nonmissing data for book assets(at), net debt issuances, net equity issuances, investment and market to book
#Net equity issues = (SSTK - PRSTKC)/AT
#Net LT Debt Issues = (DLTIS-DLTR)/AT
#Net ST Debt Issues = DLCCH/AT
#Net investment = (capx+aqc-sppe+ivch-siv)/(at)
#market-to-book_2 = prcc_f*csho+TD+pstkl-txditc)/at

#must have:AT SSTK, prcc_f, csho  PRSTKC, DLTIS DLTR DLCCH
# pstkl :prferred stock liquidating value. If nan turn to zero
# txditc :deferrred taxes and investment tax credt

#Debt specialization df sample drops if 1: missing zero values for AT 2:missing or zero total debt
#firms yeears with market or book leverage outside unit interval
# must be traded in Amex, Nasdaq and NYSE, civered by compustat, removed utilities and financials.
#must have leverage

import datetime, csv
import pandas as pd
import pandasql as ps
import numpy as np

#first import new compustat file
#second match not_collpased_v1 (file with classifications of credit rating) to the necessary compustat variables
#third check missing data according to LR (2010)

COMP = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/Source/compstat_LR_79_96.csv'
COMP_CAPX = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/Source/capex.csv'
sample_path = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/not_collapsed_v3.txt'
COMP_DATA = pd.read_csv(COMP, sep=",")
COMP_CAPX = pd.read_csv(COMP_CAPX, sep=",")
sample = pd.read_csv(sample_path, sep=",")
sample = sample.drop(columns=['Unnamed: 0', 'tot_pre', 'tot_psot', 'tot_hfpre', 'tot_hfpost'])
sample = sample.rename(columns = {'hfile_x':'hfile_o'}) #have file for that year
sample = sample.rename(columns = {'hfile_y':'hfile_cba'}) # have at least one file in pre and post

COMP_DATA.dtypes
sample.dtypes
COMP_CAPX.dtypes
COMP_DATA = COMP_DATA.astype({'datadate':'str'})
COMP_CAPX = COMP_CAPX.astype({'datadate':'str'})
COMP_DATA['datadate'] = pd.to_datetime(COMP_DATA['datadate'])
COMP_CAPX['datadate'] = pd.to_datetime(COMP_CAPX['datadate'])
sample['datadate'] = pd.to_datetime(sample['datadate'])
sample['doc_Date_1'] = pd.to_datetime(sample['doc_Date_1'])

sample = sample.sort_values(by=['gvkey','doc_Date_1','datadate'])
sample = sample.drop_duplicates()
########fix problem with some line duplicated due to slight difference in doc_Date_1#########
sample_mf = sample.groupby(['gvkey','datadate'], as_index=False)['doc_type'].agg(lambda col: + col)

#merge fix back to sample
sample = pd.merge(sample,
                  sample_mf,
                  left_on=['gvkey','datadate'],
                  right_on = ['gvkey','datadate'], how='left')

sample = sample.drop_duplicates(subset=['gvkey','datadate'])
sample = sample.drop(columns=['doc_type_x'])

COMP_DATA = pd.merge(COMP_DATA,
                         COMP_CAPX[['gvkey','datadate','capx', 'indfmt']],
                         left_on=['gvkey','datadate', 'indfmt'],
                         right_on = ['gvkey','datadate', 'indfmt'], how='left')

COMP_DATA = COMP_DATA.loc[:,~COMP_DATA.columns.duplicated()]
COMP_DATA = COMP_DATA.drop_duplicates()

COMP_DATA = COMP_DATA.drop(COMP_DATA[COMP_DATA['indfmt'] == 'FS'].index)

sample_merged = pd.merge(sample,
                         COMP_DATA,
                         left_on=['gvkey','datadate'],
                         right_on = ['gvkey','datadate'], how='left')



#Determine firm observations with missing at or prcc_f (this probably means not traded in an exchange so it is good)
#call this sample_LR (lemmon roberts)
#have another for firms with total debt >0
#1st erase firms whgere both pre and post not >0

#aggregate pre and post keep only those with at least one obs
sqlcode = '''
select gvkey,
sum(pre), sum(post)
from sample_merged
group by gvkey
'''
sql_test = ps.sqldf(sqlcode, locals())
sample_merged_LR = pd.merge(sample_merged, sql_test, left_on=['gvkey'], right_on = ['gvkey'], how='left')
#merge sample with sql and erase

sample_merged_LR = sample_merged_LR.drop(sample_merged_LR[sample_merged_LR['sum(pre)'] == 0].index)
sample_merged_LR = sample_merged_LR.drop(sample_merged_LR[sample_merged_LR['sum(post)'] == 0].index)

sample_merged_LR = sample_merged_LR.sort_values(by=['gvkey','datadate'])

sample_merged_LR['missing'] = np.where(np.any([
                                sample_merged_LR['at'].isna(),
                                sample_merged_LR['prcc_f'].isna()], axis=0), 1, 0)

sample_merged_LR['missing_nfile'] = np.where(np.all([
                                sample_merged_LR['missing'] == 1,
                                sample_merged_LR['tot_hfpre'] == 0], axis=0), 1, 0)

sample_merged_LR['missing_hfile'] = np.where(np.all([
                                sample_merged_LR['missing'] == 1,
                                sample_merged_LR['tot_hfpre'] > 0], axis=0), 1, 0)

#small
sample_merged_LR_small = sample_merged_LR [['gvkey','datadate','junk_1','junk_1_hfile','hfile_o','hfile_cba', 'at','prcc_f', 'csho',
                                    'splticrm', 'doc_Date_1', 'missing', 'tot_hfpre','missing_nfile',
                                    'missing_hfile']]

sample_merged_LR_small = sample_merged_LR_small.loc[sample_merged_LR_small['junk_1'] == 1]

#special cases 1311 have file including AICPA

# intead of erasing, make a new group with missing there are more than one possible groupings

# Grouping 1:
#junk_1_LR if at least one year junk in pre and post, at least one pre post obs with data
#invgrade_1_LR if at least one year junk in pre and post, at least one pre post obs with data
#junk_2_LR if at least half the obs pre and post rated junk subset of junk_1_LR
#invgrade_2_LR at least half the obs pre and post rated junk of invgrade_1_LR


# First keep only firms which have non missing obs in pre and post
#sample_merged_LR = sample_merged_LR.rename(columns = {'sum(pre)':'tot_pre'})

sqlcode = '''
select gvkey,
sum(pre*(1-missing)) as a, sum(post*(1-missing)) as b,
sum(junk*pre) as junkpre, sum(junk*post) as junkpost,
sum(inv_grade*pre) as igpre, sum(inv_grade*post) as igpost,
sum(pre) as obs_pre, sum(post) as obs_post
from sample_merged_LR
group by gvkey
'''
sql_test = ps.sqldf(sqlcode, locals())



# sum percentage of observations in which the firm is rated junk or invg_grade

sql_test['p_junkpre'] =  sql_test['junkpre']/sql_test['obs_pre']
sql_test['p_junkpost'] =  sql_test['junkpost']/sql_test['obs_post']
sql_test['p_igpre'] =  sql_test['igpre']/sql_test['obs_pre']
sql_test['p_igpost'] =  sql_test['igpost']/sql_test['obs_post']

sql_test['LR_notmissing'] = np.where(np.all([sql_test['a'] > 0,
                                     sql_test['b'] > 0],
                                     axis=0), 1, 0)
#majority of pre observations the firm is rated junk
sql_test['maj_junk_both'] = np.where(np.all([sql_test['p_junkpre'] >= 0.5,
                                     sql_test['p_junkpost'] >= 0.5],
                                     axis=0), 1, 0)

#majority of post observations the firm is rated inv_grade
sql_test['maj_ig_both'] = np.where(np.all([sql_test['p_igpre'] >= 0.5,
                                     sql_test['p_igpost'] >= 0.5],
                                     axis=0), 1, 0)

sql_test['maj_junk_pre'] = np.where(np.all([sql_test['p_junkpre'] >= 0.5],
                                     axis=0), 1, 0)

sql_test['maj_ig_pre'] = np.where(np.all([sql_test['p_igpre'] >= 0.5],
                                     axis=0), 1, 0)
# merge back and create the new groups


sample_merged_LR_1 = pd.merge(sample_merged_LR,
                         sql_test[['gvkey','LR_notmissing','maj_junk_both','maj_ig_both',
                                   'maj_junk_pre','maj_ig_pre']],
                         left_on=['gvkey'],
                         right_on = ['gvkey'], how='left')


#erase all where LR_notmissing =0
sample_merged_LR_1 = sample_merged_LR_1.drop(sample_merged_LR_1[sample_merged_LR_1['LR_notmissing'] == 0].index)

#Build junk groups
# First: junk_1 as before
# Second: Junk_2 most obs pre and post are junk
# third: junk_1 =1, Maj_pre_junk_=1, maj_junk_post=0

sample_merged_LR_1['junk_1_LR'] = sample_merged_LR_1['junk_1']

#sample_merged_LR_1['junk_2_LR'] = np.where(np.all([sample_merged_LR_1['maj_junk_pre'] == 1,
                                     #sample_merged_LR_1['maj_junk_post'] == 1],
                                     #axis=0), 1, 0)

sample_merged_LR_1['junk_2_LR'] = np.where(np.all([sample_merged_LR_1['maj_junk_both'] == 1],
                                     axis=0), 1, 0)

sample_merged_LR_1['junk_3_LR'] = np.where(np.all([sample_merged_LR_1['maj_junk_pre'] == 1,
                                     sample_merged_LR_1['junk_1'] == 1],
                                     axis=0), 1, 0)


sample_merged_LR_1['ig_1_LR'] = sample_merged_LR_1['inv_grade_1']
sample_merged_LR_1['rated_LR'] = sample_merged_LR_1['rated_nc'] # rated but not classfied as junk_1 or inv_grade_1
sample_merged_LR_1['unrated_LR'] = sample_merged_LR_1['unrated_1'] # neverrate

#downsize keep only groupings and at, prcc_f



sample_merged_LR_small = sample_merged_LR_1 [['gvkey','datadate','junk','junk_1','junk_1_hfile','maj_junk_pre','maj_junk_both','at','prcc_f', 'csho',
                                    'splticrm', 'doc_Date_1']]

sampling_LR = sample_merged_LR_1[['gvkey','datadate','fyear','hfile_o','hfile_cba','at','prcc_f',
                                  'junk_1_LR','junk_2_LR','junk_3_LR', 'ig_1_LR','rated_LR','unrated_LR',
                                  'hjunk_1','lowinv_grade_1','CP_ALL','CP_ALL_BOTH','CP_L_BOTH', 'CP_H_BOTH',
                                  'doc_type_y','doc_Date_1','splticrm','spsdrm', 'spsticrm']]

#collpase to get counts of different groups
sampling_LR = sampling_LR.sort_values(by=['gvkey','datadate'])

sqlcode = '''
select gvkey, junk_1_LR, junk_2_LR, junk_3_LR, ig_1_LR, rated_LR,
(junk_1_LR*hfile_cba) as j_1_hf, 
(junk_2_LR*hfile_cba) as j_2_hf, 
(junk_3_LR*hfile_cba) as j_3_hf, 
(ig_1_LR*hfile_cba) as ig_1_hf, 
(unrated_LR*hfile_cba) as ur_1_hf
from sampling_LR
group by gvkey
'''
sql_test_LR = ps.sqldf(sqlcode, locals())

sql_test_LR.sum(axis=0, skipna = True)

sample_collapsed = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/sampling/sampling_collapsed_v1.txt'
sample_notcollapsed = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/sampling/sampling_v1.txt'

sql_test_LR.to_csv(sample_collapsed)
sampling_LR.to_csv(sample_notcollapsed)