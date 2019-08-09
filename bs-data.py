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
sample_path = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/not_collapsed_v1.txt'
COMP_DATA = pd.read_csv(COMP, sep=",")
COMP_CAPX = pd.read_csv(COMP_CAPX, sep=",")
sample = pd.read_csv(sample_path, sep=",")
sample = sample.drop(columns=['Unnamed: 0'])

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
#fix problem with some line duplicated due to slight difference in doc_Date_1
#sample_mf = sample.groupby(['gvkey','datadate'], as_index=False)['doc_type'].agg(lambda col: list(col))
#sample_mf = sample.groupby(['gvkey','datadate'], as_index=False)['doc_type'].agg(lambda col: col)
#sample_mf = sample.groupby(['gvkey','datadate'])['doc_type'].agg(lambda col: + col).reset_index()
#sample_mf = sample.groupby(['gvkey','datadate'], as_index=False)['doc_type'].agg(lambda col: + col)
sample_mf = sample.groupby(['gvkey','datadate'], as_index=False)['doc_type'].agg(lambda col: + col)

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
#sample_merged_small = sample_merged [['gvkey','datadate','junk_1','junk_1_hfile','at','prcc_f', 'csho',
                                      #'dltis', 'capx', 'doc_Date_1', 'tot_hfpre']]
#sample_merged_small = sample_merged [['gvkey','datadate','inv_grade_1','inv_grade_1_hfile','at','prcc_f', 'csho',
                                      #'dltis', 'capx','doc_Date_1']]
#sample_merged_small = sample_merged [['gvkey','datadate','unrated_1','unrated_1_hfile','at','prcc_f', 'csho',
                                      #'dltis', 'capx', 'doc_Date_1']]

#sample_merged_small = sample_merged_small.loc[sample_merged_small['junk_1'] == 1]
#sample_merged_small = sample_merged_small.loc[sample_merged_small['inv_grade_1'] == 1]
#sample_merged_small = sample_merged_small.loc[sample_merged_small['unrated_1'] == 1]
#sample_merged_small['junk_1'].sum(axis=0, skipna = True)
#sample_merged_small['junk_1_hfile'].sum(axis=0, skipna = True)


#Determine firm observations with missing at or prcc_f (this probably means not traded in an exchange so it is good)
#have another for firms with total debt >0
sample_merged['missing'] = np.where(np.any([
                                sample_merged['at'].isna(),
                                sample_merged['prcc_f'].isna()], axis=0), 1, 0)

sample_merged['missing_nfile'] = np.where(np.all([
                                sample_merged['missing'] == 1,
                                sample_merged['tot_hfpre'] == 0], axis=0), 1, 0)

sample_merged['missing_hfile'] = np.where(np.all([
                                sample_merged['missing'] == 1,
                                sample_merged['tot_hfpre'] > 0], axis=0), 1, 0)

sample_merged_small = sample_merged [['gvkey','datadate','junk_1','junk_1_hfile','at','prcc_f', 'csho',
                                    'splticrm', 'doc_Date_1', 'tot_hfpre','missing_nfile',
                                    'missing_hfile']]


# intead of erasing, make a new group with missing there are more than one possible groupings

sample_merged_temp = sample_merged.drop(sample_merged[sample_merged.missing_nfile == 1].index)

#sample_merged_small = sample_merged_small.loc[sample_merged_small['junk_1'] == 1]
#make variable 1 if document for that year
sample_merged_temp['no_doc'] = np.where(np.any([
                                sample_merged_temp['doc_Date_1'].isna()], axis=0), 0, 1)

sample_merged_temp['junk_1_hfile_doc'] = sample_merged_temp['junk_1_hfile']*sample_merged_temp['no_doc']

sample_merged_temp['junk_1'].sum(axis=0, skipna=True)
sample_merged_temp['junk_1_hfile'].sum(axis=0, skipna=True)
sample_merged_temp['junk_1_hfile_doc'].sum(axis=0, skipna=True)

sample_merged_small = sample_merged_temp [['gvkey','datadate','junk', 'junk_1','junk_1_hfile','at','prcc_f', 'csho',
                                    'splticrm', 'doc_Date_1', 'tot_hfpre','missing_nfile',
                                    'missing_hfile']]
sample_merged_small = sample_merged_small.loc[sample_merged_small['junk_1'] == 1]

#first, deal with missing data wqith hfile
## make new junk_2 and ing_grade_2 a majority of the pre period and post period in junk
#do sum(junk)/sum(pre) > 0.5 sum(junk)/sum(post) > 0.5 #do sum(invg_grade)/sum(pre)

#sample_merged.drop_duplicates(keep=False, inplace=True)
#sample_merged = sample_merged.drop(sample_merged[sample_merged['indfmt'] == 'FS'].index)
#reclassify groups taking into account have both pre and post (or just erase?)

sqlcode = '''
select gvkey,
sum(pre), sum(post), junk_1, junk_1_hfile
from sample_merged
group by gvkey
'''
sql_test = ps.sqldf(sqlcode, locals())
COMP_DATA[(COMP_DATA.gvkey == 20423)]
#CSS = CS[(CS.gvkey == 1004)]
#gvkey 2176 somehow two entries,one capx=nan other 0 where?　check what was happening with indfmt INDustrial and FS


#erase at, prcc_f and csho nans and reclassify, especially the unrated ones

#sample date need: nonmissing data for book assets(at), net debt issuances, net equity issuances, investment and market to book