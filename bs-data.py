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

import datetime, csv
import pandas as pd
import pandasql as ps
import numpy as np

#first import new compustat file
#second match not_collpased_v1 (file with classifications of credit rating) to the necessary compustat variables
#third check missing data according to LR (2010)

COMP = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/Source/compstat_LR_79_96.csv'
sample_path = 'C:/Users/Panqiao/Documents/Research/SS - All/MFFS/not_collapsed_v1'
COMP_DATA = pd.read_csv(COMP, sep=",")
sample