import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np

count = 0
COMP_CR = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/adsprate.sas7bdat'
COMP_CRS = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/cpstCR.txt'
COMP_COMP = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/COMPSTAT_all_vars_1970_20150301.txt'
COMP_MCR = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/MCR.txt'
COMP_MCR_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/MCR_FINAL.txt'
#COMP_CR = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/names_adsprate.sas7bdat'
#with SAS7BDAT(COMP_CR, skip_header=False) as reader:
   # print(reader[0])

    #for row in reader:
        #print(row)
        #count += 1
        #if count == 1000:
            #break
        #break


def sas_to_dataframe(file_path):
    """
    Read in a SAS dataset (sas7bdat) with the proper encoding
    :param file_path: location of SAS dataset to be read in
    :return: pandas dataframe with contents of SAS dataset
    """
    return pd.read_sas(COMP_CR, format="sas7bdat", encoding="iso-8859-1")


#CR = sas_to_dataframe(COMP_CR)
#CR.to_csv(COMP_CRS)

#CS = pd.read_csv(COMP_COMP, sep="\t", nrows=10000, usecols=[0, 1,2 ,3 ,925 ,1823] )#, header=None)
#CR = pd.read_csv(COMP_CRS, sep=",", nrows=10000)
CSE = pd.read_csv(COMP_COMP, sep="\t", usecols=[0, 1,2 ,3 ,925 ,1823] )#, header=None)
CS = pd.read_csv(COMP_COMP, sep="\t", usecols=[0, 1] )#, header=None)
CR = pd.read_csv(COMP_CRS, sep=",")
#CSS = CS[(CS.gvkey == 1004)]
print(CS.describe)
print(CR.describe)
CS.dtypes
CR.dtypes

dates_CR = CR['datadate'].tolist() # turn date into python list
for x, item in enumerate(dates_CR):
    dates_CR[x] = "".join(item.split("-"))

CR['mdatadate'] = dates_CR #creates new column with values of dates_CR
CR.astype({'mdatadate': 'int64'}).dtypes
CR = CR.rename(columns = {'datadate':'oldate'})
CR.astype({'mdatadate': 'int64'}).dtypes
CR = CR.astype({'mdatadate': 'int64'})
CR.dtypes
#CSSmerged = pd.merge(CS, CR, left_on=['gvkey','datadate'], right_on = ['gvkey','mdatadate'], how='left')

#df.astype({'col1': 'int32'}).dtypes
#CR.astype({'mdatadate': 'int64'}).dtypes #change variable type
#CR.astype({'mdatadate': 'int64'})
#CR.rename(columns = {'datadate':'oldate'}) #ranme a variable

A = CS
B = CR
sqlcode = '''
select *
from A
left join B on A.gvkey = B.gvkey
where A.datadate >=  B.mdatadate and (A.datadate -  B.mdatadate) <= 366
'''
newdf = ps.sqldf(sqlcode, locals())
newdf['gvkeydate'] = 's'
newdf['gvkeydate'] = newdf['gvkey'].astype(str) #+ newdf['mdatadate'].astype(str)
newdf['gvkeydate'] = newdf['gvkeydate'].astype(str) + newdf['mdatadate'].astype(str)
newdf = newdf.astype({'gvkeydate':'int64'})
newdf = newdf.sort_values(by=['datadate','gvkeydate'], ascending=[1,0])

sqlcode = '''
select *
from newdf
group by newdf.gvkey, newdf.datadate 
'''
newdf = ps.sqldf(sqlcode, locals())
newdf.to_csv(COMP_MCR)
newdf.describe

#,erge CS and newdf by gvkey datadate
sqlcode = '''
select *
from CSE
left join newdf on CSE.gvkey = newdf.gvkey and CSE.datadate = newdf.datadate
'''
newdf1 = ps.sqldf(sqlcode, locals())
newdf1.to_csv(COMP_MCR_FINAL)
newdf1.describe





B.dtypes
sqlcode = '''
select A.gvkey
from A
left join B on A.gvkey = B.gvkey
where A.datadate <=  B.mdatadate
group by A.gvkey and A.datadate
'''

sqlcode = '''
select *
from A
left join B on A.gvkey = B.gvkey and A.datadate >= B.mdatadate
'''
sqlcode = '''
select *
from A
left join B on A.gvkey = B.gvkey
where A.datadate >=  B.mdatadate and A.datadate -  B.mdatadate >= 366
group by A.gvkey, A.datadate 
'''


newdf.sort_values(by=['gvkeydate'])
newdf = newdf.astype({'gvkey':'str','mdatadate':'str'})
newdf = newdf.astype({'mdatadate': 'str'})
newdf.dtypes

newdf[['gvkey','datadate','splticrm','mdatadate']]
sqlcode2 = '''
select *
from A
sort
'''
newdf2 = ps.sqldf(sqlcode, locals())
where  A.datadate - B.mdatadate < 30 or A.mdatadate = 'nan'
sqlcode_2 = '''
select A.gvkey
from A
left join B on A.gvkey = B.gvkey
where A.datadate <=  B.mdatadate
order by A.gvkey and A.datadate
'''
# and A.datadate <= B.nameenddt

