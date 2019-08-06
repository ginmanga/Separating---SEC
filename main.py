import datetime, csv
#from sas7bdat import SAS7BDAT
import pandas as pd
import pandasql as ps
import numpy as np
#**************************
#******Description*********
#******Take Compustat Credit ratings and merge with compustat CRSP...
#*************************
#Check why MCR_FINAL is so much larger than MCR_FINAL_1 and MCR_FINAL_A
count = 0
#COMP_CR = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/adsprate.sas7bdat' #sas Credit rating set
COMP_CRS = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/cpstCR.txt' #text credit rating
COMP_COMP = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/COMPSTAT_all_vars_1970_20150301.txt' # all compustat
COMP_MCR = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/MCR.txt'
COMP_MCR_FINAL = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/MCR_FINAL.txt'
COMP_MCR_FINAL_V1 = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/MCR_FINAL_V1.txt'
CRSPLINK  = 'C:/Users/Panqiao/Documents/Research/SS - All/CRSPCOMPSTAT/CRSPCOMPSTATLINK.csv'
#COMP_CR = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/names_adsprate.sas7bdat

CRPLINK = pd.read_csv(CRSPLINK, sep=",") #collapsed crps compstat link
#with SAS7BDAT(COMP_CR, skip_header=False) as reader:
   # print(reader[0])

    #for row in reader:
        #print(row)
        #count += 1
        #if count == 1000:
            #break
        #break



#CR = sas_to_dataframe(COMP_CR)
#CR.to_csv(COMP_CRS)

CSC = pd.read_csv(COMP_COMP, sep="\t", nrows=10)#, usecols=[0, 1,2 ,3 ,925 ,1823] )#, header=None)
#CR = pd.read_csv(COMP_CRS, sep=",", nrows=10000)
#Merge compustat and CRating
CSE = pd.read_csv(COMP_COMP, sep="\t", usecols=[0, 1, 2, 3, 4, 925, 1823] )#, header=None)
CS = pd.read_csv(COMP_COMP, sep="\t", usecols=[0, 1] )#, header=None)
CR = pd.read_csv(COMP_CRS, sep=",")
#CSS = CS[(CS.gvkey == 1004)]
print(CS.describe)
print(CR.describe)
CS.dtypes
CR.dtypes


#dates_CR = CR['datadate'].tolist() # turn date into python list
#for x, item in enumerate(dates_CR):
    #dates_CR[x] = "".join(item.split("-"))

#CR['mdatadate'] = dates_CR #creates new column with values of dates_CR
#CR.astype({'mdatadate': 'int64'}).dtypes
#CR = CR.rename(columns = {'datadate':'oldate'})
#CR.astype({'mdatadate': 'int64'}).dtypes
#CR = CR.astype({'mdatadate': 'int64'})
#CR.dtypes


#COMP_MCR_FILE = COMP_MCR_FILE.astype({'datadate':'int64'})
#COMP_MCR_FILE = COMP_MCR_FILE.astype({'datadate':'str'})
#SEC_FILES_1['doc_Date_1'] = pd.to_datetime(SEC_FILES_1['doc_Date'])
CR['datadate'] = pd.to_datetime(CR['datadate'])
CS = CS.astype({'datadate':'str'})
CS['datadate'] = pd.to_datetime(CS['datadate'])
CS.dtypes
CR.dtypes
#CSSmerged = pd.merge(CS, CR, left_on=['gvkey','datadate'], right_on = ['gvkey','mdatadate'], how='left')

#df.astype({'col1': 'int32'}).dtypes
#CR.astype({'mdatadate': 'int64'}).dtypes #change variable type
#CR.astype({'mdatadate': 'int64'})
#CR.rename(columns = {'datadate':'oldate'}) #ranme a variable

#convert dates to days...

CS['temp'] = '19600101'
CR['temp'] = '19600101'

CS['temp'] = pd.to_datetime(CS['temp'])
CR['temp'] = pd.to_datetime(CR['temp'])

CS['tempdays'] = (CS['datadate']-CS['temp']).dt.days
CR['tempdays'] = (CR['datadate']-CR['temp']).dt.days


A = CS
B = CR
sqlcode = '''
select *
from A
left join B on A.gvkey = B.gvkey
where (A.tempdays == B.tempdays or A.tempdays -  B.tempdays <= 366)
'''
sqlcode = '''
select *
from A
left join B on A.gvkey = B.gvkey
where (A.tempdays == B.tempdays)
'''
newdf = ps.sqldf(sqlcode, locals())
###CHECK IF ERASE
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
###CHECK IF ERASE

newdf = newdf.loc[:,~newdf.columns.duplicated()]
newdf = newdf.drop(columns=['temp','tempdays'])
#,erge CS and newdf by gvkey datadate
CSE.dtypes
CSE = CSE.astype({'datadate':'str'})
CSE['datadate'] = pd.to_datetime(CSE['datadate'])
sqlcode = '''
select *
from CSE
left join newdf on (CSE.gvkey = newdf.gvkey and CSE.datadate = newdf.datadate)
'''
newdf1 = ps.sqldf(sqlcode, locals())
newdf1 = newdf1.loc[:,~newdf1.columns.duplicated()]
newdf1 = newdf1.drop(columns=['Unnamed: 0'])
newdf1.to_csv(COMP_MCR_FINAL_V1)
newdf1.describe

#Finally add merger to CRSPLINK add permco permno to CR
#first convert LINKDT and LNEKDDT to dates
CRPLINK.dtypes
newdf1.dtypes
CRPLINK.loc[CRPLINK.LINKENDDT == 'E', 'LINKENDDT'] = 20151231 #convert E to date
CRPLINK = CRPLINK.astype({'LINKENDDT':'int64'})
#newdf2 = pd.merge(newdf1, CRPLINK, left_on=['gvkey','datadate'], right_on = ['gvkey','mdatadate'], how='left')

sqlcode = '''
select *
from newdf1
left join CRPLINK on newdf1.gvkey = CRPLINK.gvkey
where (newdf1.datadate >= CRPLINK.LINKDT and newdf1.datadate < CRPLINK.LINKENDDT)
'''
newdf2 = ps.sqldf(sqlcode, locals())
#newdf2 = newdf2[newdf2.columns[0,1,2,3,4,5,7,8,9,10,11,21,22]]
newdf2 = newdf2.drop(columns=['Unnamed: 0'])
newdf2 = newdf2.drop(columns=['conm'])
newdf2 = newdf2.drop(columns=['tic','cusip','CIK'])
newdf2 = newdf2.drop(columns=['LINKPRIM','LIID',])
# remove duplicated columns
#save again
COMP_MCR_FINAL_A = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/MCR_FINAL_A.txt'
newdf2.to_csv(COMP_MCR_FINAL_A)

newdf2 = newdf2.loc[:,~newdf2.columns.duplicated()] #works

newdf2 = newdf2.drop(columns=['Unnamed: 0','conm','tic','cusip','CIK','LINKPRIM',
                              'LIID','LINKTYPE'])


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

