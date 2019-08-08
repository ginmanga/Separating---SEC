COMP_COMP = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/COMPSTAT_all_vars_1970_20150301.txt' # all compustat
CSC = pd.read_csv(COMP_COMP, sep="\t", nrows=100)#, usecols=[0, 1,2 ,3 ,925 ,1823] )#, header=None)
t2 = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/test/names_adsprate.sas7bdat'
t1 = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/test/ca293dfa649fdbc6.csv'
COMP_CRS = 'C:/Users/Panqiao/Documents/Research/SS - All/COMPSTAT/cpstCR.txt'
a = CSC.columns.values.tolist()
CSC[['gvkey' == 1004,'fyear','dm']]
CR = pd.read_csv(COMP_CRS, sep=",")
CR_N = pd.read_csv(t1, sep=",")

def sas_to_dataframe(file_path):
    """
    Read in a SAS dataset (sas7bdat) with the proper encoding
    :param file_path: location of SAS dataset to be read in
    :return: pandas dataframe with contents of SAS dataset
    """
    return pd.read_sas(file_path, format="sas7bdat", encoding="iso-8859-1")

df = sas_to_dataframe(t1)
print(df.head())

#df = pd.read_sas(t1)

#first, collapse to year
CR_N.dtypes
CR_N = CR_N.astype({'datadate':'str'})

CR_N['year'] = pd.DatetimeIndex(CR_N['datadate']).year
CR_N['month'] = pd.DatetimeIndex(CR_N['datadate']).month

CR_N = CR_N.drop(CR_N[CR_N.year >= 1994].index)
CR_N = CR_N.drop(CR_N[CR_N.year < 1986].index)

CR_N = CR_N.drop(CR_N[CR_N.month != 12].index)

junk = ['BB+','BB','BB-','B+','B','B-','CCC+','CCC','CCC-','CC+','CC','CC-']#'C','D']


CR_N['junk'] = [1 if x in junk else 0 for x in CR_N['splticrm']]
CR_N['hjunk'] = [1 if x in hjunk else 0 for x in CR_N['splticrm']]
CR_N['inv_grade'] = [1 if x in ig else 0 for x in CR_N['splticrm']]
CR_N['lowinv_grade'] = [1 if x in lig else 0 for x in CR_N['splticrm']]
CR_N['pre'] = np.where(CR_N['year'] < 1990, 1, 0)
CR_N['post'] = np.where(CR_N['year'] >= 1990, 1, 0)

indexnames= CR_N[(CR_N['sic'] >= 6000) & (CR_N['sic'] < 7000)].index
CR_N.drop(indexnames, inplace=True)

sqlcode = '''
select gvkey,
sum(pre), sum(post), sum(junk*pre), sum(junk*post), sum(inv_grade*pre), sum(inv_grade*post)
from CR_N
group by gvkey
'''
sql_test2 = ps.sqldf(sqlcode, locals())

sql_test2 = sql_test2.drop(sql_test2[sql_test2['sum(pre)'] == 0].index)
sql_test2 = sql_test2.drop(sql_test2[sql_test2['sum(post)'] == 0].index)

sql_test2['unrated_1'] = np.where(np.all([sql_test2['sum(junk*pre)'] == 0, sql_test2['sum(junk*post)'] == 0,
                                         sql_test2['sum(inv_grade*post)'] == 0, sql_test2['sum(inv_grade*pre)'] == 0], axis=0), 1, 0)

#sql_test2['unrated_1_hfile'] = np.where(np.all([sql_test2['unrated_1'] == 1, sql_test2['sum(hfile*pre)'] > 0,
                                         #sql_test2['sum(hfile*post)'] > 0], axis=0), 1, 0)


sql_test2['junk_1']= np.where(np.all([sql_test2['sum(junk*pre)'] > 0, sql_test2['sum(junk*post)'] > 0,
                                         sql_test2['sum(inv_grade*post)'] == 0, sql_test2['sum(inv_grade*pre)'] == 0], axis=0), 1, 0)

#sql_test['junk_1_hfile']= np.where(np.all([sql_test['junk_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         #sql_test['sum(hfile*post)'] > 0], axis=0), 1, 0)

sql_test2['inv_grade_1']= np.where(np.all([sql_test2['sum(junk*pre)'] == 0, sql_test2['sum(junk*post)'] == 0,
                                         sql_test2['sum(inv_grade*post)'] > 0, sql_test2['sum(inv_grade*pre)'] > 0], axis=0), 1, 0)

#sql_test['inv_grade_1_hfile']= np.where(np.all([sql_test['inv_grade_1'] == 1, sql_test['sum(hfile*pre)'] > 0,
                                         #sql_test['sum(hfile*post)'] > 0], axis=0), 1, 0)

sql_test2['junk_1_total_obs'] = sql_test2['junk_1']*sql_test2['sum(pre)']+sql_test2['junk_1']*sql_test2['sum(post)']
sql_test2['ig_1_total_obs'] = sql_test2['inv_grade_1']*sql_test2['sum(pre)']+sql_test2['inv_grade_1']*sql_test2['sum(post)']
sql_test2['unrated_1_total_obs'] = sql_test2['unrated_1']*sql_test2['sum(pre)']+sql_test2['unrated_1']*sql_test2['sum(post)']
sql_test2.sum(axis=0, skipna = True)

CR_N = pd.merge(CR_N, sql_test2, left_on=['gvkey'], right_on = ['gvkey'], how='left')

indexnames= CR_N[(CR_N['sum(pre)'].isna())].index
CR_N.drop(indexnames, inplace=True)
CR_NN = CR_N[['gvkey','datadate','splticrm','unrated_1','junk_1','inv_grade_1']]