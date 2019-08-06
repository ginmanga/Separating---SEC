import pandasql as ps


def sql_grpby_basic():
sqlcode = '''
select *
from COMP_MCR_FILE
left join SEC_FILES_SHORT on COMP_MCR_FILE.gvkey = SEC_FILES_SHORT.gvkey 
where ((COMP_MCR_FILE.tempdays - SEC_FILES_SHORT.tempdays) <= 15 
    and (COMP_MCR_FILE.tempdays - SEC_FILES_SHORT.tempdays) >= -15) 
'''
newdf = ps.sqldf(sqlcode, locals())