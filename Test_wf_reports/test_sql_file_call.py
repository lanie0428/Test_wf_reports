'''
Created on Aug 16, 2018

@author: Lanie.Shannon
'''
f = open('sno_cust_report.sql', 'r')
print (f)

'''import os'''
import pyodbc

conn = pyodbc.connect(r'DSN=Workday_EIB;UID=sqllocal;PWD=IhPCIcbts!')
crsr = conn.cursor()

crsr.execute(open('sno_cust_report.sql','r'))
#===============================================================================
# for line in open('sno_cust_report.sql','r'):
#     crsr.execute(""/line/"")
#===============================================================================

import combine2xlsx

combine2xlsx.combine(['put_customer_headers.xlsx', 'sno_cust_test_hold.xlsx'], 'sno_cust_test_sql_call.xlsx')