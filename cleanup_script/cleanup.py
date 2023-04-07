import sys
import os
import impala.error
from impala.dbapi import connect
from configparser import ConfigParser
import subprocess
import shlex
import logging
from datetime import datetime
import time


#just add tables in this file
table_list="/home/rkumbar/cleanup_script/table_list"

def get_impala_connection():
    conn = connect(host='', port=21050, auth_mechanism='GSSAPI', kerberos_service_name='impala')
    cursor = conn.cursor()
    return cursor



def get_table_ddl_location(table_name):
    cursor = get_impala_connection()
    cursor.execute("show create table " + table_name + "")
    result = cursor.fetchall()
    cursor.close()
    statement = " ".join([x[0] for x in result])
    statement = statement.split('TBLPROPERTIES', 1)[0]
    TableLocation = statement.split('LOCATION', 1)[1].strip().split("'")[1]
    CheckDirectory = TableLocation.rsplit("/",1)[0] 
    return CheckDirectory,TableLocation


def execute_impala_query(query):
    try:
        cursor = get_impala_connection()
        cursor.execute(query)
        result= cursor.fetchall()
        cursor.close()
        return result
        
        
    except impala.error.HiveServer2Error as e:
        print(str(e))
        cursor.close()
    
def delete_others_except_one(CheckDir, TableLoc):
    value= subprocess.call(['bash','delete.sh',CheckDir,TableLoc])
    return value
    
if __name__ == "__main__":
    with open(table_list) as tablelist:
        for line in tablelist:
            tableName = line.strip()
            dir ,loc = get_table_ddl_location(tableName)
            print("============================ main ========================================")
            print("\n")
            print("TABLE NAME :{}".format(tableName))
            print("\n")
            before_count_query="SELECT COUNT(*) FROM "+tableName+";"
            beforecount=execute_impala_query(before_count_query)
            beforecount=[ scount[0] for scount in beforecount]
           
            result = delete_others_except_one(dir,loc)
            if result == 0:
                aftercount=execute_impala_query(before_count_query)
            aftercount=[ scount[0] for scount in aftercount]
         
            print("Before Table Count:{}".format(beforecount))
            print("After Table Count:{}".format(aftercount))
            print("=================================================================================")