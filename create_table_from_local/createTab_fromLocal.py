#script to create tables using ddls from local directory in hive

import os
from impala.dbapi import connect
import impala.error
from configparser import ConfigParser


path = "/home/rkumbar/mrs/mrs_new/mrs_facility_0007_del"
impala_db_name = "us0007sqldbfacilitydata07_001_backfill_raw_dev"
impala_tab_base_loc = "hdfs://nameservice1/data/dev/raw/us0007sqldbfacilitydata07_001_backfill_raw_dev/deleted/"
impala_table_file_format="PARQUET" 



def get_hive_connection():
    conn = connect(host='172.23.66.158', port=10000, auth_mechanism='GSSAPI', kerberos_service_name='hive')
    cursor = conn.cursor()
    return cursor
    
    
def create_hive_table(tblddl):
    try:
        cursor = get_hive_connection()
        cursor.execute(tblddl)
        cursor.close()
    except impala.error.HiveServer2Error as e:
        print(str(e))
        cursor.close()
        
        
def modify_ddl(ddl,table_name):
    table_location = '\''+impala_tab_base_loc+table_name+'/'+'\''
    ddl1= ddl.replace("$'DATABSE'", impala_db_name)
    ddl2=ddl1.replace("$'FILEFORMAT'",impala_table_file_format)
    ddl3=ddl2.replace("$'HDFS_LOCATION'",table_location)
    return ddl3
    
if __name__ == "__main__":
    for filename in os.listdir(path):
          if filename.endswith( ".sql" ):
            with open( os.path.join( path, filename ) ,"r") as fd:
                ddl = fd.read()
                #since ddl filename is same as table name
                table_name=filename.split(".")[0]
                print(table_name)
                modified_ddl = modify_ddl(ddl,table_name)
                create_hive_table(modified_ddl)