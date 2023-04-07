from impala.dbapi import connect
import subprocess
import time
import sys
from datetime import datetime
import logging
import os
import impala.error
from configparser import ConfigParser

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()
logger.addHandler(logging.FileHandler(
    '/home/etlsvcprod/cdh_administration/hive_tables_compaction/logs/rebuild_' + datetime.now().strftime("%Y%m%d-%H%M%S") + '.log', 'a'))
print = logger.info


def get_impala_connection():
    conn = connect(host='10.0.0.63', port=21050, auth_mechanism='GSSAPI', kerberos_service_name='impala')
    cursor = conn.cursor()
    return cursor


def get_hive_connection():
    conn = connect(host='10.0.0.246', port=10000, auth_mechanism='GSSAPI', kerberos_service_name='hive')
    cursor = conn.cursor()
    return cursor


def get_table_ddl(table_name):
    cursor = get_hive_connection()
    cursor.execute("show create table " + table_name + "")
    result = cursor.fetchall()
    cursor.close()
    statement = " ".join([x[0] for x in result])
    return statement


def create_hive_table(tblddl):
    cursor = get_hive_connection()
    cursor.execute(tblddl)
    cursor.close()


def get_table_row_count(tablename):
    cursor = get_impala_connection()
    cursor.execute("select count(*) from " + tablename + ";")
    result = cursor.fetchall()
    cursor.close()
    return result[0]


def load_to_stage_table(stage_table_name, prod_table_name):
    print('loading data to stage table...')
    cursor = get_hive_connection()
    cursor.execute("set mapred.job.name ="+ stage_table_name)
    cursor.execute("SET mapreduce.map.memory.mb=12288")
    cursor.execute("SET mapreduce.reduce.memory.mb=12288")
    cursor.execute("SET hive.merge.mapfiles = true")
    cursor.execute("SET hive.merge.mapredfiles = true")
    cursor.execute("SET hive.merge.size.per.task = 256000000")
    cursor.execute("SET hive.merge.smallfiles.avgsize = 128000000")
    cursor.execute("SET hive.exec.compress.output=true")
    cursor.execute("SET parquet.compression=snappy")
    cursor.execute("INSERT OVERWRITE TABLE " + stage_table_name + " SELECT * FROM " + prod_table_name + "")
    cursor.close()
    print('load to stage table done!!!')
    cursor = get_impala_connection()
    print('invalidating metadata..')
    cursor.execute("INVALIDATE METADATA " + stageTableName)
    cursor.execute("REFRESH " + stageTableName)
    print('stage table refreshed')
    cursor.close()


def rebuild_prod_table(existing_table_name, new_table_ddl):
    cursor = get_hive_connection()
    cursor.execute("drop table " + existing_table_name)
    print('Dropped existing table')
    cursor.execute(new_table_ddl)
    print('new table created')
    cursor.close()
    cursor = get_impala_connection()
    print('invalidating prod table metadata..')
    cursor.execute("INVALIDATE METADATA " + existing_table_name)
    cursor.execute("REFRESH " + existing_table_name)
    print('prod table refreshed')
    cursor.close()


def drop_stage_table(stage_table_name):
    cursor = get_impala_connection()
    cursor.execute("drop table " + stageTableName + ";")
    cursor.close()


def write_to_file(file_path, mode, file_content):
    if os.path.exists(file_path) and mode != 'a':
        os.remove(file_path)
    f1 = open(file_path, mode)
    f1.write(file_content)
    f1.close()


if __name__ == "__main__":

    # tablesList = sys.argv[1]
    source = sys.argv[1]
    parser = ConfigParser()
    parser.read('rebuild-tables.prop')
    # sources = parser.get('mrs', 'source_list').split(',')

    # outputFile = '/home/centos/impala-rebuild/automation/status.csv'
    # outputFile = '/home/centos/impala-rebuild/automation/mrsnewstatusg2.csv'
    outputFile = parser.get(source, 'status_file')
    # existingDDLDir = "/home/centos/impala-rebuild/mrs/existingdll/"
    existingDDLDir = parser.get(source, 'existing_ddl_dir')
    # newDDLDir = "/home/centos/impala-rebuild/mrs/newddl/"
    newDDLDir = parser.get(source, 'new_ddl_dir')
    # stageDDLDir = "/home/centos/impala-rebuild/mrs/stageddl/"
    stageDDLDir = parser.get(source, 'stage_ddl_dir')

    # stageDbName = "prodrebuild"
    stageDbName = parser.get('default', 'stage_db_name')

    # newLocation = 'LOCATION \'hdfs://nameservice1/data/prod/mrs2/'
    newLocation = 'LOCATION \'' + parser.get(source, 'new_hdfs_location')
    table_list = parser.get(source, 'tables_list_file')
    today = datetime.today()
    dt = today.strftime("%m%d%y_%H%M")
    with open(table_list) as f:
        for line in f:
            existingTableName = line.strip()  # antuit_prod.<tablename>
            print('Rebuild initiated for - ' + existingTableName)
            tableName = existingTableName.split('.')[1]  # <tablename> without dbname
            existingTblDbName = existingTableName.split('.')[0]
            stageTableName = stageDbName + '.' + tableName  # prodrebuild.<tablename>
            print('stage tablename - ' + stageTableName)
            '''
            Existing table ddl
            '''
            existingTableDDL = get_table_ddl(existingTableName)
            # log ddl
            print('get ' + existingTableName + ' ddl completed')
            write_to_file(existingDDLDir + tableName + '.sql', 'x', existingTableDDL)

            '''prepare new table ddl
            #1. ignore contents after TBLPROPERTIES
            #2. get old location
            #3. replace location
            #4. replace partition columns - This step will have new production table ddl
            #5. replace production dbname with stage db name - This step will have stage table ddl
            '''
            existingTableDDL = existingTableDDL.split('TBLPROPERTIES', 1)[0]
            existingTableLocation = existingTableDDL.split('LOCATION', 1)[1].strip()
            newTableDDL = existingTableDDL.split('LOCATION', 1)[
                              0] + newLocation + tableName + '/compaction_dt_' + str(dt) + '/\''

            # check this from properties
            # newTableDDL = newTableDDL.replace(') PARTITIONED BY (', ',')

            # log ddl
            print('new table ddl is created')
            write_to_file(newDDLDir + tableName + '.sql', 'x', newTableDDL)

            '''
            stage table ddl
            '''
            stageTableDDL = newTableDDL.replace(existingTblDbName, stageDbName)
            # log ddl
            print('stage table ddl is created')
            write_to_file(stageDDLDir + tableName + '.sql', 'x', stageTableDDL)

            '''
            Create stage table
            '''
            try:
                create_hive_table(stageTableDDL)
                print('create stage table done')
            except impala.error.HiveServer2Error:
                print('ERROR : Unable to create stage table')
                status = existingTableName + '|' + "NA" + '|' + "NA" + '|' + stageTableName + '|' \
                         + "stage table creation failed" + '|' + "failed" + '|' + str(datetime.now()) + '\n'
                write_to_file(outputFile, 'a', status)
                continue

            '''
            Check existing table count
            '''
            existingTableRowCount = get_table_row_count(existingTableName)
            print('old tab row count - ' + str(existingTableRowCount[0]))

            load_to_stage_table(stageTableName, existingTableName)
            '''
            Check stage table row count
            '''
            stageTabRowCount = get_table_row_count(stageTableName)
            print('stage tab row  count - ' + str(stageTabRowCount[0]))

            '''
            If count matches -> drop existing table and create new table
            '''

            '''if int(existingTableRowCount[0]) == int(stageTabRowCount[0]):
                print('counts matching')
            else:
                print('counts not matching')'''

            if int(existingTableRowCount[0]) == int(stageTabRowCount[0]):
                rebuild_prod_table(existingTableName, newTableDDL)
                newTableRowCount = get_table_row_count(existingTableName)
                print('new tab row  count - ' + str(newTableRowCount[0]))

                #"existingtabname","existingtabloc","exisitingtabrowcount","stagetabname","stagetabrowcount", "newtabrowcount"

                status = existingTableName+'|'+existingTableLocation+'|'+str(existingTableRowCount[0])+'|'+stageTableName+'|'\
                         +str(stageTabRowCount[0])+'|'+str(newTableRowCount[0])+'|'+ str(datetime.now())+'\n'
                write_to_file(outputFile,'a',status)
                drop_stage_table(stageTableName)
            else:
                print('row count not matching')
                status = existingTableName+'|'+existingTableLocation+'|'+str(existingTableRowCount[0])+'|'+stageTableName+'|'\
                         +str(stageTabRowCount[0])+'|'+'new table not created'+'|'+str(datetime.now())+'\n'
                write_to_file(outputFile,'a',status)
                drop_stage_table(stageTableName)