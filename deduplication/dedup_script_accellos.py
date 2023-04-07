from impala.dbapi import connect
import subprocess
import time
import sys
from datetime import datetime
import logging
import json
import os
import impala.error
from configparser import ConfigParser
import shlex

def connectToHive():
  hiveConn = connect(host='',port=10000,auth_mechanism='GSSAPI',user='username',password='password',kerberos_service_name='hive')
  cursor = hiveConn.cursor()
  return cursor

def connectToImpala():
  hiveConn = connect(host='',port=21051,auth_mechanism='GSSAPI',user='username',password='password',kerberos_service_name='impala')
  cursor = hiveConn.cursor()
  return cursor

def cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return_code = proc.returncode
    return s_return_code, s_output, s_err

if __name__ == "__main__":
    conf_file=str(sys.argv[1])
    print('conf_file:',conf_file)
    
    dtm = datetime.now().strftime("%Y%m%d%H%M")
    print("start time: "+dtm)

    # Config read
    with open(conf_file) as file:
        sources = json.load(file)

    #parser.read('create_ddl.conf')
    for source in sources:
        destination_process_schema = source['destination_process_schema']
        print(destination_process_schema)
        ##destination_process_schema='accellos_pfs3x_ods_dev'

        #table_name = source['table_name']
        #print(table_name)
        tables = source['tables']
        tables = tables.strip('[').strip(']').split(',')



        process_dedup_schema = source['process_dedup_schema']
        print(process_dedup_schema)
        #process_dedup_schema='accellos_pfs3x_ods_dedup_dev'

        source_system_id = source['source_system_id']
        print(source_system_id)
        idlist = source_system_id.strip('[').strip(']').split(',')
        #idlist1 = ','.join([str(elem) for elem in idlist])
        pfs_5x = ['203','204','205','206']
        id = idlist[0]
        if id in pfs_5x:
            if len(idlist) == 1:
                number_of_id=1
                id = idlist[0]
                #condn_all = "generated_dts < date_add(current_date,-30) or source_system_id not in (id).format
                #condn_one month = "generated_dts >= date_add(current_date,-30) and source_system_id in (id).format
                condn_all = "source_system_id not in ({0})".format(id)
                condn_one_month = "source_system_id in ({0})".format(id)
            elif len(idlist) == 2:
                number_of_id=2
                print("2 id")
                id1 = idlist[0]
                id2 = idlist[1]
                #condn_all = "generated_dts < date_add(current_date,-30) or source_system_id not in ({id1},{id2}).format
                #condn_one month = "generated_dts >= date_add(current_date,-30) and source_system_id in ({id1},{id2}).format
                condn_all = "source_system_id not in ({0},{1})".format(id1,id2)
                condn_one_month = "source_system_id in ({0},{1})".format(id1,id2)
            elif len(idlist) == 3:
                number_of_id=3
                print("3 id")
                id1 = idlist[0]
                id2 = idlist[1]
                id3 = idlist[2]
                condn_all = "source_system_id not in ({0},{1},{2})".format(id1,id2,id3)
                condn_one_month = "source_system_id in ({0},{1},{2})".format(id1,id2,id3)
            else:
                number_of_id=4
                id1 = idlist[0]
                id2 = idlist[1]
                id3 = idlist[2]
                id4 = idlist[3]
                condn_all = "source_system_id not in ({0},{1},{2},{3})".format(id1,id2,id3,id4)
                condn_one_month = "source_system_id in ({0},{1},{2},{3})".format(id1,id2,id3,id4)         
            partition_with_comp = " PARTITION (source_system_id, facilityid)"
            partition_without_comp = " PARTITION (source_system_id)"
            idlist1 = ','.join([str(elem) for elem in pfs_5x])
        else:
            condn_all = "source_system_id not in ({0})".format(id)
            condn_one_month = "source_system_id in ({0})".format(id)
            partition_with_comp = " PARTITION (facilityid)"
            partition_without_comp = " "
            idlist1 = ','.join([str(elem) for elem in idlist])
            
        
        #source_system_id=202


        environment = source['environment']
        print(environment)
        #environment='dev'

        dedup_schema_query="create database IF NOT EXISTS {1} LOCATION 'hdfs://nameservice1/data/{0}/deduped/{1}'".format(environment,process_dedup_schema)
        print("dedup_schema_query:"+dedup_schema_query)
        hive_cursor = connectToHive()
        hive_cursor.execute(dedup_schema_query)
        print("created dedup schema")
        for table in tables:
            table_name=table.lower()
            print(table_name)
            process_dedup_table=table_name+'_temp'
            try:
                drop_dedup_table = "drop table if exists {0}.{1}".format(process_dedup_schema,process_dedup_table)
                print("drop_dedup_table:"+drop_dedup_table)
                hive_cursor.execute(drop_dedup_table)
                print("dropped dedup table if already exists")
                dtm = datetime.now().strftime("%Y%m%d%H%M")
                updated_dir="hdfs://nameservice1/data/{0}/deduped/{1}/{2}_{3}".format(environment,process_dedup_schema,process_dedup_table,dtm)
                dedup_table_query="create external table {0}.{1} like {2}.{3} LOCATION '{4}'".format(process_dedup_schema,process_dedup_table,destination_process_schema,table_name,updated_dir)
                print("dedup_table_query:"+dedup_table_query)
                hive_cursor.execute(dedup_table_query)
                print("created dedup table")

                total_count_query_temp="select count(*) from {}.{} where source_system_id in ({})".format(process_dedup_schema, process_dedup_table,idlist1)
                print(total_count_query_temp)
                #cursor = connectToImpala()
                #cursor.execute("INVALIDATE METADATA " + process_dedup_schema+"."+process_dedup_table)
                #cursor.execute("REFRESH " +  process_dedup_schema+"."+process_dedup_table)
                hive_cursor.execute(total_count_query_temp)
                result = hive_cursor.fetchall()
                #cursor.close()
                total_count= result[0][0]
                print("Total Number of records in temp before insert:"+str(total_count))

                sql = "SHOW COLUMNS IN {}.{}".format(destination_process_schema, table_name)
                #cursor = connectToHive()
                hive_cursor.execute(sql)
                result = hive_cursor.fetchall()
                #cursor.close()
                column_names_list = []
                for i in range (len(result)):
                    column_names_list.append(result[i][0])
                if 'comp_code' in column_names_list:
                    comp_flag = 1
                    del column_names_list[:8]
                    del column_names_list[-1:]
                    print("comp_code exists")
                else:
                    comp_flag = 0
                    del column_names_list[:9]
                    print("comp_code does not exist")
                cast_columns_raw_lst = ['`' + item + '`' for item in column_names_list]
                column_names = ", ".join(map(str,cast_columns_raw_lst))
                duplicate_count_query="select count(*) from (Select *,row_number() over (partition by is_deleted,{} ORDER BY sequence desc) AS rn from {}.{} where source_system_id in ({}))a where rn>1".format(column_names,destination_process_schema, table_name,idlist1)
                print("duplicate_count_query: "+duplicate_count_query)
                impala_cursor = connectToImpala()
                impala_cursor.execute("INVALIDATE METADATA " + destination_process_schema+"."+table_name)
                impala_cursor.execute("REFRESH " +  destination_process_schema+"."+table_name)
                impala_cursor.execute(duplicate_count_query)
                result = impala_cursor.fetchall()
                #cursor.close()
                duplicate_count= result[0][0]
                print("Number of duplicate records :"+str(duplicate_count))

                total_count_query="select count(*) from {}.{} where source_system_id in ({})".format(destination_process_schema, table_name,idlist1)
                print(total_count_query)
                #cursor = connectToImpala()
                impala_cursor.execute("INVALIDATE METADATA " + destination_process_schema+"."+table_name)
                impala_cursor.execute("REFRESH " +  destination_process_schema+"."+table_name)
                impala_cursor.execute(total_count_query)
                result = impala_cursor.fetchall()
                #cursor.close()
                total_count= result[0][0]
                print("Total Number of records :"+str(total_count))

                dedup_percentage=(duplicate_count/total_count)*100
                print("Percentage of duplication :"+str(dedup_percentage)+"%")

                if dedup_percentage >5:
                    Hive_properties= 'set hive.variable.substitute=true; set logger.PerfLogger.level = ERROR; set hive.auto.convert.join = false; set hive.exec.dynamic.partition = true; set hive.exec.dynamic.partition.mode=nonstrict; set hive.vectorized.execution.enabled=true; set hive.vectorized.execution.reduce.enabled=true; set hive.cbo.enable=true; set hive.compute.query.using.stats=true; set hive.stats.fetch.column.stats=true; set hive.stats.fetch.partition.stats=true; SET hive.exec.compress.output = true; SET parquet.compression = snappy; set hive.mapjoin.smalltable.filesize; set hive.auto.convert.join.noconditionaltask.size;SET mapreduce.map.memory.mb=12288;SET mapreduce.reduce.memory.mb=12288; SET hive.merge.mapfiles = true; SET hive.merge.mapredfiles = true; SET hive.merge.size.per.task = 134217728; SET hive.merge.smallfiles.avgsize = 134217728; set hive.merge.tezfiles=true; set hive.exec.reducers.bytes.per.reducer=134217728; SET dfs.block.size=134217728; SET parquet.block.size=134217728; set hive.exec.max.dynamic.partitions=2000; set hive.exec.max.dynamic.partitions.pernode=2000;'
                    if comp_flag == 1:
                        process_audit_cols = "generated_dts, ingested_dts, facility_id, warehouse_facility_id, ingested_from ,is_deleted,sequence,row_id"
                        Process_SQL = """{Hive_properties} INSERT INTO TABLE {process_dedup_schema}.{process_dedup_table}{partition_with_comp} SELECT * from (select * from {destination_process_schema}.{table_name} where {condn_all} UNION ALL SELECT {process_audit_cols},{column_names}, facilityid from (Select *,row_number() over (partition by is_deleted,{column_names} ORDER BY sequence desc) AS rn from {destination_process_schema}.{table_name} where {condn_one_month})a where rn=1 )b;""".format(Hive_properties=Hive_properties, process_dedup_schema=process_dedup_schema,table_name=table_name, process_audit_cols=process_audit_cols, column_names=column_names,destination_process_schema=destination_process_schema,source_system_id=source_system_id,process_dedup_table=process_dedup_table,condn_all=condn_all,condn_one_month=condn_one_month,partition_with_comp=partition_with_comp)
                        """Process_SQL = "hive -S -e \"{Hive_properties} INSERT INTO TABLE {process_dedup_schema}.{process_dedup_table} PARTITION (facilityid) SELECT {process_audit_cols},{column_names}, facilityid from (Select *,row_number() over (partition by is_deleted,{column_names} ORDER BY sequence desc) AS rn from {destination_process_schema}.{table_name} where source_system_id = {source_system_id})a where rn=1 \"".format(Hive_properties=Hive_properties, process_dedup_schema=process_dedup_schema,table_name=table_name, process_audit_cols=process_audit_cols, column_names=column_names,destination_process_schema=destination_process_schema,source_system_id=source_system_id,process_dedup_table=process_dedup_table)"""
                    else:
                        process_audit_cols = "generated_dts, ingested_dts, facility_id, warehouse_facility_id, ingested_from ,source_system,is_deleted,sequence,row_id"

                        Process_SQL = """{Hive_properties} INSERT INTO TABLE {process_dedup_schema}.{process_dedup_table}{partition_without_comp} SELECT * from (select * from {destination_process_schema}.{table_name} where {condn_all} UNION ALL SELECT {process_audit_cols},{column_names} from (Select *,row_number() over (partition by is_deleted,{column_names} ORDER BY sequence desc) AS rn from {destination_process_schema}.{table_name} where {condn_one_month})a where rn=1 )b;""".format(Hive_properties=Hive_properties, process_dedup_schema=process_dedup_schema,table_name=table_name, process_audit_cols=process_audit_cols, column_names=column_names,destination_process_schema=destination_process_schema,source_system_id=source_system_id,process_dedup_table=process_dedup_table,partition_without_comp=partition_without_comp, condn_all=condn_all, condn_one_month=condn_one_month)
                        """Process_SQL = "hive -S -e \"{Hive_properties} INSERT INTO TABLE {process_dedup_schema}.{process_dedup_table} SELECT {process_audit_cols},{column_names} from (Select *,row_number() over (partition by is_deleted,{column_names} ORDER BY sequence desc) AS rn from {destination_process_schema}.{table_name} where source_system_id = {source_system_id})a where rn=1 \"".format(Hive_properties=Hive_properties, process_dedup_schema=process_dedup_schema,table_name=table_name, process_audit_cols=process_audit_cols, column_names=column_names,destination_process_schema=destination_process_schema,source_system_id=source_system_id,process_dedup_table=process_dedup_table)"""
                    print(Process_SQL)
                    #hive_cmd = shlex.split(Process_SQL)
                    #return_code, output, error = cmd(hive_cmd)
                    (success_code, success_output, success_err) = cmd(['hive', '-e', Process_SQL])
                    if (success_code == 0):
                        total_count_query_temp1="select count(*) from {}.{} where source_system_id in ({})".format(process_dedup_schema, process_dedup_table,idlist1)
                        print("total_count_query_temp1:"+total_count_query_temp1)
                        hive_cursor.execute(total_count_query_temp1)
                        result = hive_cursor.fetchall()
                        #cursor.close()
                        total_count= result[0][0]
                        print("Total Number of records in temp after dedup insert :"+str(total_count))
                        #alter_stmt = "ALTER TABLE {}.{} SET LOCATION \'{}\'".format(destination_process_schema, table_name,updated_dir)
                        #cursor = connectToHive()
                        #print("alter_stmt"+alter_stmt)
                        #hive_cursor.execute(alter_stmt)
                        #cursor.close()
                        drop_backup= "drop table if exists {0}.{1}_bckup".format(process_dedup_schema,table_name)
                        print("drop_backup_table:"+drop_backup)
                        hive_cursor.execute(drop_backup)
                        alter_stmt_o_b = "ALTER TABLE {0}.{1} RENAME TO {2}.{1}_bckup".format(destination_process_schema, table_name,process_dedup_schema)
                        print("alter_stmt_for_rename : "+alter_stmt_o_b)
                        (success_code, success_output, success_err) = cmd(['hive', '-e', alter_stmt_o_b])
                        if (success_code == 0):
                            create_original ="create external table {0}.{1} like {2}.{3} LOCATION '{4}'".format(destination_process_schema,table_name,process_dedup_schema,process_dedup_table,updated_dir)
                            print("create_original :"+create_original)
                            hive_cursor.execute(create_original)
                            
                            duplicate_count_query="select count(*) from (Select *,row_number() over (partition by is_deleted,{} ORDER BY sequence desc) AS rn from {}.{} where source_system_id in ({}))a where rn>1".format(column_names,destination_process_schema, table_name,idlist1)
                            print("duplicate_count_query after dedup : "+duplicate_count_query)
                            #impala_cursor = connectToImpala()
                            #impala_cursor.execute("INVALIDATE METADATA " + destination_process_schema+"."+table_name)
                            #impala_cursor.execute("REFRESH " +  destination_process_schema+"."+table_name)
                            msck_command="MSCK REPAIR TABLE {}.{}".format(destination_process_schema, table_name)
                            hive_cursor.execute(msck_command)
                            hive_cursor.execute(duplicate_count_query)
                            result = hive_cursor.fetchall()
                            #cursor.close()
                            duplicate_count= result[0][0]
                            print("Number of duplicate records after dedup :"+str(duplicate_count))
                        else:
                            print('taking backup into '+process_dedup_schema+'.'+table_name+' failed')
                    else:
                        print( '*****Ingestion Failed For Table: ' + process_dedup_schema + '.' + table_name + ' With Exception: ' + str(success_err))

                else:
                    print("dedup percentage for" +table_name+"less than 5%")
            except Exception:
                print("deduplication for " +table_name+" is failed")
                #print(error)

            finally:
                print("dedup script run is completed for " +table_name)
                dtm = datetime.now().strftime("%Y%m%d%H%M")
                print("end time: "+dtm)



