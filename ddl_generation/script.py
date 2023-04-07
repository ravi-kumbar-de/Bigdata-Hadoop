# DDL Generation Script. Accellos PFS

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

from impala.dbapi import connect
from collections import OrderedDict
import subprocess
import shlex
import yaml
import sys
import os
import re

sc = SparkContext("local", "first app")
sqlContext = SQLContext(sc)

tables = ['L_FRT_LOAD']  # Provide the table name here
with open(sys.argv[1]) as file:
    config = yaml.safe_load(file)
print(config)


# {'source': 'Accellos', 'source_tableList': [], 'source_schema': 'DELFOUR',
# 'db_config': {'url': 'jdbc:oracle:thin:@//10.202.93.160:1521/A11', 'username': 'Brillio',
# 'password': 'Polar1989', 'driver': 'oracle.jdbc.driver.OracleDriver'}}


def cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return_code = proc.returncode
    return s_return_code, s_output, s_err


def connectToImpala(query):
    impalaConn = connect(host='10.0.0.53', port=21050, auth_mechanism='GSSAPI', user='username', password='password',
                         kerberos_service_name='impala')
    cursor = impalaConn.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print('Exception', e, '\n for query ', query)
    finally:
        impalaConn.close()


def get_table_list(sql_context, source):
    if source == 'Accellos':
        df = sql_context.read \
            .format("jdbc") \
            .option("url", config['db_config']['url']) \
            .option("dbtable",
                    "(SELECT Table_Name FROM all_tables WHERE owner = '{0}')".format(config['source_schema'])) \
            .option("user", config['db_config']['username']) \
            .option("password", config['db_config']['password']) \
            .option("driver", config['db_config']['driver']) \
            .load()
    return df


def create_df(sql_context, source, table_name):
    if source == 'Accellos':
        df = sql_context.read \
            .format("jdbc") \
            .option("url", config['db_config']['url']) \
            .option("dbtable", "(SELECT * FROM DELFOUR.{0} WHERE ROWNUM <= 100)".format(table_name)) \
            .option("user", config['db_config']['username']) \
            .option("password", config['db_config']['password']) \
            .option("driver", config['db_config']['driver']) \
            .load()

    schema = OrderedDict((column, col_type) for column, col_type in df.dtypes)
    df = df.select(
        [col(column).cast(StringType()).alias(column) if schema[column] == "timestamp" else col(column) for column in
         schema])

    audit_columns = config['audit_columns']
    if audit_columns == 'Raw':
        df = df.select(lit(None).cast(StringType()).alias("genrated_dts"),
                       lit(None).cast(StringType()).alias("ingested_dts"),
                       lit(None).cast(StringType()).alias("facility_id"),
                       lit(None).cast(StringType()).alias("warehouse_facility_id"),
                       lit(None).cast(StringType()).alias("ingested_from"),
                       "*")

    df.coalesce(1).write.mode('overwrite').parquet('/user/rkumbar/ddl/{0}'.format(table_name))


def file_location(table_name):
    args = "hadoop fs -ls {0}{1} | awk '{{print $8}}'".format(config['hdfs_location'], table_name)
    args = shlex.split(args)
    a, b, c = cmd(args)
    file_list = b.split()
    file = [file for file in file_list if file.endswith(".parquet")][0]
    return file


table_list_df = get_table_list(sqlContext, config['source'])
tables_1 = table_list_df.select("TABLE_NAME").rdd.flatMap(lambda x: x).collect()
print(tables_1)
tables_1 = tables_1[67:69]
for table in tables_1:
    create_df(sqlContext, config['source'], table)
    print('Parquet File Stored in HDFS')
    file_loc = file_location(table)
    print("File Location Found")
    impala_cmd = "impala-shell -i 10.0.0.53 -q \"CREATE TABLE IF NOT EXISTS {0}.accel_{1}_testing LIKE PARQUET '{2}' STORED AS PARQUET\"".format(
        config['impala_schema'], table, file_loc)
    # print(impala_cmd)
    impala_cmd = shlex.split(impala_cmd)
    a, b, c = cmd(impala_cmd)
    print('Table: {0} is created.'.format(table))
    ddl = connectToImpala("SHOW CREATE TABLE {0}.accel_{1}_testing".format(config['impala_schema'], table))
    ddl = ddl[0][0].replace('\n', '') \
        .replace("COMMENT 'Inferred from Parquet file.'", '') \
        .replace('CREATE TABLE', 'CREATE EXTERNAL TABLE') \
        .split('STORED AS')[0]
    ddl = ddl + " STORED AS $'FILEFORMAT' " + "LOCATION '$'PATH'' ;"
    with open('{0}{1}.sql'.format(config['output_dir'], table), 'w+') as file:
        file.write(ddl)