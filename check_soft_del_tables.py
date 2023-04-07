from impala.dbapi import connect
import impala.error
import re


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
	
	
if __name__ == "__main__":
    table_list = sys.argv[1]
    with open(table_list) as f:
        for line in f:
            existingTableName = line.strip()
            try:
                existingTableDDL = get_table_ddl(existingTableName)
            except impala.error.HiveServer2Error:
                print('ERROR : '+existingTableName)
                continue
				
			if re.search(r'\bis_deleted\b', existingTableDDL):
				print(existingTableName+'- Soft delete exist')
			else:
				print(existingTableName+'- Soft delete NOT exist')