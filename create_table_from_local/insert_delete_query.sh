OIFS=$IFS;
kinit -kt /home/rkumbar/cdhnp-all-dev.keytab cdhnp-all-dev

db_to_overwrite=wcs_backfill_raw_dev
from_db=wcs_restore_dev
TableList=('alarm_groups')

for table in "${TableList[@]}"
do
  echo $table
  table_del=$( echo $table'_del')
  #echo $table_del
  #echo "$db_to_overwrite.$table"
  
  fields=`impala-shell  -i 172.23.66.153 -B -q "describe $db_to_overwrite.$table" --quiet 2>/dev/null|awk '{print $1}'`
  echo "$fields"
  fields_with_sep=`echo $fields|tr ' ' '|'`
  echo "$fields_with_sep"
  IFS='|' read -r -a fields_with_arr <<< "$fields_with_sep"
  echo "$fields_with_arr"
  final_string=""
  for col in "${fields_with_arr[@]}"
    do
      #echo "$col"
      if [ $col == "generated_dts" ]; then
        final_string=`echo "${final_string} wcs.ingested_dts as generated_dts, "`
      elif [ $col == "ingested_dts" ]; then
        final_string=`echo "${final_string} current_timestamp() as ingested_dts, "`
      elif [ $col == "facility_id" ]; then
        final_string=`echo "${final_string} '41' as facility_id, "`
      elif [ $col == "warehouse_facility_id" ]; then
        final_string=`echo "${final_string} '0055' as warehouse_facility_id, "`
      elif [ $col == "ingested_from" ]; then
        final_string=`echo "${final_string}concat('cdh 5.x wcs_facilitydata41_raw.$table | Back-fill to raw' )  as ingested_from, "`
	  elif [ $col == "ingested_dt_key" ]; then
        final_string=`echo "${final_string}  from_unixtime(unix_timestamp(CURRENT_TIMESTAMP()), 'yyyyMMdd')  as ingested_dt_key, "`
      else
        final_string=`echo "${final_string} ${col}, "`
        #echo $final_string
      fi
    done
	
  final_string=`echo ${final_string} | sed 's/.$//'`
  query=`echo -e  "INSERT OVERWRITE TABLE ${db_to_overwrite}.${table} PARTITION (ingested_dt_key) select $final_string from $from_db.$table wcs where  wcs.is_deleted = 'N'" `

  query2=`echo -e  "INSERT OVERWRITE TABLE ${db_to_overwrite}.${table_del} select $final_string from $from_db.$table wcs where  wcs.is_deleted = 'Y'" `
  #echo $query
  #echo $query2
  
  #hive -e "set hive.exec.dynamic.partition.mode=nonstrict;$query" >> /home/rkumbar/wcs/wcsrawlog/$table.out
  #hive -e "$query2"
done
