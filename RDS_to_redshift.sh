#!/bin/bash

## This script is wriiten to create csv dump of restore table from rds and restore it to redshift . This script will run by jenkins job named "restore redhift" .

## table name of rds db  and schema name of redshift will be taken as cli args input by user in jenkins job. 
## I have used mysql login-config-editor so used login path in this script to connect my mysql (rds db) you can avoid this.

Year=`date "+%Y"`
month=`date "+%m"`
day=`date "+%d"`



for t in "$@"
do
case "$t" in
    --source_db=*)
    source_db="${t#*=}"
    echo $source_db
    shift
    ;;
    --source_table=*)
    source_table="${t#*=}"
    echo $source_table
    shift
    ;;
    --redshift_schema=*)
    redshift_schema="${t#*=}"
    echo $redshift_schema
    shift
    ;;
    --login_path=*)
    login_path="${t#*=}"
    echo $login_path
    shift
    ;;
    *)
    echo "Invalid argument passed"
    ;;
esac
done

dump_file="/path_to_dump_folder/prod_"$source_db"_"$source_table"_dump"$day"_"$month"_"$Year".csv"
dump_file1="/path_to_dump_folder/prod_"$source_db"_"$source_table"_dump"$day"_"$month"_"$Year"_1.csv"
dump_file2="/path_to_dump_folder/prod_"$source_db"_"$source_table"_dump"$day"_"$month"_"$Year"_2.csv"
rds_user=root
rds_passwd='password'
bucketpath="s3://bucketname/"
redshift_table=${source_db}"."${source_table}
aws_credential="aws_provided_credentials"
psql_host="your_redshift_host_endpoint_redshift.amazonaws.com"
schema_file="/path_to_your_script_folder/"${source_table}"_schema.txt"
attributes_file="path_to_your_script_folder/attributes_"${source_table}

## create a new redshift copy access role
iam_role='role_arn'

## chosing different rds db based on mysql login path
if [[ $login_path == 'path1' ]]
then
rds_host='db1'
elif [[ $login_path == 'path2' ]]
then
rds_host='db2'
elif [[ $login_path == 'path3' ]]
then
rds_host='db3'
else
echo "Invalid login path .please chose from db1/db2/db3."
exit 1
fi

if [ -n "${schema_file}" ] &&  [ -n "${attributes_file}" ]
then
rm  ${schema_file} ${attributes_file}
fi 
####################### Step 1########################################
### Take table structure and csv dump from rds using db name and table name.

mysql -h ${rds_host} -u${rds_user} -p${rds_passwd}  ${source_db} --batch -e "show create table ${source_table} \G" > ${schema_file}

cat ${schema_file}|grep -v '^ENGINE&'|grep -vi '^create&'| grep -vi '^table&'| grep -vi '^row&'| grep -vi '^index&'|grep -vi '^primary&' > ${attributes_file}



attributes=`cat ${attributes_file} |
grep -v 'ENGINE' |
grep -vi 'KEY' | 
grep -vi 'create' | 
grep -vi 'table' | 
grep -vi 'row' |
grep -vi 'index' |
grep -vi 'modified' |
grep -vi 'date' |
awk -F " " '{print $1" "$2" ,"}' | 
sed 's/\<bigint([0-9][0-9]\>) unsigned/bigint/g' |
sed 's/\<bigint([0-9]\>) unsigned/bigint/g' |
sed 's/\<bigint([0-9][0-9]\>)/bigint/g' |
sed 's/\<bigint([0-9]\>)/bigint/g' |
sed 's/\<tinyint\>/smallint/g'  |
sed 's/\<smallint unsigned\>/integer/g' |
sed 's/\<mediumint interger\>/int/g' |
sed 's/\<int unsigned\>/bigint/g' |
sed 's/\<int([0-9][0-9]\>) unsigned/integer/g' |
sed 's/\<int([0-9]\>) unsigned/bigint/g' |
sed 's/\<bigint unsigned\>/varchar/g' |
sed 's/\<float\>/real/g'|
sed 's/varchar(max)/varchar/g' |
sed 's/\<double\>/double precision/g' |
sed  's/\<char\>/varchar/g' |
sed 's/\<varchar([0-9][0-9][0-9]\>)/varchar/g' |
sed 's/\<longtext\>/varchar(65535)/g' |
sed 's/\<mediumtext\>/varchar(65535)/g' |
sed 's/\<tinytext\>/text/g' |
sed 's/\<blob\>/varchar/g' |
sed 's/\<mediumblob\>/varchar/g' |
sed 's/\<datetime\>/timestamp/g' |
sed 's/\`//g' |
sed '$ s/,$//g' |
sed 's/\<varchar([0-9][0-9]\>)/varchar/g' |
sed 's/\<varchar([0-9]\>)/varchar/g' |
sed 's/\<int([0-9][0-9]\>)/integer/g' |
sed 's/\<int([0-9]\>)/integer/g' |
sed '$ s/,//g'`

echo ${attributes} > /path_to_script_folder/attribute_value.txt 
cat attribute_value.txt |sed 's/,/\n/g' |awk -F " " '{print $1}'|tr '\n' ','|sed 's/.$//' > mysql_columns.txt
mysql_columns=`cat mysql_columns.txt`
count=$(mysql -h ${rds_host} -u${rds_user} -p${rds_passwd} --batch -s -N -e "select max(ID) from ${redshift_table} ;")

i=1

while [ $i -le $count ]

do 

start=$i
## extracting 10000 rows at a time to avoid mysql timeout exception 
end=`expr $start + 10000` 

mysql -h ${rds_host} -u${rds_user} -p${rds_passwd} ${source_db} --batch -e "select ${mysql_columns} from ${source_table} where id>=$start AND id < $end ;" > ${dump_file}

#mysql -h ${rds_host} -u${rds_user} -p${rds_passwd} ${source_db} --batch -e "select * from ${source_table} where id>=$start AND id < $end ;" > ${dump_file}

#tail -n +2 ${dump_file} > ${dump_file1}

#cat ${dump_file1} |sed 's/,//g'| sed 's/\r$//g'|sed 's/\t/,/g' |sed 's/0000-00-00 00:00:00//g'> ${dump_file2}


cd /data/jk_dump/

##################################### step 2################################
############# move this csv file to s3 bucket.
for j in `ls prod_"$source_db"_"$source_table"_dump"$day"_"$month"_"$Year".csv`
do 
echo "aws s3 cp $j  ${bucketpath}"
aws s3 cp $j  ${bucketpath}
done


############################### step 3 ##############################
############## restore the redshift table before that drop table if exist 

#PGPASSWORD=your_redsift_passwd psql -h ${psql_host} -p 5439 -U root  db_name -A -c "create table  ${redshift_table}"_test" ($attributes);"
cd /data/jk_dump/

for x in `ls prod_"$source_db"_"$source_table"_dump"$day"_"$month"_"$Year".csv | sort`
do
s3_object=$x
s3_download=$bucketpath${s3_object}

PGPASSWORD=your_redsift_passwd psql -h ${psql_host} -p 5439 -U root  db_name -A -c "copy ${redshift_table}"_test" from '${s3_download}' iam_role '${iam_role}' NULL AS 'NULL' EMPTYASNULL trimblanks maxerror 20000"



done

i=$(( $i + 10000 ))

if [ -n "${dump_file}" ] &&  [ -n "${dump_file1}" ] &&  [ -n "${dump_file2}" ]
then
rm  ${dump_file} ${dump_file1} ${dump_file2}
echo "hello"
fi

done


if [ -n "${dump_file}" ] &&  [ -n "${dump_file1}" ] &&  [ -n "${dump_file2}" ]
then
rm  ${dump_file} ${dump_file1} ${dump_file2}
echo "finished"
fi

aws s3 rm ${s3_download}

exit 0 
