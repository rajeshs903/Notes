// hdfs dfs -mkdir /user/rajeshs/questions/

// [cloudera@quickstart retail_db]$ pwd
// /home/cloudera/data/retail_db



/*
1) sqoop import with only few columns
Sqoop import from account table only with selected columns
only order_status that are completed
<<<<<<< HEAD

sqoop eval --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --P --query "select * from order_items limit 10"
password : cloudera


=======
>>>>>>> 0bd0d8520c22c6cb63e9adfcbbc0c053396e3243
*/
1)parquetfile snappy: 

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table customers \
--target-dir /user/rajeshs/practice/problem1/output_parquet_snappy2 \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--as-parquetfile \
-m 1

2)parquetfile uncompressed: 

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table customers \
--target-dir /user/rajeshs/practice/problem1/output_parquet_uncompressed \
--as-parquetfile \
-m 1


3) textfile snappy :  

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table customers \
--target-dir /user/rajeshs/practice/problem1/output_text_snappy2 \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
-m 1


/*
2) sqoop export with tab delimited data ..export to mysql provided table
Sqoop export 25 million records. data was tab delimited in hdfs

mysql -u retail_dba -h quickstart -p
password : cloudera

or 

mysql -u retail_dba -h nn01.itversity.com -p
password : itversity


CREATE TABLE `orders_exported` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` datetime NOT NULL,
  `order_status` varchar(45) NOT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=68884 DEFAULT CHARSET=utf8 


*/

sqoop export \
--connect jdbc:mysql://nn01.itversity.com/retail_export \
--username retail_dba \
--password itversity \
--table customers_export_rajeshs \
--export-dir /user/rajeshs/sqoop_import/retail_db/customers \
-m 12 \
--input-fields-terminated-by ","

//verify the answer :
sqoop eval \
--connect jdbc:mysql://nn01.itversity.com/retail_export \
--username retail_dba \
--password itversity \
--query "select * from retail_export.customers_export_rajeshs limit 10"
sqoop eval \
--connect jdbc:mysql://nn01.itversity.com/retail_export \
--username retail_dba \
--password itversity \
--query "select count(*) from retail_export.customers_export_rajeshs"
------------------------
| count(*)             |
------------------------
| 12435                |
------------------------

[rajeshs@gw02 ~]$ hdfs dfs -cat /user/rajeshs/sqoop_import/retail_db/customers/part-m-0000* | wc -l
12435

/*
3) select data where charge > 10$. Save as parquet gzip format
Hive metastore table is given. Problem3 is the database and billing is the table
name. Get the records from billing table where charges > 10. billing table in hive
metastore need to read and charge>10 and save output as parquet file gzip compression.

order_items
*/

scala> sqlContext.getConf("spark.sql.parquet.compression.codec")
res14: String = gzip

scala> val result3 = sqlContext.sql("select * from retail_db.order_items where order_item_subtotal> 299.95")
scala> sqlContext.read.parquet("/user/rajeshs/practice/problem3/output_parquet_gzip").show


/*
4) Given data in tab format, 8 columns total. get the first name, last name, state only.
Save it as parquet snappy format.
Customer data in text tab delim, needs to print first name, last name and state and
save as parquet with snappy.
*/


val customersRDD = sc.textFile("/user/rajeshs/practice/problem4/input/tab_delimited_text/")

val customerDF = customersRDD.map( x=> {
	val c= x.split("\t")
	(c(1),c(2),c(7))
}).toDF("fname","lname","state")


scala> customerDF.show
+-----------+---------+-----+
|      fname|    lname|state|
+-----------+---------+-----+
|    Richard|Hernandez|   TX|
|       Mary|  Barrett|   CO|
|        Ann|    Smith|   PR|

sqlContext.getConf("spark.sql.parquet.compression.codec")
res27: String = gzip

 sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

 sqlContext.getConf("spark.sql.parquet.compression.codec")
res29: String = snappy

customerDF.write.parquet("/user/rajeshs/practice/problem4/output_snappy")


//verify the answer :

hdfs dfs -ls -R "/user/rajeshs/practice/problem4/output_snappy"

or

sqlContext.read.parquet("/user/rajeshs/practice/problem4/output_snappy").show


/*
5) Provided almost similar data as previous.. except they change street column to
address columns. Select count of customers based on each city and state.. save as text
tab delimiter.
*/

val customerRDD = sc.textFile("/user/rajeshs/practice/problem5/input/input/tab_delimited_text/")

customerRDD.first.split("\t").foreach(println)

val custDF = customerRDD.map(x=> {
 val c = x.split("\t")
 (c(0).toInt,c(6),c(7))}).toDF("customer_id","city","state")


custDF.registerTempTable("customers")

sqlContext.sql("select city,state,count(customer_id) as num_employee_in_state_city from customers group by city,state").show

result5.rdd.map(x=>x.mkString("\t")).saveAsTextFile("/user/rajeshs/practice/problem5/output_text_tab_delim")


//verify the answer
scala> sc.textFile("/user/rajeshs/practice/problem5/output_text_tab_delim").take(10).foreach(println)
Carmichael      CA      8
Vista   CA      15
Spring Valley   CA      12
Normal  IL      5

//saving the same result5 in avro


/*
6) String Manipulation, they provided many columns comma delimiter text format . Just
want output as fname, lname, column name as “alias “ concatenation first character of
fname and lname.
Ex: Meghal Gandhi MGandhi.
Save as parquet gzip format.
*/
val customers = sc.textFile("/user/rajeshs/sqoop_import/retail_db/customers")

val customersDF = customers.map(x=> {
}).toDF()

/*
7) Famous billing customers question.
billing and customer in hdfs , both tab delimited, fins customer owed amount for
single billing transaction. save fname space lname tab amount in text format.
*/


val customersRDD = sc.textFile("/user/rajeshs/practice/problem6/input/customers")

val customersDF = customersRDD.map(x=> {
 val c = x.split(",")
 (c(1),c(2))}).toDF("fname","lname")

customersDF.registerTempTable("cust")

 val result6 = sqlContext.sql("select fname,lname,concat(substr(fname,1,1),lname) fullname from cust")

scala> sqlContext.getConf("spark.sql.parquet.compression.codec")
res13: String = gzip
//default for parquet 
result6.write.parquet("/user/rajeshs/practice/problem6/output_parquet_gzip")


//Verify the answer : 
hdfs dfs -ls -R "/user/rajeshs/practice/problem6/output_parquet_gzip"

-rw-r--r--   2 rajeshs hdfs          0 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/_SUCCESS
-rw-r--r--   2 rajeshs hdfs        378 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/_common_metadata
-rw-r--r--   2 rajeshs hdfs       1865 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/_metadata
-rw-r--r--   2 rajeshs hdfs      22467 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/part-r-00000-26d3b9f9-d976-4eba-af2b-a61ffee518f7.gz.parquet
-rw-r--r--   2 rajeshs hdfs      22326 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/part-r-00001-26d3b9f9-d976-4eba-af2b-a61ffee518f7.gz.parquet
-rw-r--r--   2 rajeshs hdfs      22494 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/part-r-00002-26d3b9f9-d976-4eba-af2b-a61ffee518f7.gz.parquet
-rw-r--r--   2 rajeshs hdfs      22336 2018-10-12 06:08 /user/rajeshs/practice/problem6/output_parquet_gzip/part-r-00003-26d3b9f9-d976-4eba-af2b-a61ffee518f7.gz.parquet

ii)save parquet uncompressed  :

sqlContext.getConf("spark.sql.parquet.compression.codec")
res15: String = gzip
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
sqlContext.getConf("spark.sql.parquet.compression.codec")
res17: String = snappy
result6.write.parquet("/user/rajeshs/practice/problem6/output_parquet_snappy")

//verify the answer:

 hdfs dfs -ls -R "/user/rajeshs/practice/problem6/output_parquet_snappy"
-rw-r--r--   2 rajeshs hdfs          0 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/_SUCCESS
-rw-r--r--   2 rajeshs hdfs        378 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/_common_metadata
-rw-r--r--   2 rajeshs hdfs       1917 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/_metadata
-rw-r--r--   2 rajeshs hdfs      30440 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/part-r-00000-2e48ea8d-e4c7-4eaa-8bee-788c1e201d5e.snappy.parquet
-rw-r--r--   2 rajeshs hdfs      30554 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/part-r-00001-2e48ea8d-e4c7-4eaa-8bee-788c1e201d5e.snappy.parquet
-rw-r--r--   2 rajeshs hdfs      30489 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/part-r-00002-2e48ea8d-e4c7-4eaa-8bee-788c1e201d5e.snappy.parquet
-rw-r--r--   2 rajeshs hdfs      30471 2018-10-12 06:10 /user/rajeshs/practice/problem6/output_parquet_snappy/part-r-00003-2e48ea8d-e4c7-4eaa-8bee-788c1e201d5e.snappy.parquet




// II) do the same with avro files : default compression , snappy compression, uncompressed


//a) default :

scala> import com.databricks.spark.avro
import com.databricks.spark.avro

scala> result6.write.format("com.databricks.spark.avro").save("/user/rajeshs/practice/problem6/output_avro_default")

hdfs dfs -ls -R "/user/rajeshs/practice/problem6/output_avro_default"
-rw-r--r--   2 rajeshs hdfs          0 2018-10-12 06:16 /user/rajeshs/practice/problem6/output_avro_default/_SUCCESS
-rw-r--r--   2 rajeshs hdfs      31851 2018-10-12 06:16 /user/rajeshs/practice/problem6/output_avro_default/part-r-00000-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r--   2 rajeshs hdfs      31890 2018-10-12 06:16 /user/rajeshs/practice/problem6/output_avro_default/part-r-00001-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r--   2 rajeshs hdfs      31749 2018-10-12 06:16 /user/rajeshs/practice/problem6/output_avro_default/part-r-00002-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r--   2 rajeshs hdfs      31753 2018-10-12 06:16 /user/rajeshs/practice/problem6/output_avro_default/part-r-00003-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro

//verifying the compression 

[rajeshs@gw02 ~]$ hdfs dfs -get "/user/rajeshs/practice/problem6/output_avro_default" .
[rajeshs@gw02 ~]$ cd output_avro_default/
[rajeshs@gw02 output_avro_default]$ ll
total 128
-rw-r--r-- 1 rajeshs students 31851 Oct 12 06:18 part-r-00000-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r-- 1 rajeshs students 31890 Oct 12 06:18 part-r-00001-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r-- 1 rajeshs students 31749 Oct 12 06:18 part-r-00002-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r-- 1 rajeshs students 31753 Oct 12 06:18 part-r-00003-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
-rw-r--r-- 1 rajeshs students     0 Oct 12 06:18 _SUCCESS


[rajeshs@gw02 output_avro_default]$ avro-tools getmeta part-r-00000-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro
log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
avro.schema     {"type":"record","name":"topLevelRecord","fields":[{"name":"fname","type":["string","null"]},{"name":"lname","type":["string","null"]},{"name":"fullname","type":["string","null"]}]}
avro.codec      snappy

scala> sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/practice/problem6/output_avro_default/part-r-00001-055ec1cd-98c0-4fc9-a3bd-6fef62482cff.avro").show
+--------+-------+--------+
|   fname|  lname|fullname|
+--------+-------+--------+
| Phillip|  Smith|  PSmith|
|    Mary|  Brown|  MBrown|
|Kimberly|  Marsh|  KMarsh|
|   Aaron|  Smith|  ASmith|


 sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/practice/problem6/output_avro_default/").show
+-----------+---------+----------+
|      fname|    lname|  fullname|
+-----------+---------+----------+
|    Richard|Hernandez|RHernandez|
|       Mary|  Barrett|  MBarrett|
|        Ann|    Smith|    ASmith|

b) avro - gzip : gzip wont support avro.
c) avro - uncompressed :

sqlContext.setConf("spark.sql.avro.compression.codec", "uncompressed")

result6.write.format("com.databricks.spark.avro").save("/user/rajeshs/practice/problem6/output_avro_uncompressed")
hdfs dfs -get "/user/rajeshs/practice/problem6/output_avro_uncompressed" .

[rajeshs@gw02 output_avro_uncompressed]$ avro-tools getmeta part-r-00000-e2446280-8dcf-4bee-9c76-26cfd50f0d8a.avro
log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
avro.schema     {"type":"record","name":"topLevelRecord","fields":[{"name":"fname","type":["string","null"]},{"name":"lname","type":["string","null"]},{"name":"fullname","type":["string","null"]}]}
avro.codec      null


/*
7) Famous billing customers question.
billing and customer in hdfs , both tab delimited, fins customer owed amount for
single billing transaction. save fname space lname tab amount in text format.
*/

val customers = sc.textFile("/user/root/exam/question_7/customers")
val billing = sc.textFile("/user/root/exam/question_7/input")

val billingDF = billing.map(x=> {
 val b= x.split("\t")
(b(2),b(3))
}).toDF("customer_id","status")

val customerDF = customers.map( x=> 
 val c = x.split("\t")
(c(0),c(1),c(2),c(8))}).toDF("customer_id","fname","lname","amount")

billingDF.registerTempTable("billling7")
customersDF.registerTempTable("customers7")

val result7 =sqlContext.sql("select concat(fname,' ',lname) as fullname,amount,status from customer7 inner join billling7 on customer7.customer_id=billling7.customer_id where amount is not null ")
result7.rdd.map(x=> x.mkString("\t")).saveAsTextFile("/user/rajeshs/practice/problem7/text_customer_billing_problem")



/*
8) Parquet sensor data has given . Find the avg temperature based on phone model. save
as comma delim text format
*/
val sensorData = sqlContext.read.parquet("path")
sensorData.registerTempTable("sensors")
val result8 =sqlContext.sql("select phone_model,avg(temperature) from sensors group by phone_model")

result8.rdd.map(x=> x.mkString(",")).saveAsTextFile("/user/rajeshs/practice/problem/sensors_problem8")

//verify the answer
sc.textFile("/user/rajeshs/practice/problem/sensors_problem8").take(10).foreach(println)

/*
9) Employee birthday anniversary report. Provided data in text with tab delim.
Get the first name space last name, employee birthday Year/Month. sort by employee
birthday only. if two birthdays are same then no need to sort by name.
Ex: Meghal Gandhi 11/19
*/


val employee = sc.textFile("inputpath")

val employeeDF = employee.map( x=> {
 val e = x.split("\t")
(e(1),e(2),e(7))
})toDF("fname","lname","dob")

employeeDF.registerTempTable("employee")
sqlContext.sql("select concat(fname,' ',lname) as fullname,substr(dob,1,5) as birthday from employee order by birthday")
