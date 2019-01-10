[cloudera@quickstart ~]$ hdfs dfs -ls /user/rajeshs/sqoop_import_all/retail_db/
Found 9 items
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:30 /user/rajeshs/sqoop_import_all/retail_db/categories
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:32 /user/rajeshs/sqoop_import_all/retail_db/customers
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:33 /user/rajeshs/sqoop_import_all/retail_db/departments
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:35 /user/rajeshs/sqoop_import_all/retail_db/order_items
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:37 /user/rajeshs/sqoop_import_all/retail_db/orders
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:38 /user/rajeshs/sqoop_import_all/retail_db/orders_column_mapping
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:39 /user/rajeshs/sqoop_import_all/retail_db/orders_exported
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:41 /user/rajeshs/sqoop_import_all/retail_db/products
drwxr-xr-x   - cloudera supergroup          0 2018-09-09 13:43 /user/rajeshs/sqoop_import_all/retail_db/t


/*
1) Sqoop import from account table only with only filter orders = COMPLETE.
*/

sqoop eval \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--query "describe orders"

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table orders \
--where "order_status='COMPLETE'" \
--target-dir /user/rajeshs/exam2/problem1 \
-m 1 \
--fields-terminated-by ","

hdfs dfs -ls /user/rajeshs/exam2/problem1

/*
2)Sqoop export 25 million records. data was tab delimited in hdfs
*/


CREATE TABLE `orders_empty_for_export2` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` datetime NOT NULL,
  `order_customer_id` int(11) NOT NULL,
  `order_status` varchar(45) NOT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=68884 DEFAULT CHARSET=utf8 |



sqoop export \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table orders_empty_for_export2 \
--input-fields-terminated-by "\t" \
--export-dir /user/rajeshs/sqoop_import_all/retail_db/orders \
-m 12 

/*
3) Hive metastore table is given. Problem3 is the database and billing is the table
name. Get the records from billing table where charges > 10. billing table in hive
metastore need to read and charge>10 and save output as parquet file gzip compression.
*/

val order_items299 = sqlContext.sql("select * from rajeshs_sqoop_import.order_items where order_item_subtotal>299")
sqlContext.getConf("spark.sql.parquet.compression.codec")
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
order_items299.write.parquet("/user/rajeshs/exam2/problem3_hive_metastore")

//verify:
sqlContext.read.parquet("/user/rajeshs/exam2/problem3_hive_metastore").show



/*
4(1). Customer data in avro needs to print first name, last name and state. // output : "\t" text, snappy
4(2) count customers in each city and state, group by city, state. output in text
formate tab delimited.


*/
import com.databricks.spark.avro._
val customerAvroDF= sqlContext.read.avro("/user/rajeshs/fileformats_customers/avro_uncompressed")

customerAvroDF: org.apache.spark.sql.DataFrame = [customer_id: string, customer_fname: string, customer_lname: string, customer_email: string, customer_password: string, customer_street: string, customer_city: string, customer_state: string, customer_zipcode: string]

customerAvroDF.registerTempTable("customers")

val result41 = sqlContext.sql("select customer_fname,customer_lname,customer_state from customers")

result41.rdd.map(x=>x.mkString("\t")).saveAsTextFile("user/rajeshs/exam2/problem4_1", classOf[org.apache.hadoop.io.compress.SnappyCodec])

//verify
hdfs dfs -ls user/rajeshs/exam2/problem4_1



val result42 = sqlContext.sql("select customer_city,customer_state,count(customer_id) customer_count from customers group by customer_city,customer_state")

result42.rdd.map(x=>x.mkString("\t")).saveAsTextFile("user/rajeshs/exam2/problem4_2")

// verify
hdfs dfs -ls user/rajeshs/exam2/problem4_2
//or
sc.textFile("user/rajeshs/exam2/problem4_2").first
// Vista	CA	15



/*
5(1) ​json source and save it in avro format with snappy with same schema, used avro
tools to verify the codec and schema
5(2) Json to Parquet snappy customers Data
5(3) Avro to parquet format customer Data


*/

val customersJson = sqlContext.read.json("/user/rajeshs/fileformats_customers/json_uncompressed")
customersJson: org.apache.spark.sql.DataFrame = [customer_city: string, customer_email: string, customer_fname: string, customer_id: string, customer_lname: string, customer_password: string, customer_state: string, customer_street: string, customer_zipcode: string]

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
sqlContext.getConf("spark.sql.avro.compression.codec")
//res16: String = snappy
customersJson.write.format("com.databricks.spark.avro").save("user/rajeshs/exam2/problem5_json_to_avro_snappy")

// verify
hdfs dfs -ls user/rajeshs/exam2/problem5_json_to_avro_snappy

[cloudera@quickstart ~]$ hdfs dfs -cat user/rajeshs/exam2/problem5_json_to_avro_snappy/part-r-00000-b19112d4-dcb4-4fdb-a8be-1d4fe9e0ee2e.avro | head
Objavro.codec
                snappyavro.schema{"type":"record","name":"topLevelRecord","fields":[{"name":"customer_city","type":["string","null"]},{"name":"customer_email","type":["string","null"]},{"name":"customer_fname","type":["string","null"]},{"name":"customer_id","type":["string","null"]},{"name":"customer_lname","type":["string","null"]},{"name":"customer_password","type":["string","null"]},{"name":"customer_state","type":["string","null"]},{"name":"customer_street","type":["string","null"]},{"name":"customer_zipcode","type":["string","null"]}]}��{��d�~�DF?c��"
"
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
sqlContext.getConf("spark.sql.parquet.compression.codec")
//res19: String = snappy
customersJson.write.parquet("user/rajeshs/exam2/problem5_json_to_parquet_snappy")


// verify :
[cloudera@quickstart ~]$ hdfs dfs -ls user/rajeshs/exam2/problem5_json_to_parquet_snappy
Found 5 items
-rw-r--r--   1 cloudera cloudera          0 2018-10-13 23:21 user/rajeshs/exam2/problem5_json_to_parquet_snappy/_SUCCESS
-rw-r--r--   1 cloudera cloudera       1033 2018-10-13 23:21 user/rajeshs/exam2/problem5_json_to_parquet_snappy/_common_metadata
-rw-r--r--   1 cloudera cloudera       3508 2018-10-13 23:21 user/rajeshs/exam2/problem5_json_to_parquet_snappy/_metadata
-rw-r--r--   1 cloudera cloudera     154717 2018-10-13 23:21 user/rajeshs/exam2/problem5_json_to_parquet_snappy/part-r-00000-13c79710-15e4-47f0-a6b0-f9c4b76e2060.snappy.parquet
-rw-r--r--   1 cloudera cloudera     153886 2018-10-13 23:21 user/rajeshs/exam2/problem5_json_to_parquet_snappy/part-r-00001-13c79710-15e4-47f0-a6b0-f9c4b76e2060.snappy.parquet


/*
6). customer ​first name, last name sort with last name name and leave first name
without sorting. 
output format "fname lname". 
​save in text format fname space lname. (Verify lname, fname order while
writing)
*/

val customersDF6 = sqlContext.read.json("/user/rajeshs/fileformats_customers/json_uncompressed")
customersDF6: org.apache.spark.sql.DataFrame = [customer_city: string, 
customer_email: string, customer_fname: string, customer_id: string, customer_lname: string, 
customer_password: string, customer_state: string, customer_street: string, customer_zipcode: string]

customersDF6.registerTempTable("cust6")

val result6 = sqlContext.sql("select concat(customer_fname,' ',customer_lname) as fullname from cust6 order by customer_lname")

result6.rdd.map(x=>x.mkString("\t")).saveAsTextFile("user/rajeshs/exam2/problem6_fname_space_lname")

// verify 
hdfs dfs -ls user/rajeshs/exam2/problem6_fname_space_lname
//or
sc.textFile("user/rajeshs/exam2/problem6_fname_space_lname").first
// res26: String = Marie Abbott

/*

7) billing and customer in hdfs , both tab delimited, fins customer owed amount for
single billing transaction. save fname space lname tab amount in text format

do orders and order_items

scala> sqlContext.sql("describe rajeshs_sqoop_import.products").show
+-------------------+---------+-------+
|           col_name|data_type|comment|
+-------------------+---------+-------+
|         product_id|      int|   null|
|product_category_id|      int|   null|
|       product_name|   string|   null|
|product_description|   string|   null|
|      product_price|   double|   null|
|      product_image|   string|   null|
+-------------------+---------+-------+


scala> sqlContext.sql("describe rajeshs_sqoop_import.categories").show
+--------------------+---------+-------+
|            col_name|data_type|comment|
+--------------------+---------+-------+
|         category_id|      int|   null|
|category_departme...|      int|   null|
|       category_name|   string|   null|
+--------------------+---------+-------+


output :top 10 priced categories and their product names
/user/rajeshs/exam2/problem7_billing_alternative_prod_cat


*/
sqlContext.sql("use rajeshs_sqoop_import")
sqlContext.sql("describe rajeshs_sqoop_import.categories").show
sqlContext.sql("describe rajeshs_sqoop_import.products").show
//incomplete

sqlContext.sql("select product_category_id,sum(product_price) categoryTotalPrice,category_name from products join categories on product_category_id = category_id  group by product_category_id order by categoryTotalPrice desc").show
sqlContext.sql("select product_category_id,sum(product_price) categoryTotalPrice from products  group by product_category_id order by categoryTotalPrice desc").show
 // propblem incomplete due to no datasets

/*

8) customers in hdfs , filter for state = TX and save as parquet uncompressed format
 input : /user/rajeshs/fileformats_customers/text_csv
 output : 
​first name, last name

path :  /user/rajeshs/exam2/problem8_TX_state
*/
val customerRDD8 =  sc.textFile("/user/rajeshs/fileformats_customers/text_csv")

val customer8DF = customerRDD8.map(x=> {
val c = x.split(",")
(c(1),c(2),c(7))
}).toDF("fname","lname","state")


customer8DF.registerTempTable("cust8")
val result8 = sqlContext.sql("select fname,lname from cust8 where state='TX'")

sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
sqlContext.getConf("spark.sql.parquet.compression.codec")
//String = uncompressed

result8.write.parquet("/user/rajeshs/exam2/problem8_TX_state")

// verify
hdfs dfs -ls /user/rajeshs/exam2/problem8_TX_state
and 
sqlContext.read.parquet("/user/rajeshs/exam2/problem8_TX_state").show



/*

9). Out of 20 columns in employee table which is in hdfs in text file format with tab
delimiter need to output first 7 columns which has id, fname, lname and address details
with pipe as delimiter and text file format
*/

val customerRDD9 = sc.textFile("/user/rajeshs/fileformats_customers/text_tab_delim")
customerRDD9.first
//String = 1	Richard	Hernandez	XXXXXXXXX	XXXXXXXXX	6303 Heather Plaza	Brownsville	TX	78521

val customer9ResultRDD = customerRDD9.map(x=> {
val c = x.split("\t")
(c(0),c(1),c(2),c(3),c(4),c(5),c(6))
})

customer9ResultRDD.first
// (String, String, String, String, String, String, String) = (1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville)
customer9ResultRDD.map(x=> x._1,x._2,x._3,x._4,x._5,x._6,x._7).first

customer9ResultRDD.map(x=>x.mkString("\t")).saveAsTextFile("/user/rajeshs/exam2/problem9_7cols_out_of20")

customer9ResultRDD.map(x=>x._).map(x=>x.mkString("\t")).saveAsTextFile("/user/rajeshs/exam2/problem9_7cols_out_of20")



//

val customer9ResultDF = customerRDD9.map(x=> {
val c = x.split("\t")
(c(0),c(1),c(2),c(3),c(4),c(5),c(6))
})toDF()

 customer9ResultDF.rdd.map(x=>x.mkString("|")).saveAsTextFile("/user/rajeshs/exam2/problem9_7cols_out_of20_with_DF")
// verify

[cloudera@quickstart ~]$ hdfs dfs -ls /user/rajeshs/exam2/problem9_7cols_out_of20_with_DF
Found 3 items
-rw-r--r--   1 cloudera supergroup          0 2018-10-14 00:28 /user/rajeshs/exam2/problem9_7cols_out_of20_with_DF/_SUCCESS
-rw-r--r--   1 cloudera supergroup     420623 2018-10-14 00:28 /user/rajeshs/exam2/problem9_7cols_out_of20_with_DF/part-00000
-rw-r--r--   1 cloudera supergroup     420987 2018-10-14 00:27 /user/rajeshs/exam2/problem9_7cols_out_of20_with_DF/part-00001
[cloudera@quickstart ~]$ hdfs dfs -cat /user/rajeshs/exam2/problem9_7cols_out_of20_with_DF/part-00000 | head
1|Richard|Hernandez|XXXXXXXXX|XXXXXXXXX|6303 Heather Plaza|Brownsville
2|Mary|Barrett|XXXXXXXXX|XXXXXXXXX|9526 Noble Embers Ridge|Littleton
3|Ann|Smith|XXXXXXXXX|XXXXXXXXX|3422 Blue Pioneer Bend|Caguas
4|Mary|Jones|XXXXXXXXX|XXXXXXXXX|8324 Little Common|San Marcos
5|Robert|Hudson|XXXXXXXXX|XXXXXXXXX|10 Crystal River Mall |Caguas

