input files :

/user/rajeshs/sqoop_import_all/retail_db/customers



1)

sqoop import \
 --connect jdbc:mysql://quickstart.cloudera/retail_db \
 --username root \
 --password cloudera \
 --table orders \
 --columns "order_id,order_status,order_date" \
 --target-dir /user/rajeshs/questions/orders_limited \
 --where "order_status='COMPLETE'" \
 -m 1


 [cloudera@quickstart ~]$ hdfs dfs -cat /user/rajeshs/questions/orders_limited/part-m-00000 | head
3,COMPLETE,2013-07-25 00:00:00.0
5,COMPLETE,2013-07-25 00:00:00.0
6,COMPLETE,2013-07-25 00:00:00.0
7,COMPLETE,2013-07-25 00:00:00.0
15,COMPLETE,2013-07-25 00:00:00.0
17,COMPLETE,2013-07-25 00:00:00.0
22,COMPLETE,2013-07-25 00:00:00.0
26,COMPLETE,2013-07-25 00:00:00.0
28,COMPLETE,2013-07-25 00:00:00.0
32,COMPLETE,2013-07-25 00:00:00.0
cat: Unable to write to output stream.

1) a)

sqoop import \
 --connect jdbc:mysql://quickstart.cloudera/retail_db \
 --username root \
 --password cloudera \
 --table orders \
 --columns "order_id,order_status,order_date" \
 --target-dir /user/rajeshs/questions/orders_limited_avro_snappy \
 --where "order_status='COMPLETE'" \
 --compress \
 --compression-codec snappy \
 --as-avrodatafile \
 -m 1


2) export :

create a empty table :

CREATE TABLE `orders_empty_for_export` (
  `order_id` int(11) NOT NULL AUTO_INCREMENT,
  `order_date` datetime NOT NULL,
  `order_customer_id` int(11) NOT NULL,
  `order_status` varchar(45) NOT NULL,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=68884 DEFAULT CHARSET=utf8 |

orders_empty_for_export

sqoop export \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table orders_empty_for_export \
--input-fields-terminated-by "," \
--export-dir /user/rajeshs/sqoop_import_all/retail_db/orders \
-m 12 


3)

val result3 = sqlContext.sql("select * from order_items where order_item_subtotal < 299")

sqlContext.getConf("spark.sql.parquet.compression.codec") //String = gzip  DEFAULT
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
result3.write.parquet("/user/rajeshs/questions/problem3/order_items299_parquet_snappy")

[cloudera@quickstart ~]$ hdfs dfs -ls /user/rajeshs/questions/problem3/order_items299_parquet_snappy 
Found 7 items
-rw-r--r--   1 cloudera supergroup          0 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/_SUCCESS
-rw-r--r--   1 cloudera supergroup        797 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/_common_metadata
-rw-r--r--   1 cloudera supergroup       4159 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/_metadata
-rw-r--r--   1 cloudera supergroup     324034 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/part-r-00000-c4c8bb8d-a37f-42f7-80df-44f25dd85046.snappy.parquet
-rw-r--r--   1 cloudera supergroup     323283 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/part-r-00001-c4c8bb8d-a37f-42f7-80df-44f25dd85046.snappy.parquet
-rw-r--r--   1 cloudera supergroup     323819 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/part-r-00002-c4c8bb8d-a37f-42f7-80df-44f25dd85046.snappy.parquet
-rw-r--r--   1 cloudera supergroup     326604 2018-10-13 20:17 /user/rajeshs/questions/problem3/order_items299_parquet_snappy/part-r-00003-c4c8bb8d-a37f-42f7-80df-44f25dd85046.snappy.parquet


4)

save customers into all the file formats and their respective compressions under /user/rajeshs/fileformats_customers

val customersRDD = sc.textFile("/user/rajeshs/sqoop_import_all/retail_db/customers")
val customersDF = customersRDD.map(y=> {
 val x= y.split(",")
 (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)) }).toDF("customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode")

customersDF.registerTempTable("customers")

1)text => 
	i)csv (gzip)
	ii)tab delim, (snappy)
	iii)pipe delim (original)
	
2) json => 
	i)original
	ii)snappy
	iii)gzip
3)parquet => 
	i) gzip
	ii)snappy
	iii)uncompressed
4)avro => 
	i)uncompressed
	ii)snappy
	iii)gzip is not supported

/user/rajeshs/fileformats_customers/text_csv


1.1) 
customersDF.rdd.map(x=>x.mkString(",")).saveAsTextFile("/user/rajeshs/fileformats_customers/text_csv")
customersDF.rdd.map(x=>x.mkString(",")).saveAsTextFile("/user/rajeshs/fileformats_customers/text_csv_gzip",classOf[org.apache.hadoop.io.compress.GzipCodec])

1.2)
customersDF.rdd.map(x=>x.mkString("\t")).saveAsTextFile("/user/rajeshs/fileformats_customers/text_tab_delim")
customersDF.coalesce(1).rdd.map(x=>x.mkString("\t")).saveAsTextFile("/user/rajeshs/fileformats_customers/text_tab_snappy",classOf[org.apache.hadoop.io.compress.SnappyCodec]
1.3) 
customersDF.rdd.map(x=>x.mkString("|")).saveAsTextFile("/user/rajeshs/fileformats_customers/text_pipe_delim")

2)
2.1)
customersDF.write.json("/user/rajeshs/fileformats_customers/json_direct_DF_writeJson")
customersDF.toJson.saveAsTextFile("/user/rajeshs/fileformats_customers/json_uncompressed")
customersDF.coalesce(1).toJSON.saveAsTextFile("/user/rajeshs/fileformats_customers/json_uncompressed_coalesce")
2.2)
customersDF.coalesce(1).toJSON.saveAsTextFile("/user/rajeshs/fileformats_customers/json_snappy",classOf[org.apache.hadoop.io.compress.SnappyCodec])
2.3)

customersDF.toJSON.saveAsTextFile("/user/rajeshs/fileformats_customers/json_gzip",classOf[org.apache.hadoop.io.compress.GzipCodec])

3)
3.1)
customersDF.write.parquet("/user/rajeshs/fileformats_customers/parquet_default_gzip")
3.2)
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
customersDF.write.parquet("/user/rajeshs/fileformats_customers/parquet_snappy")
3.3)
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
customersDF.write.parquet("/user/rajeshs/fileformats_customers/parquet_uncompressed")


4)
import com.databricks.spark.avro._
sqlContext.getConf("spark.sql.avro.compression.codec") // java.util.NoSuchElementException: spark.sql.avro.compression.codec {no default compression for avro}
4.1)
sqlContext.setConf("spark.sql.avro.compression.codec","uncompressed") // only snappy or uncompressed .. gzip wont support avro
customersDF.write.avro("/user/rajeshs/fileformats_customers/avro_uncompressed")
/*
//verify the compression either by avro tool or by below method
[cloudera@quickstart ~]$ hdfs dfs -cat /user/rajeshs/fileformats_customers/avro_uncompressed/part-r-00000-9350a6fc-44a8-4fe4-b117-78ca405a87fb.avro | head 
Objavro.codenullavro.schema{"type":"record","name":"topLevelRecord","fields":[{"name":"customer_id","type":["string","null"]},{"name":"customer_fname","type":["string","null"]},{"name":"customer_lname","type":["string","null"]},{"name":"customer_email","type":["string","null"]},{"name":"customer_password","type":["string","null"]},{"name":"customer_street","type":["string","null"]},{"name":"customer_city","type":["string","null"]},{"name":"customer_state","type":["string","null"]},{"name":"customer_zipcode","type":["string","null"]}]}-���E>�Be�x�"
*/
//observe avro.codenullavro.schema...} in above line. which mean no compression applied

4.2)
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
customersDF.write.avro("/user/rajeshs/fileformats_customers/avro_snappy")
/*
//verify the compression
[cloudera@quickstart ~]$ hdfs dfs -cat /user/rajeshs/fileformats_customers/avro_snappy/part-r-00000-02940bc7-a814-44e7-9de2-95d595e2e0da.avro | head
Objavro.codecsnappyavro.schema{"type":"record","name":"topLevelRecord","fields":[{"name":"customer_id","type":["string","null"]},{"name":"customer_fname","type":["string","null"]},{"name":"customer_lname","type":["string","null"]},{"name":"customer_email","type":["string","null"]},{"name":"customer_password","type":["string","null"]},{"name":"customer_street","type":["string","null"]},{"name":"customer_city","type":["string","null"]},{"name":"customer_state","type":["string","null"]},{"name":"customer_zipcode","type":["string","null"]}]}_�ʲ(��l������"
*/
	 // observe avro.codecsnappyavro.schema{...} in above line. which mean snappy compression applied


