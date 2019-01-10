

/*Problem 1 :
Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import only records that are in “COMPLETE” status
Import all columns other than customer id
Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem1/
*/

sqoop import \
 --connect jdbc:mysql://ms.itversity.com/retail_db \
 --username retail_user \
 --password itversity \
 --table orders \
 --columns "order_id,order_date,order_status"
 --target-dir /user/rajeshs/youcan/problem1 \
 -m 1 \
 --fields-terminated-by "\t"



/*Problem 2
Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from Orders table
Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem2/
*/


[rajeshs@gw02 ~]$ sqoop import \
 --connect jdbc:mysql://ms.itversity.com/retail_db \
 --username retail_user \
 --password itversity \
 --table orders \
 --target-dir /user/rajeshs/youcan/problem2 \
 -m 1 \
 --fields-terminated-by "\t"

/*Problem 3 :
Export orders data into mysql
Input Source : /user/yourusername/jay/problem2/
Target Table : Mysql . DB = retail_export . Table Name : jay__mock_orders
Reason for somealias in table name is … to not overwrite others in mysql db in labs
*/
sqoop export \
 --connect jdbc:mysql://ms.itversity.com/retail_export \
 --username retail_user \
 --password itversity \
 --table orders_export \
 --export-dir /user/rajeshs/youcan/problem2 \
 --input-fields-terminated-by "\t"

sqoop eval \
 --connect jdbc:mysql://ms.itversity.com/retail_export \
 --username retail_user \
 --password itversity \
 --query "select * from orders_export limit 10"
 ---------------------------------------------------------------------------
| order_id    | order_date           | order_customer_id | order_status         |
---------------------------------------------------------------------------
| 1           | 2013-07-25 00:00:00.0 | 11599       | CLOSED               |
| 2           | 2013-07-25 00:00:00.0 | 256         | PENDING_PAYMENT      |
| 3           | 2013-07-25 00:00:00.0 | 12111       | COMPLETE             |
| 4           | 2013-07-25 00:00:00.0 | 8827        | CLOSED               |
| 5           | 2013-07-25 00:00:00.0 | 11318       | COMPLETE             |
| 6           | 2013-07-25 00:00:00.0 | 7130        | COMPLETE             |
| 7           | 2013-07-25 00:00:00.0 | 4530        | COMPLETE             |
| 8           | 2013-07-25 00:00:00.0 | 2911        | PROCESSING           |
| 9           | 2013-07-25 00:00:00.0 | 5657        | PENDING_PAYMENT      |
| 10          | 2013-07-25 00:00:00.0 | 5648        | PENDING_PAYMENT      |
---------------------------------------------------------------------------


4)
/*Problem 4 :
Read data from hive and perform transformation and save it back in HDFS
Read table populated from Problem 3 (jay__mock_orders )
Produce output in this format (2 fields) , sort by order count in descending and save it as avro with snappy compression in hdfs location /user/yourusername/jay/problem4/avro-snappy
ORDER_STATUS : ORDER_COUNT
COMPLETE 54
CANCELLED 89
INPROGRESS 23

Save above output in avro snappy compression in avro format in hdfs location /user/yourusername/jay/problem4/avro
*/


sqoop import \
 --connect jdbc:mysql://ms.itversity.com/retail_export \
 --username retail_user \
 --password itversity \
 --table orders_export \
 --hive-import \
 --fields-terminated-by "\t"



  sqlContext.sql("select order_status,count(order_id) as order_count from orders_export group by order_status order by order_count desc").show
+---------------+-----------+
|   order_status|order_count|
+---------------+-----------+
|       COMPLETE|      22899|
|PENDING_PAYMENT|      15030|
|     PROCESSING|       8275|
|        PENDING|       7610|
|         CLOSED|       7556|
|        ON_HOLD|       3798|
|SUSPECTED_FRAUD|       1558|
|       CANCELED|       1428|
| PAYMENT_REVIEW|        729|
+---------------+-----------+

val order_status_count_DF =  sqlContext.sql("select order_status,count(order_id) as order_count from orders_export group by order_status order by order_count desc")

scala> import com.databricks.spark.avro
import com.databricks.spark.avro

scala> sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

scala> sqlContext.getConf("spark.sql.avro.compression.codec")
res6: String = snappy

scala> order_status_count_DF.write.format("com.databricks.spark.avro").save("/user/rajeshs/youcan/problem4")


[rajeshs@gw02 ~]$ hdfs dfs -cat /user/rajeshs/youcan/problem4/part-r-00000-ade5eca8-3b2a-4db6-84d8-0c7bb97cca95.avro | head
Objavro.schema▒{"type":"record","name":"topLevelRecord","fields":[{"name":"order_status","type":["string","null"]},{"name":"order_count","type":["long","null"]}]}avro.codec
snappyopQ▒▒M5Jh1▒▒M▒(4COMPLETE▒▒q▒@aopQ▒▒M5Jh1▒▒M▒[rajeshs@gw02 ~]$

/*
Problem 5 :

Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from Orders table
Save the imported data as avro and snappy compression in hdfs location /user/yourusername/jay/problem5-avro-snappy/

Read above hdfs data
Consider orders only in “COMPLETE” status and order id between 1000 and 50000 (1001 to 49999)
Save the output (only 2 columns orderid and orderstatus) in parquet format with gzip compression in location /user/yourusername/jay/problem5-parquet-gzip/
Advance : Try if you can save output only in 2 files (Tip : use coalesce(2))

*/

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/rajeshs/youcan/problem5-avro-snappy/ \
--compress \
--compression-codec snappy \
--as-avrodatafile \
-m 1


[rajeshs@gw02 ~]$ hdfs dfs -ls -R /user/rajeshs/youcan/problem5-avro-snappy/
-rw-r--r--   2 rajeshs hdfs          0 2018-10-12 10:00 /user/rajeshs/youcan/problem5-avro-snappy/_SUCCESS
-rw-r--r--   2 rajeshs hdfs     653049 2018-10-12 10:00 /user/rajeshs/youcan/problem5-avro-snappy/part-m-00000.avro

[rajeshs@gw02 ~]$ hdfs dfs -cat /user/rajeshs/youcan/problem5-avro-snappy/part-m-00000.avro | head
Objavro.schema{"type":"record","name":"orders","doc":"Sqoop import of orders","fields":[{"name":"order_id","type":["null","int"],"default":null,"columnName":"order_id","sqlType":"4"},{"name":"order_date","type":["null","long"],"default":null,"columnName":"order_date","sqlType":"93"},{"name":"order_customer_id","type":["null","int"],"default":null,"columnName":"order_customer_id","sqlType":"4"},{"name":"order_status","type":["null","string"],"default":null,"columnName":"order_status","sqlType":"12"}],"tableName":"orders"}avro.codec
snappy▒

val ordersDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/youcan/problem5-avro-snappy/part-m-00000.avro")

ordersDF.registerTempTable("orders")
 val onlyComplete_and_orders10000to50000 = sqlContext.sql("select order_id,order_status from orders where order_status='COMPLETE' and order_id > 10000 and order_id<50000")


sqlContext.getConf("spark.sql.parquet.compression.codec")
sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")

onlyComplete_and_orders10000to50000.coalesce(2).write.parquet("/user/rajeshs/youcan/problem5-parquet-gzip/")

/*

Problem 6 :

Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from Orders table
Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem6/orders/

Import order_items table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from Order_items table
Save the imported data as text and tab delimitted in this hdfs location /user/yourusername/jay/problem6/order-items/

Read orders data from above HDFS location
Read order items data form above HDFS location
Produce output in this format (price and total should be treated as decimals)
Consider only CLOSED & COMPLETE orders
ORDER_ID ORDER_ITEM_ID PRODUCT_PRICE ORDER_SUBTOTAL ORDER_TOTAL

Note : ORDER_TOTAL = combined total price for this order

Save above output as ORC in hive table “jay_mock_orderdetails”
(Tip : Try saving into hive table from DF directly without explicit table creation manually)

Note : This problem updated on Jun 4 with more details to reduce ambiguity based on received feedback/comments from users. (Thank You )
…
*/

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/rajeshs/youcan/problem6/orders/ \
--fields-terminated-by "\t" \
-m 1


sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/rajeshs/youcan/problem6/order_items/ \
--fields-terminated-by "\t" \
-m 1


val orders = sc.textFile("/user/rajeshs/youcan/problem6/orders")
val order_items = sc.textFile("/user/rajeshs/youcan/problem6/order_items")


val ordersDF = orders.map(x=>{
val o = x.split("\t")
(o(0).toInt,o(3))
}).toDF("orde_id","order_status")


sqoop eval \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--query "describe order_items"

val order_itemsDF = order_items.map(x=>{
val o = x.split("\t")
(o(0).toInt,o(1).toInt,o(2).toInt,o(3).toFloat,o(4).toFloat,o(5).toFloat)
}).toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")


ordersDF.registerTempTable("orders")

order_itemsDF.registerTempTable("order_items")

sqlContext.sql("")

scala> order_itemsDF.show
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|            1|                  1|                  957|                1.0|             299.98|                  299.98|
|            2|                  2|                 1073|                1.0|             199.99|                  199.99|
|            3|                  2|                  502|                5.0|              250.0|                    50.0|
|            4|                  2|                  403|                1.0|             129.99|                  129.99|
|            5|                  4|                  897|                2.0|              49.98|                   24.99|
|            6|                  4|                  365|                5.0|             299.95|                   59.99|
|            7|                  4|                  502|                3.0|              150.0|                    50.0|

scala> ordersDF.show
+-------+---------------+
|orde_id|   order_status|
+-------+---------------+
|      1|         CLOSED|
|      2|PENDING_PAYMENT|
|      3|       COMPLETE|
|      4|         CLOSED|
|      5|       COMPLETE|

order_id order_item_id product_price order_subtotal order_total



scala> sqlContext.sql("select o.orde_id,oi.order_item_id,cast(oi.order_item_product_price as decimal(10,2)) product_price ,sum(oi.order_item_subtotal) order_total,cast(oi.order_item_subtotal as decimal(10,2)) order_subtotal from orders o join  order_items oi on o.orde_id=oi.order_item_order_id where o.order_status in ('COMPLETE','CLOSED') group by o.orde_id,oi.order_item_id,oi.order_item_product_price,oi.order_item_subtotal").show
+-------+-------------+-------------+------------------+--------------+
|orde_id|order_item_id|product_price|       order_total|order_subtotal|
+-------+-------------+-------------+------------------+--------------+
|    231|          567|        49.98| 49.97999954223633|         49.98|
|    231|          568|       129.99|129.99000549316406|        129.99|
|    231|          569|        39.99|  79.9800033569336|         79.98|
|    231|          570|       299.98| 299.9800109863281|        299.98|
|    431|         1072|        59.99| 239.9600067138672|        239.96|
|    431|         1073|       399.98| 399.9800109863281|        399.98|


val result = sqlContext.sql("select o.orde_id,oi.order_item_id,cast(oi.order_item_product_price as decimal(10,2)) product_price ,sum(oi.order_item_subtotal) order_total,cast(oi.order_item_subtotal as decimal(10,2)) order_subtotal from orders o join  order_items oi on o.orde_id=oi.order_item_order_id where o.order_status in ('COMPLETE','CLOSED') group by o.orde_id,oi.order_item_id,oi.order_item_product_price,oi.order_item_subtotal")

scala> result.write.format("orc").saveAsTable("order_total_summary")

hive (rajeshs_sqoop_import)> select * from order_total_summary limit 10;
OK
231     567     49.98   49.97999954223633       49.98
231     568     129.99  129.99000549316406      129.99
231     569     39.99   79.9800033569336        79.98
231     570     299.98  299.9800109863281       299.98
431     1072    59.99   239.9600067138672       239.96
431     1073    399.98  399.9800109863281       399.98
831     2072    399.98  399.9800109863281       399.98
831     2073    59.99   179.97000122070312      179.97
1031    2573    129.99  129.99000549316406      129.99
1031    2574    50      100.0   100
Time taken: 0.917 seconds, Fetched: 10 row(s)


/*
Problem 7:

Import order_items table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from Order_items table
Save the imported data as parquet in this hdfs location /user/yourusername/jay/problem7/order-items/

Import products table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from products table
Save the imported data as avro in this hdfs location /user/yourusername/jay/problem7/products/

Read above orderitems and products from HDFS location
Produce this output :

ORDER_ITEM_ORDER_ID PRODUCT_ID PRODUCT_NAME PRODUCT_PRICE ORDER_SUBTOTAL

Save above output as avro snappy in hdfs location /user/yourusername/jay/problem7/output-avro-snappy/

Note : This problem updated on Jun 4 with more details to reduce ambiguity based on received feedback/comments from users. (Thank You )

*/

sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/rajeshs/youcan/problem7/order_items/ \
--fields-terminated-by "\t" \
--as-parquetfile \
-m 1


/user/rajeshs/youcan/problem7/order_items/


sqoop import \
--connect jdbc:mysql://ms.itversity.com/retail_db \
--username retail_user \
--password itversity \
--table products \
--target-dir /user/rajeshs/youcan/problem7/products/ \
--fields-terminated-by "\t" \
--as-avrodatafile \
-m 1


order_item_order_id, product_id ,product_name ,product_price ,order_subtotal


val order_itemsDF = sqlContext.read.parquet("/user/rajeshs/youcan/problem7/order_items/")
val productsDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/youcan/problem7/products/")

val result7 = sqlContext.sql(" select order_item_order_id, product_id ,product_name ,product_price ,order_item_subtotal from order_items join products on product_id=order_item_product_id")

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

result7.write.format("com.databricks.spark.avro").save("/user/rajeshs/youcan/problem7/output_avroSnappy")


// verify :
scala> sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/youcan/problem7/output_avroSnappy").show
+-------------------+----------+--------------------+-------------+-------------------+
|order_item_order_id|product_id|        product_name|product_price|order_item_subtotal|
+-------------------+----------+--------------------+-------------+-------------------+
|                  1|       957|Diamondback Women...|       299.98|             299.98|
|                  2|      1073|Pelican Sunstream...|       199.99|             199.99|
|                  2|       502|Nike Men's Dri-FI...|         50.0|              250.0|

/*
Problem 8

Read order item from /user/yourusername/jay/problem7/order-items/
Read products from /user/yourusername/jay/problem7/products/

Produce output that shows product id and total no. of orders for each product id.
Output should be in this format… sorted by order count descending
If any product id has no order then order count for that product id should be “0”

PRODUCT_ID PRODUCT_PRICE ORDER_COUNT

Output should be saved as sequence file with Key=ProductID , Value = PRODUCT_ID|PRODUCT_PRICE|ORDER_COUNT (pipe separated)



*/

val order_itemsDF = sqlContext.read.parquet("/user/rajeshs/youcan/problem7/order_items/")
val productsDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/youcan/problem7/products/")

order_itemsDF.registerTempTable("order_items")
productsDF.registerTempTable("products")

product_id,product_price,order_count

val result8inner = sqlContext.sql("select product_id,product_price,count(order_item_id) order_count from products join order_items on order_item_product_id=product_id group by product_id,product_price order by order_count desc")

or 

val result8left = sqlContext.sql("select product_id,product_price,count(order_item_id) order_count from products left outer join order_items on order_item_product_id=product_id where order_item_quantity is not null  group by product_id,product_price order by order_count desc")

scala> result8left.count
res59: Long = 100

scala> result8inner.count
res60: Long = 100


/*Problem 9

Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
Import all records and columns from Orders table
Save the imported data as avro in this hdfs location /user/yourusername/jay/problem9/orders-avro/

Read above Avro orders data
Convert to JSON
Save JSON text file in hdfs location /user/yourusername/jay/problem9/orders-json/

Read json data from /user/yourusername/jay/problem9/orders-json/
Consider only “COMPLETE” orders.
Save orderid and order status (just 2 columns) as JSON text file in location /user/yourusername/jay/problem9/orders-mini-json/

*/

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--as-avrodatafile \
--target-dir /user/rajeshs/youcan/problem9/orders \
-m 1

val ordersDF= sqlContext.read.format("com.databricks.spark.avro").load("/user/rajeshs/youcan/problem9/orders")
 ordersDF.write.json("/user/rajeshs/youcan/problem9/order_json")
