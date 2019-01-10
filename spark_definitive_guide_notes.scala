val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/rajeshs/datasets/online-retail-dataset.csv")
.coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")

 df.withColumn("isNullValue",when($"CustomerID".isNull,"'null'").otherwise(lit("notNull"))).show


val df1 = df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan"),expr("count(Quantity)"))

df1.withColumn("invalidInvoiceNo",col("InvoiceNo").length)


df1.withColumn("countofNo",col("InvoiceNo").length).show
import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quantity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
maxPurchaseQuantity: 
org.apache.spark.sql.Column = 
	max(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
"MM/d/yyyy H:mm"))
val purchaseDenseRank = dense_rank().over(windowSpec)

 dfWithDate.withColumn("isNullValue",where($"CustomerID".isNull,"'null'").otherwise(lit("notNull"))).show
dfWithDate.where("CustomerId IS NULL").show

dfWithDate.withColumn("isNullValue",when($"CustomerID".isNull,"yes").otherwise(lit("no"))).where("CustomerId IS NULL").show

dfWithDate.withColumn("isNullValue",when($"CustomerID".isNull,"yes").otherwise(lit("no"))).orderBy($"CustomerID".asc_nulls_first).show

dfWithDate.withColumn("isNullValue",when($"CustomerID".isNull,"yes").otherwise(lit("no"))).orderBy($"CustomerID".asc_nulls_last).show



val newDF = dfWithDate.withColumn("isNullValue",when($"CustomerID".isNull,"yes").otherwise(lit("no"))).orderBy($"CustomerID".asc_nulls_first)


scala> newDF.filter(col("isNullValue") =!= "yes").count
res60: Long = 406829

scala> newDF.filter(col("isNullValue") === "yes").count
res61: Long = 135080



dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId").select(
col("CustomerId"),
 col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()



sqoop eval \
--driver oracle.jdbc.OracleDriver \
--connect jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL \
--username ousername \
--password opassword \
--query "show databases" \
--verbose


https://stackoverflow.com/questions/7053471/understanding-the-differences-between-cube-and-rollup

val dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity")).selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity").orderBy("Date")

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity"))).select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

scala> dfNoNull.rollup("Date", "Country").agg(sum(col("Quantity"))).select("Date", "Country", "sum(Quantity)").orderBy("Date").count
res20: Long = 2022

scala> dfNoNull.cube("Date", "Country").agg(sum(col("Quantity"))).select("Date", "Country", "sum(Quantity)").orderBy("Date").count
res21: Long = 2060

dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity")).orderBy(expr("grouping_id()").desc).show()










val person = Seq(
(0, "Bill Chambers", 0, Seq(100)),
(1, "Matei Zaharia", 1, Seq(500, 250, 100)),
(2, "Michael Armbrust", 1, Seq(250, 100))).toDF("id", "name", "graduate_program", "spark_status")
val graduateProgram = Seq((0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")).toDF("id", "degree", "department", "school")
val sparkStatus = Seq((500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")).toDF("id", "status")

scala> person.show
+---+----------------+----------------+---------------+
| id|            name|graduate_program|   spark_status|
+---+----------------+----------------+---------------+
|  0|   Bill Chambers|               0|          [100]|
|  1|   Matei Zaharia|               1|[500, 250, 100]|
|  2|Michael Armbrust|               1|     [250, 100]|
+---+----------------+----------------+---------------+


scala> graduateProgram.show
+---+-------+--------------------+-----------+
| id| degree|          department|     school|
+---+-------+--------------------+-----------+
|  0|Masters|School of Informa...|UC Berkeley|
|  2|Masters|                EECS|UC Berkeley|
|  1|  Ph.D.|                EECS|UC Berkeley|
+---+-------+--------------------+-----------+


scala> sparkStatus.show
+---+--------------+
| id|        status|
+---+--------------+
|500|Vice President|
|250|    PMC Member|
|100|   Contributor|
+---+--------------+


val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpression).show()


+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+

var joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()


joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+

joinType = "left_outer"
person.join(graduateProgram, joinExpression, joinType).show()
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+


joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+----+----------------+----------------+---------------+---+-------+--------------------+-----------+

joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()
+---+-------+--------------------+-----------+
| id| degree|          department|     school|
+---+-------+--------------------+-----------+
|  0|Masters|School of Informa...|UC Berkeley|
|  1|  Ph.D.|                EECS|UC Berkeley|
+---+-------+--------------------+-----------+

val gradProgram2 = graduateProgram.union(Seq((0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
gradProgram2.join(person, joinExpression, joinType).show()
+---+-------+--------------------+-----------------+
| id| degree|          department|           school|
+---+-------+--------------------+-----------------+
|  0|Masters|School of Informa...|      UC Berkeley|
|  1|  Ph.D.|                EECS|      UC Berkeley|
|  0|Masters|      Duplicated Row|Duplicated School|
+---+-------+--------------------+-----------------+


joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()

+---+-------+----------+-----------+
| id| degree|department|     school|
+---+-------+----------+-----------+
|  2|Masters|      EECS|UC Berkeley|
+---+-------+----------+-----------+

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

person.crossJoin(graduateProgram).show()
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  0|Masters|School of Informa...|UC Berkeley|
|  0|   Bill Chambers|               0|          [100]|  2|Masters|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  2|Masters|                EECS|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  2|Masters|                EECS|UC Berkeley|
|  0|   Bill Chambers|               0|          [100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+---+-------+--------------------+-----------+


val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")

person.join(gradProgramDupe, joinExpr).show()
+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
| id|            name|graduate_program|   spark_status|graduate_program| degree|          department|     school|
+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
|  0|   Bill Chambers|               0|          [100]|               0|Masters|School of Informa...|UC Berkeley|
|  2|Michael Armbrust|               1|     [250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
|  1|   Matei Zaharia|               1|[500, 250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+

// person.join(gradProgramDupe, joinExpr).select("graduate_program").show() // error

person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()