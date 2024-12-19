import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

// // approach 1: apply the schema to RDD
// val seq: Seq[(String, Int)] = Seq(("Bob", 14), ("Alice", 18))
// val rdd: RDD[(String, Int)] = sc.parallelize(seq)

// val schema:StructType = StructType( Array(
//     StructField("name", StringType),
//     StructField("age", IntegerType)
// ))

// val rowRDD: RDD[Row] = rdd.map(fileds => Row(fileds._1, fileds._2))

// val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)

// dataFrame.show

// // approach 2: toDF without explicit schema defintion
// import spark.implicits._
// val dataFrame2: DataFrame = rdd.toDF
// dataFrame2.printSchema
// /** Schema显示
// root
// |-- _1: string (nullable = true)
// |-- _2: integer (nullable = false)
// */

// dataFrame2.show

// // approach 3: toDF, apply on seq, not RDD
// import spark.implicits._
// val dataFrame3: DataFrame = seq.toDF
// dataFrame3.printSchema
// /** Schema显示
// root
// |-- _1: string (nullable = true)
// |-- _2: integer (nullable = false)
// */
// dataFrame3.show

// load from csv file
import org.apache.spark.sql.DataFrame
val csvFilePath: String = s"/Users/chloe/git/synergit/spark-0-to-1/sparl-sql/sample.csv"
val df: DataFrame = spark.read.format("csv").option("header", true).load(csvFilePath)
// df: org.apache.spark.sql.DataFrame = [name: string, age: string]
df.show
/** 结果打印
+-----+---+
| name| age|
+-----+---+
| alice| 18|
| bob| 14|
+-----+---+
*/

// load csv with schema
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
val schema:StructType = StructType( Array(
    StructField("name", StringType),
    StructField("age", IntegerType)
))
val csvFilePath: String = s"/Users/chloe/git/synergit/spark-0-to-1/sparl-sql/sample.csv"
val df1: DataFrame = spark.read.format("csv").schema(schema).option("header", true).option("mode", "dropMalformed").load(csvFilePath)
// df: org.apache.spark.sql.DataFrame = [name: string, age: int]
df1.show