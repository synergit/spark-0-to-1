import org.apache.spark.sql.DataFrame

// start netcat server: ```nc -lk 9999```
 
// 设置需要监听的本机地址与端口号
val host: String = "127.0.0.1"
val port: String = "9999"

// 从监听地址创建DataFrame
var df: DataFrame = spark.readStream.format("socket").option("host", host).option("port", port).load()

/*
WITH Tumbling Window - .groupBy(window(col("eventTime"), "5 minute"), col("word"))
if WITH Sliding Window - .groupBy(window(col("eventTime"), "5 minute", "1 minute"), col("word"))
the input data needs to have a timestamp, in format of 'yyyy-MM-dd HH:mm:ss' 
i.e. 
```
2025-01-01 10:30:56,Apache Spark
2025-01-01 10:34:56,Spark Logo
2025-01-01 10:36:56,Structured Streaming
2025-01-01 10:39:56,Spark Streaming
2025-01-01 10:41:56,Git Lab
2025-01-01 10:44:56,Spark SQL
2025-01-01 10:29:56,Test Test // with watermark 10min and tumbling window 5min, this line will be dropped
2025-01-01 10:33:56,Spark is cool
```
*/
// 提取事件时间 
// 提取单词序列
// 拆分单词
// 按照Tumbling Window与单词做分组
// 统计计数
df = df.withColumn("inputs", split($"value", ",")).withColumn("eventTime", element_at(col("inputs"),1).cast("timestamp")).withColumn("words", split(element_at(col("inputs"),2), " ")).withColumn("word", explode($"words")).withWatermark("eventTime", "10 minute").groupBy(window(col("eventTime"), "5 minute"), col("word")).count()

/**
将Word Count结果写入到终端（Console）
*/
// 指定Sink为终端（Console）
// 指定输出选项
// 指定输出模式
// 启动流处理应用
// 等待中断指令 

//.outputMode("update")
// Complete mode：输出到目前为止处理过的全部内容
// Append mode：仅输出最近一次作业的计算结果
// Update mode：仅输出内容有更新的计算结果
df.writeStream.format("console").option("truncate", false).outputMode("update").start().awaitTermination()

