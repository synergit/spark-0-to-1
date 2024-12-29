import org.apache.spark.sql.DataFrame

// start netcat server: ```nc -lk 9999```
 
// 设置需要监听的本机地址与端口号
val host: String = "127.0.0.1"
val port: String = "9999"


// 从监听地址创建DataFrame
var df: DataFrame = spark.readStream.format("socket").option("host", host).option("port", port).load()

/**
使用DataFrame API完成Word Count计算
*/
 
// 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
// 把数组words展平为单词word
// 以单词word为Key做分组
// 分组计数
df = df.withColumn("words", split($"value", " ")).withColumn("word", explode($"words")).groupBy("word").count()
 

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

