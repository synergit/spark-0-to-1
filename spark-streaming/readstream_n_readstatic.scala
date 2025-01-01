import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
 
// 保存staging、interactions、userProfile等文件夹的根目录
val rootPath: String = "/Users/chloe/git/synergit/spark-0-to-1/spark-streaming/data"
 
// 使用read API读取离线数据，创建DataFrame
val staticDF: DataFrame = spark.read.format("csv").option("header", true).load(s"${rootPath}/userProfile/userProfile.csv")
// 定义用户反馈文件的Schema
val actionSchema = new StructType().add("userId", "integer").add("videoId", "integer").add("event", "string").add("eventTime", "timestamp")
 
// 使用readStream API加载数据流，注意对比readStream API与read API的区别与联系
// 指定文件格式
// 指定监听目录
// 指定数据Schema
var streamingDF: DataFrame = spark.readStream.format("csv").option("header", true).option("path", s"${rootPath}/interactions").schema(actionSchema).load

/*
In order to see the streaming process, copy interaction*.csv from userProfile folder to interactions folder. 
*/
// 互动数据分组、聚合，对应流程图中的步骤4
// 创建Watermark，设置最大容忍度为30分钟
// 按照时间窗口、userId与互动类型event做分组
// 记录不同时间窗口，用户不同类型互动的计数
streamingDF = streamingDF.withWatermark("eventTime", "30 minutes").groupBy(window(col("eventTime"), "1 hours"), col("userId"), col("event")).count
 
/**
流批关联，对应流程图中的步骤5
可以看到，与普通的两个DataFrame之间的关联，看上去没有任何差别
*/
val jointDF: DataFrame = streamingDF.join(staticDF, streamingDF("userId") === staticDF("id"))

jointDF.writeStream.format("console").option("truncate", false).outputMode("update").start().awaitTermination()