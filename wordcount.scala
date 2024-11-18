import org.apache.spark.rdd.RDD
 
// find current path: System.getProperty("user.dir")
val rootPath: String = "/Users/chloe/git/synergit/spark-0-to-1"
val file: String = s"${rootPath}/wikiOfSpark.txt"
 
// 读取文件内容
val lineRDD: RDD[String] = spark.sparkContext.textFile(file) 

// 以行为单位做分词
val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" ")) 

// 过滤掉空字符串
val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

// 把RDD元素转换为（Key，Value）的形式
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1)) 

// 按照单词做分组计数
val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y) 

// 打印词频最高的5个词汇
wordCounts.map{case (k, v) => (v, k)}.sortByKey(false).take(5)