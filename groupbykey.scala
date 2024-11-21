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

// 把普通RDD映射为Paired RDD
val kvRDD: RDD[(String, String)] = cleanWordRDD.map(word => (word, word))
 
// 按照单词做分组收集
val words: RDD[(String, Iterable[String])] = kvRDD.groupByKey()

// 打印词频最高的5个词汇
words.map{case (k, v) => (v, k)}.sortByKey(false).take(5)