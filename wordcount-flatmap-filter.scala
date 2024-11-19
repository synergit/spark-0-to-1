import org.apache.spark.rdd.RDD
 
// find current path: System.getProperty("user.dir")
val rootPath: String = "/Users/chloe/git/synergit/spark-0-to-1"
val file: String = s"${rootPath}/wikiOfSpark.txt"
 
// 读取文件内容
val lineRDD: RDD[String] = spark.sparkContext.textFile(file) 

// 以行为单位做分词
val wordPairRDD: RDD[String] = lineRDD.flatMap(line => {
// line.split(" ")
    // 将行转换为单词数组 
    val words: Array[String] = line.split(" ")
    // 将单个单词数组，转换为相邻单词数组 
    for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i+1)
    }
) 

// 过滤掉空字符串
// val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

// 定义特殊字符列表
val list: List[String] = List("&", "|", "#", "^", "@")
 
// 定义判定函数f
def f(s: String): Boolean = {
    val words: Array[String] = s.split("-")
    val b1: Boolean = list.contains(words(0))
    val b2: Boolean = list.contains(words(1))
    return !b1 && !b2 // 返回不在特殊字符列表中的词汇对
}
 
// 使用filter(f)对RDD进行过滤
val cleanedPairRDD: RDD[String] = wordPairRDD.filter(f)

// 把RDD元素转换为（Key，Value）的形式
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1)) 

// 按照单词做分组计数
val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y) 

// 打印词频最高的5个词汇
wordCounts.map{case (k, v) => (v, k)}.sortByKey(false).take(5)