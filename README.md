# spark-0-to-1
This is a repo for Spark related topics

**Pre-req**
Install Spark on your local: https://spark.apache.org/downloads.html
1. download
2. unzip
3. configure system $PATH
```shell
export SPARK_HOME=${unziped file path}
export PATH=$PATH:$SPARK_HOME/bin
```
If you don't have Java install, download it for your env. In case of mac, jre-8u431-macosx-aarch64.dmg from https://www.java.com/en/download/, and follow instructions on https://www.java.com/en/download/help/mac_install.html.

## Four modules to learn Spark
### 1. Core
**Example: word count**
<br>

```shell
spark-shell
```
Output
```shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/11/18 16:01:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://chloes-air:4040
Spark context available as 'sc' (master = local[*], app id = local-1731963681925).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/
         
Using Scala version 2.12.18 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_431)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```
then 
```
scala> :load /Users/chloe/git/synergit/spark-0-to-1/word-count.scala
```
Output
```shell
Loading /Users/chloe/git/synergit/spark-0-to-1/temp.scala...
import org.apache.spark.rdd.RDD
rootPath: String = /Users/chloe/git/synergit/spark-0-to-1
file: String = /Users/chloe/git/synergit/spark-0-to-1/wikiOfSpark.txt
lineRDD: org.apache.spark.rdd.RDD[String] = /Users/chloe/git/synergit/spark-0-to-1/wikiOfSpark.txt MapPartitionsRDD[11] at textFile at /Users/chloe/git/synergit/spark-0-to-1/temp.scala:26
wordRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[12] at flatMap at /Users/chloe/git/synergit/spark-0-to-1/temp.scala:26
cleanWordRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[13] at filter at /Users/chloe/git/synergit/spark-0-to-1/temp.scala:26
kvRDD: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[14] at map at /Users/chloe/git/synergit/spark-0-to-1/temp.scala:26
wordCounts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[15] at reduceByKey at /Users/chloe/git/synergit/spark-0-to-1/temp.scala:26
res3: Array[(Int, String)] = Array((67,the), (63,Spark), (54,a), (51,and), (50,of))
```

**Key concepts**
<br>
* RDD: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds
* RDD Operations: https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations
* Lazy Evaluation: https://medium.com/@john_tringham/spark-concepts-simplified-lazy-evaluation-d398891e0568
* DAGScheduler
* TaskScheduler
* SchedulerBackeng
* ExecutorBackend
* Stage
* Shuffle
* WorkOffer
* Processor_level(executor), Node_level, RACK_level, Any
* Executor-Memory: https://kb.databricks.com/clusters/spark-executor-memory
* RDD persistent caching: https://data-flair.training/blogs/apache-spark-rdd-persistence-caching/


**Common Transformations**
<br>
**Transformation without Shuffle**
<br>
* map(func)
* mapPartitions(func)
* mapPartitionsWithIndex(func)
* flatMap(func): check [example](./wordcount-flatmap.scala)
* filter(func)

**Action**
<br>
* show(), take() and collect():[.collect() vs .show() vs .take() in Apache Spark](https://medium.com/@vishalbarvaliya/collect-vs-show-vs-take-in-apache-spark-683531e149a1)

**Transformation with Shuffle**
<br>
* groupByKey
* reduceByKey
* aggregateByKey
* sortByKey

## 2. Spark SQL
## 3. Spark MLlib
## 4. structured streaming

# Utility

Run scala file in Spark-Shell
```
scala> :load {file_name}
```

Reference
* https://github.com/wulei-bj-cn/learn-spark
