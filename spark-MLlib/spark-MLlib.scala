import org.apache.spark.sql.DataFrame
 
val filePath: String = s"/Users/chloe/git/synergit/spark-0-to-1/spark-MLlib/train.csv"
 
// 从CSV文件创建DataFrame
val trainDF: DataFrame = spark.read.format("csv").option("header", true).load(filePath)
 
trainDF.show
trainDF.printSchema


import org.apache.spark.sql.types.IntegerType
 
// 提取用于训练的特征字段与预测标的（房价SalePrice）
val selectedFields: DataFrame = trainDF.select("LotArea", "GrLivArea", "TotalBsmtSF", "GarageArea", "SalePrice")
 
// 将所有字段都转换为整型Int
val typedFields = selectedFields.withColumn("LotAreaInt",col("LotArea").cast(IntegerType)).drop("LotArea").withColumn("GrLivAreaInt",col("GrLivArea").cast(IntegerType)).drop("GrLivArea").withColumn("TotalBsmtSFInt",col("TotalBsmtSF").cast(IntegerType)).drop("TotalBsmtSF").withColumn("GarageAreaInt",col("GarageArea").cast(IntegerType)).drop("GarageArea").withColumn("SalePriceInt",col("SalePrice").cast(IntegerType)).drop("SalePrice")
 
typedFields.printSchema
 
/** 结果打印
root
 |-- LotAreaInt: integer (nullable = true)
 |-- GrLivAreaInt: integer (nullable = true)
 |-- TotalBsmtSFInt: integer (nullable = true)
 |-- GarageAreaInt: integer (nullable = true)
 |-- SalePriceInt: integer (nullable = true)
*/

import org.apache.spark.ml.feature.VectorAssembler
 
// 待捏合的特征字段集合
val features: Array[String] = Array("LotAreaInt", "GrLivAreaInt", "TotalBsmtSFInt", "GarageAreaInt")
 
// 准备“捏合器”，指定输入特征字段集合，与捏合后的特征向量字段名
val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
 
// 调用捏合器的transform函数，完成特征向量的捏合
val featuresAdded: DataFrame = assembler.transform(typedFields).drop("LotAreaInt").drop("GrLivAreaInt").drop("TotalBsmtSFInt").drop("GarageAreaInt")
 
featuresAdded.printSchema
 
/** 结果打印
root
 |-- SalePriceInt: integer (nullable = true)
 |-- features: vector (nullable = true) // 注意，features的字段类型是Vector
*/

val Array(trainSet, testSet) = featuresAdded.randomSplit(Array(0.7, 0.3))


import org.apache.spark.ml.regression.LinearRegression
 
// 构建线性回归模型，指定特征向量、预测标的与迭代次数
val lr = new LinearRegression().setLabelCol("SalePriceInt").setFeaturesCol("features").setMaxIter(10)
 
// 使用训练集trainSet训练线性回归模型
val lrModel = lr.fit(trainSet)

val trainingSummary = lrModel.summary
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
 
/** 结果打印
RMSE: 45798.86
*/