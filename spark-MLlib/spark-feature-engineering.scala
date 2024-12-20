import org.apache.spark.sql.DataFrame
 
// 这里的下划线"_"是占位符，代表数据文件的根目录

val filePath: String = s"/Users/chloe/git/synergit/spark-0-to-1/spark-MLlib/train.csv"
 
val sourceDataDF: DataFrame = spark.read.format("csv").option("header", true).load(filePath)
sourceDataDF.printSchema

// 导入StringIndexer
import org.apache.spark.ml.feature.StringIndexer
 
// 所有非数值型字段，也即StringIndexer所需的“输入列”
val categoricalFields: Array[String] = Array("MSSubClass", "MSZoning", "Street", "Alley", "LotShape", "LandContour", "Utilities", "LotConfig", "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", "HouseStyle", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "RoofStyle", "RoofMatl", "Exterior1st", "Exterior2nd", "MasVnrType", "ExterQual", "ExterCond", "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2", "Heating", "HeatingQC", "CentralAir", "Electrical", "KitchenQual", "Functional", "FireplaceQu", "GarageType", "GarageYrBlt", "GarageFinish", "GarageQual", "GarageCond", "PavedDrive", "PoolQC", "Fence", "MiscFeature", "MiscVal", "MoSold", "YrSold", "SaleType", "SaleCondition")
 
// 非数值字段对应的目标索引字段，也即StringIndexer所需的“输出列”
val indexFields: Array[String] = categoricalFields.map(_ + "Index").toArray
 
// 将engineeringDF定义为var变量，后续所有的特征工程都作用在这个DataFrame之上
var engineeringDF: DataFrame = sourceDataDF
 
// 核心代码：循环遍历所有非数值字段，依次定义StringIndexer，完成字符串到数值索引的转换
for ((field, indexField) <- categoricalFields.zip(indexFields)) {
    // println(field, indexField)
    // 定义StringIndexer，指定输入列名、输出列名
    val indexer = new StringIndexer()
    .setInputCol(field)
    .setOutputCol(indexField)
    
    // 使用StringIndexer对原始数据做转换
    engineeringDF = indexer.fit(engineeringDF).transform(engineeringDF)
    
    // 删除掉原始的非数值字段列
    engineeringDF = engineeringDF.drop(field)
}
engineeringDF.printSchema

// engineeringDF.select("GarageTypeIndex").show(5)
 
/** 结果打印
+----------+---------------+
|GarageType|GarageTypeIndex|
+----------+---------------+
| Attchd| 0.0|
| Attchd| 0.0|
| Attchd| 0.0|
| Detchd| 1.0|
| Attchd| 0.0|
+----------+---------------+
only showing top 5 rows 
*/
// 所有数值型字段，共有27个
val numericFields: Array[String] = Array("LotFrontage", "LotArea", "MasVnrArea", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea")
 
// 预测标的字段
val labelFields: Array[String] = Array("SalePrice")
 
import org.apache.spark.sql.types.IntegerType
 
// 将所有数值型字段，转换为整型Int
for (field <- (numericFields ++ labelFields)) {
    engineeringDF = engineeringDF.withColumn(s"${field}Int",col(field).cast(IntegerType)).drop(field)
}
 
import org.apache.spark.ml.feature.VectorAssembler
 
// 所有类型为Int的数值型字段
val numericFeatures: Array[String] = numericFields.map(_ + "Int").toArray
 
// 定义并初始化VectorAssembler
val assembler = new VectorAssembler()
.setInputCols(numericFeatures)
.setOutputCol("features")
 
// 在DataFrame应用VectorAssembler，生成特征向量字段"features"
engineeringDF = assembler.transform(engineeringDF)
engineeringDF.printSchema()

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.feature.ChiSqSelectorModel
 
// 定义并初始化ChiSqSelector
val selector = new ChiSqSelector()
.setFeaturesCol("features")
.setLabelCol("SalePriceInt")
.setNumTopFeatures(20)

/* Following ChiSeqSelector and MinMaxScalar report error */
 
// 调用fit函数，在DataFrame之上完成卡方检验
// val chiSquareModel = selector.fit(engineeringDF)
 
// // 获取ChiSqSelector选取出来的入选特征集合（索引）
// val indexs: Array[Int] = chiSquareModel.selectedFeatures
 
// import scala.collection.mutable.ArrayBuffer
 
// val selectedFeatures: ArrayBuffer[String] = ArrayBuffer[String]()
 
// // 根据特征索引值，查找数据列的原始字段名
// for (index <- indexs) {
//     selectedFeatures += numericFields(index)
// }

// // 所有类型为Int的数值型字段
// // val numericFeatures: Array[String] = numericFields.map(_ + "Int").toArray
 
// // 遍历每一个数值型字段
// for (field <- numericFeatures) { 
//     // 定义并初始化VectorAssembler
//     val assembler = new VectorAssembler()
//     .setInputCols(Array(field))
//     .setOutputCol(s"${field}Vector")
    
//     // 调用transform把每个字段由Int转换为Vector类型
//     engineeringData = assembler.transform(engineeringData)
// }

// import org.apache.spark.ml.feature.MinMaxScaler
 
// // 锁定所有Vector数据列
// val vectorFields: Array[String] = numericFeatures.map(_ + "Vector").toArray
 
// // 归一化后的数据列
// val scaledFields: Array[String] = vectorFields.map(_ + "Scaled").toArray
 
// // 循环遍历所有Vector数据列
// for (vector <- vectorFields) {
//     // 定义并初始化MinMaxScaler
//     val minMaxScaler = new MinMaxScaler()
//     .setInputCol(vector)
//     .setOutputCol(s"${vector}Scaled")
//     // 使用MinMaxScaler，完成Vector数据列的归一化
//     engineeringData = minMaxScaler.fit(engineeringData).transform(engineeringData)
// }

// 原始字段
val fieldBedroom: String = "BedroomAbvGrInt"
// 包含离散化数据的目标字段
val fieldBedroomDiscrete: String = "BedroomDiscrete"
// 指定离散区间，分别是[负无穷, 2]、[3, 4]和[5, 正无穷]
val splits: Array[Double] = Array(Double.NegativeInfinity, 3, 5, Double.PositiveInfinity)
 
import org.apache.spark.ml.feature.Bucketizer
 
// 定义并初始化Bucketizer
val bucketizer = new Bucketizer()
// 指定原始列
.setInputCol(fieldBedroom)
// 指定目标列
.setOutputCol(fieldBedroomDiscrete)
// 指定离散区间
.setSplits(splits)
 
// 调用transform完成离散化转换
engineeringDF = bucketizer.transform(engineeringDF)

import org.apache.spark.ml.feature.OneHotEncoder
 
// 非数值字段对应的目标索引字段，也即StringIndexer所需的“输出列”
// val indexFields: Array[String] = categoricalFields.map(_ + "Index").toArray
 
// 热独编码的目标字段，也即OneHotEncoder所需的“输出列”
val oheFields: Array[String] = categoricalFields.map(_ + "OHE").toArray
 
// 循环遍历所有索引字段，对其进行热独编码
for ((indexField, oheField) <- indexFields.zip(oheFields)) {
    val oheEncoder = new OneHotEncoder()
    .setInputCol(indexField)
    .setOutputCol(oheField)
    engineeringDF = oheEncoder.transform(engineeringDF)
}

import org.apache.spark.ml.feature.VectorAssembler
 
/**
入选的数值特征：selectedFeatures - doesn't work on my local
归一化的数值特征：scaledFields - doesn't work on my local
离散化的数值特征：fieldBedroomDiscrete
热独编码的非数值特征：oheFields
*/
 
val assembler = new VectorAssembler()
.setInputCols(selectedFeatures ++ scaledFields ++ fieldBedroomDiscrete ++ oheFields)
.setOutputCol("features")
 
engineeringDF = assembler.transform(engineeringDF)