import org.apache.spark.sql.DataFrame
 
// rootPath为房价预测数据集根目录
val rootPath: String = "/Users/chloe/git/synergit/spark-0-to-1/spark-MLlib"
val filePath: String = s"${rootPath}/train.csv"
 
// 读取文件，创建DataFrame
var engineeringDF: DataFrame = spark.read.format("csv").option("header", true).load(filePath)
 
// 所有数值型字段
val numericFields: Array[String] = Array("LotFrontage", "LotArea", "MasVnrArea", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea")
 
// Label字段
val labelFields: Array[String] = Array("SalePrice")
 
import org.apache.spark.sql.types.IntegerType
 
for (field <- (numericFields ++ labelFields)) {
    engineeringDF = engineeringDF.withColumn(s"${field}Int",col(field).cast(IntegerType)).drop(field)
}

// transformer 1
import org.apache.spark.ml.feature.StringIndexer
 
// 所有非数值型字段
val categoricalFields: Array[String] = Array("MSSubClass", "MSZoning", "Street", "Alley", "LotShape", "LandContour", "Utilities", "LotConfig", "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", "HouseStyle", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "RoofStyle", "RoofMatl", "Exterior1st", "Exterior2nd", "MasVnrType", "ExterQual", "ExterCond", "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2", "Heating", "HeatingQC", "CentralAir", "Electrical", "KitchenQual", "Functional", "FireplaceQu", "GarageType", "GarageYrBlt", "GarageFinish", "GarageQual", "GarageCond", "PavedDrive", "PoolQC", "Fence", "MiscFeature", "MiscVal", "MoSold", "YrSold", "SaleType", "SaleCondition")
 
// StringIndexer期望的输出列名
val indexFields: Array[String] = categoricalFields.map(_ + "Index").toArray
 
// 定义StringIndexer实例
val stringIndexer = new StringIndexer().setInputCols(categoricalFields).setOutputCols(indexFields).setHandleInvalid("skip") 

// transformer 2
import org.apache.spark.ml.feature.VectorAssembler
 
// 转换为整型的数值型字段
val numericFeatures: Array[String] = numericFields.map(_ + "Int").toArray
 

/** 输入列为：数值型字段 + 非数值型字段
注意，非数值型字段的列名，要用indexFields，
而不能用原始的categoricalFields，不妨想一想为什么？
*/
val vectorAssembler = new VectorAssembler().setInputCols(numericFeatures ++ indexFields).setOutputCol("features").setHandleInvalid("skip")

// transformer 3
import org.apache.spark.ml.feature.VectorIndexer
val vectorIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(30).setHandleInvalid("skip")

//estimator
import org.apache.spark.ml.regression.GBTRegressor
val gbtRegressor = new GBTRegressor().setLabelCol("SalePriceInt").setFeaturesCol("indexedFeatures").setMaxIter(30).setMaxDepth(5)

// pipeline
import org.apache.spark.ml.Pipeline
val components = Array(stringIndexer, vectorAssembler, vectorIndexer, gbtRegressor)
val pipeline = new Pipeline().setStages(components)

// Pipeline保存地址的根目录
val savePath: String = "/Users/chloe/git/synergit/spark-0-to-1/spark-MLlib/pipeline-models"
 
// 将Pipeline物化到磁盘，以备后用（复用）
pipeline.write.overwrite().save(s"${savePath}/unfit-gbdt-pipeline")

// 划分出训练集和验证集
val Array(trainingData, validationData) = engineeringDF.randomSplit(Array(0.7, 0.3))
 
// 调用fit方法，触发Pipeline计算，并最终拟合出模型
val pipelineModel = pipeline.fit(trainingData)

// val trainingSummary = pipelineModel.summary
// println(s"Root Mean Squared Error (RMSE) on train data: ${trainingSummary.rootMeanSquaredError}")
// RMSE: 38288.77947156114

// import org.apache.spark.ml.evaluation.RegressionEvaluator

// val predictions: DataFrame = pipelineModel.transform(validationData).select("SalePriceInt", "prediction")
// val evaluator = new RegressionEvaluator().setLabelCol("SalePriceInt").setPredictionCol("prediction").setMetricName("rmse")
// val rmse = evaluator.evaluate(predictions)