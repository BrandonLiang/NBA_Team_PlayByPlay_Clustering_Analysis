// Courtesy to https://spark.apache.org/docs/1.5.2/ml-ann.html
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
// import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

object Multilayer_Perception_NN {
  def toDouble(a: Array[String]): Array[Double] = {
    a.foldLeft(Array[Double]()){
      (accu, curr) => {
        if ( curr == "W") {
          accu :+ 1.0
        } else if (curr == "L") {
          accu :+ 0.0
        } else {
          accu :+ curr.toDouble
        }
    }}}

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Neural Network NBA").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession.builder()
        .appName("SparkSessionZipsExample")
        //.config("spark.driver.memory","4G")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
    // Load training data
    //val data = MLUtils.loadLibSVMFile(sc, "NN_Each_Game_Distribution_4_15_ready.csv").toDF()
    val data_header = sc.textFile("NN_Each_Game_Distribution_4_15.csv")
    val header = "features,label"
    val fields = header.split(',').map({
        case "features" => StructField("features", VectorType, nullable = true)
        case "label" => StructField("label", DoubleType, nullable = true)
        })
    val schema = StructType(fields)
    // val schema = StructType(StructField("features", DoubleType), StructField("label", DoubleType))
    val data = sc.textFile("NN_Each_Game_Distribution_4_15_ready.csv")
    
    // val rowRDD = data.map(_.split(','))
    //                   .map(toDouble)
    //                   .map(Vectors.dense(_.slice(0,56)))
    //                   .map(attributes => Row.fromSeq(attributes))

    // val rowRDD = data.map(_.split(',')).map(toDouble).map(x => Row.fromSeq(Array(Vectors.dense(x.slice(0,56)), Vectors.dense(x.slice(56,57)))))

    val rowRDD = data.map(_.split(',')).map(toDouble).map(x => Row.fromSeq(Array(Vectors.dense(x.slice(0,56)), x(56))))

    // val dataDF = rowRDD.toDF("features", "label")
    // val dataDF = spark.createDataFrame(rowRDD)
    val dataDF = spark.createDataFrame(rowRDD, schema)
    // dataDF.show()
    // val dataDF_ = dataDF.select(dataDF.columns.map{
    //     case label @ "W/L" => dataDF.withColumn("label", encodeLabel(dataDF(label)))
    //     case all => dataDF.withColumn("features", toVec4( dataDF(all)))
    //     }).select("features", "label")

    // Need to turn features into a vector/matrix under "features" and then "label"
    //val dataDF_ = dataDF.toDF("features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "features", "label")

    // val dataDF = dataDF_.select(dataDF_.columns.map{
    //     case all => dataDF_(all).cast(DoubleType).as(all)
    //   }: _*)

    // dataDF.show()
    // Split the data into train and test
    val splits = dataDF.randomSplit(Array(0.6, 0.4)) //, seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    // specify layers for the neural network: 
    // input layer of size 4 (features), two intermediate of size 5 and 4 and output of size 3 (classes)
    val layers = Array[Int](4, 5, 4, 3)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(100)
    // train the model
    val model = trainer.fit(train)
    // compute precision on the test set
    val result = model.transform(test)

    result.show()
    /*
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
         .setMetricName("accuracy")
    */
    
    /*
    println("Accuracy:" + evaluator.evaluate(predictionAndLabels))
    */
  }
}
