// Courtesy to https://spark.apache.org/docs/1.5.2/ml-ann.html
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Multilayer_Perception_NN {
  val conf = new SparkConf().setAppName("Neural Network NBA").setMaster("")
  val sc = new SparkContext(conf)
  def main(args: Array[String]){
    // Load training data
    //val data = MLUtils.loadLibSVMFile(sc, "NN_Each_Game_Distribution_4_15_ready.csv").toDF()
    val data_header = sc.textFile("NN_Each_Game_Distribution_4_15.csv")
    val header = data_header.first
    val fields = header.split(',').map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val data = sc.textFile("NN_Each_Game_Distribution_4_15_ready.csv")
    
    val rowRDD = data.map(_.split(','))
                      .map(attributes => Row(attributes))

    val dataDF = spark.createDataFrame(rowRDD, schema)

    // Split the data into train and test
    val splits = dataDF.randomSplit(Array(0.6, 0.4), seed = 1234L)
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
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
         .setMetricName("precision")
    println("Precision:" + evaluator.evaluate(predictionAndLabels))
  }
}
