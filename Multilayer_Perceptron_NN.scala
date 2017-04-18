// Courtesy to https://spark.apache.org/docs/1.5.2/ml-ann.html
import org.apache.spark.sql.DataFrame
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

object Multilayer_Perceptron_NN {
    def load_data(spark: SparkSession, sc: SparkContext, filepath: String): DataFrame = {
      val data = sc.textFile( args(0) )
      val fields = Array( StructField("gameId", StringType), StructField("features", VectorType), StructField("label", DoubleType))
      val schema = StructType(fields)
      val rowRDD = data.map(_.split(',')).map(x => x(57) match {
        case "W" => Row.fromSeq(Array(x(0).toString, Vectors.dense(x.slice(1,57).map(_.toDouble)), 1.0))
        case "L" => Row.fromSeq(Array(x(0).toString, Vectors.dense(x.slice(1,57).map(_.toDouble)), 0.0))
      })
      spark.createDataFrame(rowRDD, schema)
    }

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
      val dataDF = load_data(spark, sc, args(0))
      val splits = dataDF.randomSplit(Array(0.8, 0.2))
      val train = splits(0)
      val test = splits(1)

      val layers = Array[Int](56, 28, 28, 1)

      val trainer = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(100)
      
      
      val model = trainer.fit(train)
      /*
      val result = model.transform(test)

      val evaluator = new MulticlassClassificationEvaluator()
        .setMetricName("accuracy")
      */
      //println("Accuracy: " + evaluator.evaluate(result.select("prediction", "label")))

      spark.stop()
      sc.stop()
    }
}
