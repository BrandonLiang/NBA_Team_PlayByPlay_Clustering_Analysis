/*
 *  Tresata Academy - Backend Spark Exercises
 *
 *  Note: In order to complete these exercises you must be able to do the following -
 *    RDDs
 *    - Take and manipulate elements of an RDD
 *      - Data Cleansing
 *      - String Manipulation
 *
 *    DataFrames
 *    - Print the schema of a DataFrame
 *    - Show n number of records from a DataFrame
 *    - Create and apply a User Defined Function (UDF)
 *    - Perform aggregations and sorts
 *
 * Disclaimer: It's probably a good idea to check what your data looks like in each step as you go through the exercises.
 */

// Necessary imports.
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val conf = new SparkConf().setAppName("spark-exercise").setMaster("local")
val sc = new SparkContext(conf)


/*  Exercise 1:
    a) Read in the sample-txns file from your home on HDFS as a text file.
    b) Clean the data. Be sure to handle null values appropriately (You'll probably want to get rid of quotes as well - be careful though).
    c) Parse the data into an RDD[Array[String]].
*/

  /* a) Once the file is on HDFS read it in as a text file. */
  val rawData = sc.textFile("hdfs://dev/user/brandon/backend/spark/sample-txns.csv")

  /* b) Clean the data. */
  val data = rawData.map( s => s.replace("\"\"", "\" \"") ) // Takes care of the ending nulls
                    .map( s => s.replace("\",\"","|") ).map( s => s.substring(1, s.length-1) ) // transforming the delimiter to "|"
 

  /* c) Parse the data. */
  val head = data.first
  var parsedData = data.map(x => x.split('|')) // Use Single Quotation Mark instead of Double Quotation Mark!
    
==========================================================================================================================

/*  Exercise 2: Create a DataFrame using the parsed RDD from Exercise 1. [You can make them all Strings for convenience]
    a) Define the schema.
    b) Convert the RDD[Array[String]] into an RDD[Row], then drop out the header row.
    c) Create a dataframe using the RDD[Row] and schema. Cache it and then print the schema.
*/

  /* a) Define the schema. (Hint: Your schema should look like the first record in the RDD). */
  val header = head.split('|')
  val fields = header.map( fieldName => StructField(fieldName, StringType, nullable = true) )
  val schema = StructType(fields)

  /* b) Convert the parsed records into Rows and then drop the first line. */
  val rowRDD = parsedData.map( r => Row.fromSeq(r) ).zipWithIndex.filter(_._2 > 0).map(_._1)

  /* c) Create a dataframe using the schema and rows. */
  val df = spark.createDataFrame(rowRDD, schema).cache

==========================================================================================================================

/*  Exercise 3: [Note: Use the dataframe you created in Exercise 2c]
    a) Rename price to grossPrice and confirm it's been renamed by printing the schema.
    b) Convert all numerical fields to doubles (grossPrice, quantity, discounts).
    c) Create a udf to calculate Net Sales for each record (Price - Discount).
*/

  /* -) Optional - Define numeric/nonNumeric fields. */
  lazy val nonNumerics = Seq("hhid","datatime","receipt","upc","store","subcat","zip").map(col(_))
  lazy val numerics = Seq("grossPrice","discount","quantity").map(col(_))

  /* a) Rename price to grossPrice. */
  val renamed = df.withColumnRenamed("price", "grossPrice")

  /* b) Convert numeric fields to Doubles. */
  //val converted = renamed.select(nonNumerics += numerics.map(_.cast(DoubleType)): _*)
  val converted = renamed.select(
    renamed.columns.map {
      case grossPrice @ "grossPrice" => renamed(grossPrice).cast(DoubleType).as(grossPrice)
      case discount @ "discount" => renamed(discount).cast(DoubleType).as(discount)
      case quantity @ "quantity" => renamed(quantity).cast(DoubleType).as(quantity)
      case other => renamed(other)
    }: _*
  )

  /* c) Create a udf to calculate netPrice and then apply it. */
  val netPrice = udf { (grossPrice: Double, discount: Double) => grossPrice - discount }
  val withNetPrice = converted.withColumn("netPrice", netPrice($"grossPrice", $"discount"))

==========================================================================================================================

/*  Exercise 4: [Note: Use the dataframe you created in Exercise 3c]
    a) Find the most expensive item based off of Net Price. Do the same for least expensive (ignoring anything less than 0).
    db) Use your answers from (a) to find the respective hhids by filtering.
*/

  /* a) Find minima/maxima. */
  val maxNetPrice = withNetPrice.agg(max("netPrice").as("maxPrice"))
  val minNetPrice = withNetPrice.filter("netPrice > 0.0").agg(min("netPrice").as("minPrice"))
  
  /* b) Find records associated to those values. */
  val maxNetPriceRow = withNetPrice.reduce((a,b) => if (a.getAs[Double]("netPrice") > b.getAs[Double]("netPrice")) a else b)
  
  val minNetPriceRow = withNetPrice.reduce((a,b) => if (a.getAs[Double]("netPrice") < b.getAs[Double]("netPrice") && a.getAs[Double]("netPrice") >= 0) a else b)
==========================================================================================================================

/*  Exercise 5: [Note: Use the dataframe you created in Exercise 3c]
    a) Calculate average spend per item by household based of netPrice (hhid).
    b) Find the max.
    c) Calculate overallAvgSpend for the dataset. Make sure to take quantity into account.
*/

  /* a) Define a unit price udf and then apply it. */
  val unitPrice = udf { (totalPrice: Double, totalQuantity: Integer) => if (totalQuantity == 0) totalPrice else totalPrice / totalQuantity }
  val withUnitPrice = withNetPrice.withColumn("unitPrice", unitPrice($"netPrice", $"quantity"))

  /* b) Calculate avgSpend per hhid using unitPrice. Then find the max. */
  val withAvgSpend = withUnitPrice.groupBy("hhid").agg(avg("unitPrice").as("avgSpend"))
  val maxSpend = withAvgSpend.agg(max("avgSpend"))

  /* c) Calculate overall avgSpend for dataset using avgSpend from (b). */
  val oervallAvgSpend = withAvgSpend.agg(avg("avgSpend").as("overallAvgSpend"))

==========================================================================================================================

/*  Exercise 6: [Note: Use the dataframe you created in Exercise 2c]
    a) Count the number of records for hhids, upcs, and stores.
    b) Find the max by sorting on the count for each of the following: hhid, ean, store.
    c) Find the distinct count for each field.
*/

  /* a) Counts. */
  val hhid_count = df.groupBy("hhid").count()
  val upc_count = df.groupBy("upcs").count()
  val store_count = df.groupBy("store").count()

  /* b) Maximums. */
  val hhid_max = hhid_count.sort(desc("count")).head
  val upc_max = upc_count.sort(desc("count")).head
  val store_max = store_count.sort(desc("count")).head

  /* c) Distinct Counts. */
  val distinct_hhid = df.agg(countDistinct("hhid"))
  val distinct_upc = df.agg(countDistinct("upc"))
  val distinct_store = df.agg(countDistinct("store"))

