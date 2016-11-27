import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Model;
import org.apache.spark.sql.*;

import scala.Tuple2;
public class Process_Team {
	public static void main(String[] args) throws IOException{
		// Create bash script to run in terminal for each file in the directory
		
		// Path subject to change
		//System.out.println(args[0]);
		String filename = args[0];
		String team = filename.split("/")[filename.split("/").length-1].split("-")[0].split("_")[1];
		String season = args[1];
		//String season = filename.split("/")[filename.split("/").length-2].split("_")[0];
		// String filename = "/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/15/Warriors-All Plays.csv";
		SparkConf configuration = new SparkConf().setAppName("PageRank").setMaster("local");
		//System.out.println(team);
		JavaSparkContext sparkContext = new JavaSparkContext(configuration);
		
		// Load the file
		JavaRDD<String> file = sparkContext.textFile(season+"_Team_Summary/"+filename);
		
		String header = file.first();
		JavaRDD<String> new_file = file.filter(s -> !s.equals(header));
		JavaPairRDD<Integer,Integer> before_after =  new_file.mapToPair(s-> {
			String[] l = s.split(",");
			Integer before = Integer.valueOf(l[8]);
			Integer after =  Integer.valueOf(l[11]);
			Tuple2<Integer,Integer> tuple = new Tuple2<Integer,Integer>(before,after);
			return tuple;
		});
		
		//before_after.collect().forEach(s->System.out.println(s));
		
		JavaPairRDD<Tuple2<Integer,Integer>,Integer> count = before_after.mapToPair(s->new Tuple2<Tuple2<Integer,Integer>,Integer>(s,1));
		JavaPairRDD<Integer,Tuple2<Integer,Integer>> count_reduced = count.reduceByKey((x,y)->x+y).mapToPair(s->{
			Tuple2<Integer,Tuple2<Integer,Integer>> result = new Tuple2<Integer, Tuple2<Integer,Integer>>(s._1._1,new Tuple2<Integer,Integer>(s._1._2,s._2));
			return result;
		});
		
		//JavaRDD<String> cc = count_reduced.flatMap(s->)
		
		//count_reduced.collect().forEach(s->System.out.println(s));
		
		// Requires a separate parser to change the file name and split lines into wanted format for further processing
		
		File dir = new File("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"+season+"_Spark_Team_Summary/"+team+"-Margin Change Summary");
		if (dir.exists()){
			FileUtils.deleteDirectory(dir);
		}
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
		Dataset<Row> c = sqlContext.createDataFrame(count_reduced.values(),Model.class);
		JavaRDD<String> result = count_reduced.sortByKey().map(s -> {
			Integer before = s._1;
			Integer after = s._2._1;
			Integer coun = s._2._2;
			String st = String.valueOf(before)+"|"+String.valueOf(after) + "|"+String.valueOf(coun);
			return st;
		});
				
		result.coalesce(1,true).saveAsTextFile("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"+season+"_Spark_Team_Summary/"+team+"-Margin Change Summary");
		sparkContext.stop();
	}
}
