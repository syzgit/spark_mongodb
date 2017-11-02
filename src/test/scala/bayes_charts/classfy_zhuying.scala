package bayes_charts
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.lang.String
import scala.collection.mutable
//import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.Statement
import org.apache.log4j.{Level, Logger}
import java.io.PrintWriter
import java.io.File
object classfy_zhuying {
   def main(args:Array[String]){
      Logger.getLogger("org").setLevel(Level.ERROR)
     val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
    //取要分类的数据 mongo_hb_tables_jiexi_data
   var RDD_bson = sc
   .textFile("/user/yzsun/10-month/juchao_jiexi_data/_alldata_/*")
  //.textFile("/user/yzsun/result_juchao_alldata/")
   //.textFile("/user/yzsun/mongo_hb_tables_jiexi_data/")
  //.textFile("C:/Users/yzsun.abcft/Desktop/juchao_4_test.txt")
  //.map(row=> row.replace(",", "").split(" "))
  .map(row=> row.substring(4, row.length()-1).replace("-", "").replace(">", "").split(" "))
   var RDD_zhuying = sc
  .textFile("/user/yzsun/bayes_train_result/999_8.txt").map(x => x.split(" "))
  
  
  
  
  
  
  
   }
  
}