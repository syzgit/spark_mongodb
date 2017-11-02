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
import org.apache.hadoop.conf.Configuration
import org.bson._
import org.bson.{BSONObject, BasicBSONObject}
import com.mongodb.hadoop.{
  MongoInputFormat, MongoOutputFormat,
  BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
import java.io.PrintWriter
import java.io.File
import com.hankcs.hanlp.HanLP
import java.io.Writer
import org.bson.Document
import com.mongodb.spark._
import com.mongodb.spark.MongoSpark

object classfy {
  
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("YZSUN_bson").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
      val config = new Configuration()
    

   /* config.set("mongo.input.uri", "mongodb://bigdata:defu33%40rEpam@121.40.131.65:3718/cr_data.hb_tables")
    config.set("mongo.input.noTimeout", "true")
    config.set("mongo.auth.uri", "mongodb://bigdata:defu33%40rEpam@121.40.131.65:3718/cr_data")
   config.set("mongo.input.query", "{$and:[{'fileId':1796786}]}")

  config.set("mongo.input.fields", "{'data':1, 'rowdata':1 ,'title':1}")
 config.set("mongo.output.uri", "mongodb://bigdata:defu33%40rEpam@121.40.131.65:3718/cr_data.hb_tables")
 

      
 
     val documentRDD = sc.newAPIHadoopRDD(
      config,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject])
val localfile = documentRDD.collect().mkString

     documentRDD.saveAsTextFile("/user/yzsun/bson1796786")*/
    
     
     
            // Create a separate Configuration for saving data back to MongoDB.
  /*val outputConfig = new Configuration()
  outputConfig.set("mongo.output.uri", " mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")*/

  // Save this RDD as a Hadoop "file". The path argument is unused; all documents will go to "mongo.output.uri".
   val RDD = sc.textFile("/user/yzsun/888_5").map { x =>
      {
        val splits = x.substring(1, x.length()-1).split(",")
        (splits(0).to, (splits(1)))
      }
    }
    //val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))

   MongoSpark.save(RDD) 
  /*documentRDD.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig)*/
     
   sc.stop()
    
  }
   
   
}