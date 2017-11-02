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
import org.bson.{BSONObject, BasicBSONObject}
import com.mongodb.hadoop.{
  MongoInputFormat, MongoOutputFormat,
  BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
import java.io.PrintWriter
import java.io.File
import com.hankcs.hanlp.HanLP
import java.io.Writer

import com.hankcs.hanlp.HanLP
import java.io.Writer


import com.hankcs.hanlp.seg.common.Term;  
import com.hankcs.hanlp.seg.Segment
import javafx.animation.PathTransition.Segment
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.hankcs.hanlp.dictionary.CustomDictionary
import scala.util.parsing.json.JSON
object pipei_id {
  
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
     val RDD_id = sc.textFile("/user/yzsun/10-month/finance.juchao.item_data/finance.juchao.item_data_5/")
     val RDD_jiexi_data = sc.textFile("/user/yzsun/11-month/juchao_fenlei_result/all/")
   /*  val RDD_id = sc.textFile("C:/Users/yzsun.abcft/Desktop/juchao_4_test.txt")
     val RDD_jiexi_data = sc.textFile("C:/Users/yzsun.abcft/Desktop/result_test.txt")*/
    
     val jiexi_id = RDD_id.map(
         x => 
         {
           
           val x1 = x.split(",")
          if (x1.length ==1 )
          {
            (x1(0),x1(0),x1(0))
          }
           if (x1.length ==2)
           {
             (x1(1),x1(1),x1(1))
           }
           else
           {
           val x_id = x1(0).subSequence(1, x1(0).length()).toString()
 
           val x_stock_name = x1(2).replace("\"", "").split(":")(1)
           val x_stock_code1 = x1(3).replace("\"", "").split(":")//(1)//.subSequence(0, x1(3).length()).toString()
           val x_stock_code2 = x_stock_code1(x_stock_code1.length-1)
           val x_stock_code = x_stock_code2.substring(0, x_stock_code2.length()-2)
           (x_id,x_stock_name,x_stock_code)
           }
         }
         )
         
         //var RDD_map:Array[(String,String)] = Array()
        var RDD_map = ArrayBuffer[(String,String)]()
         
       val  RDD_jiexi_data_1 =  RDD_jiexi_data.map(
             x =>{
               if (x.length()< 49){
                 var a = 0 
               }
               var tmp_1 = ArrayBuffer[String]()
               val x1 = x.replace("-", "").replace(",", ">").subSequence(4, x.length()-1).toString().split(">")
               for (i <- 0 until x1.length)
               {
                 if (i != 0 || i != x1.length-1)
                 {
                   tmp_1 += x1(i)
                 }
               }
               if (x1.length< 2)
               {
                 x1(0)
               }
               (x1(0),tmp_1,x1(x1.length-1))
             }
             )
             
           val jiexi_id_arr =   jiexi_id.collect()
           
           //val RDD_jiexi_data_1_arr =RDD_jiexi_data_1.collect()
           var combineResult1:Map[String,String] = Map()
         val  result =  RDD_jiexi_data_1.map(
               x =>
                 {
                    val bson_buffer = ArrayBuffer[(String,String)]()
                    var combineResult:Map[String,String] = Map()
                   for (i <- 0 until jiexi_id_arr.length)
                   {
                      val c1 =  x._1.trim().indexOf("_")
                    
                      if (c1 > 0)
                      {
                     if (( x._1.trim().substring(0, c1)).equals( jiexi_id_arr(i)._1.trim()))
                     {
                       combineResult += (x._1 -> (jiexi_id_arr(i)._1+","+jiexi_id_arr(i)._2+","+jiexi_id_arr(i)._3+","+x._2.toString().subSequence(42, x._2.toString().length()-3)))
                     }
                     else
                       {
              var a= 0 
                       }
                     }
                   }
      if (combineResult.isEmpty) "" else combineResult
                 }
               )    
               val result1 = result.filter(!_.equals(""))
              // result1.foreach(println)
             result1.repartition(1).saveAsTextFile("/user/yzsun/11-month/juchao_pipei_result/all_5_"+System.currentTimeMillis())
         sc.stop()
  }
}