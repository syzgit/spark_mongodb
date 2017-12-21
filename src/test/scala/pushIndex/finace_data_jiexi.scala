package pushIndex
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
import com.alibaba.fastjson.JSON
import java.text.SimpleDateFormat

object finace_data_jiexi {
   def main(args:Array[String])=
  {
     Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
    val RDD_fin = sc.textFile("D:/BaiduNetdiskDownload/finace_data/*")
    //val RDD_fin = sc.textFile("C:/Users/yzsun.abcft/Desktop/data_fiance.txt")
    val RDD_fin_data =  RDD_fin.map{
     row =>
       {
         val data_before_arr_1 = ArrayBuffer[String]()
          val row1 = row.replaceAll("},", ",")  
          val int1  = row1.indexOf(",")
        val row2 =  row1.substring(int1+1, row.length-1)
         val json=JSON.parseObject(row2)
         data_before_arr_1 +=json.getJSONObject("_id").get("$oid").toString().replaceAll(",", " ")
         data_before_arr_1 += ("sc_"++json.getJSONObject("_id").get("$oid").toString())
         data_before_arr_1 += json.getString("src_id").replaceAll(",", " ")
         if (json.getString("title")==null)
         {
           data_before_arr_1 += ""
         }
         else
         {
           data_before_arr_1 +=json.getString("title").replaceAll(",", " ")
         }
           if (json.getString("type")==null)
         {
           data_before_arr_1 += ""
         }
         else
         {
         data_before_arr_1 += json.getString("type").replaceAll(",", " ")
         }
           data_before_arr_1 +=json.getString("stock_name")
         data_before_arr_1 += json.getString("stock_code")
        // val sss =json.getString("industry")
         if (json.getString("industry")==null)
         {
           data_before_arr_1 += ""
         }
         else
         {
           data_before_arr_1 +=json.getString("industry").replaceAll(",", " ")
         }
          val date=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")  //2008-12-11T06:30:00.000Z 2016-08-09T00:00:00.000Z
          val ddd = json.getJSONObject("time").get("$date").toString()
          //.replaceAll(".253Z", ":00").replaceAll(".218Z", ":00").replaceAll(".222Z", ":00").replaceAll(".221Z", ":00").replaceAll(".220Z", ":00").replaceAll(".224Z", ":00").replaceAll(".219Z", ":00").replaceAll(".999Z", ":00").replaceAll(".000Z", ":00")
        val dd =  ddd.replaceAll(",", "").replaceAll("T00:", " ").replaceAll("T01:", " ").replaceAll("T02:", " ").replaceAll("T03:", " ").replaceAll("T04:", " ").replaceAll("T05:", " ").replaceAll("T09:", " ").replaceAll("T10:", " ").replaceAll("T13:", " ").replaceAll("T14:", " ").replaceAll("T22:", " ").replaceAll("T24:", " ")
        .replaceAll("T06:", " ").replaceAll("T20:", " ").replaceAll("T19:", " ").replaceAll("T18:", " ").replaceAll("T17:", " ").replaceAll("T15:", " ").replaceAll("T16:", " ").replaceAll("T12:", " ").replaceAll("T11:", " ").replaceAll("T07:", " ").replaceAll("T08:", " ").replaceAll("T21:", " ").replaceAll("T23:", " ")
       val dd1 = dd.substring(0, dd.length()-5)+":00"
    val addtime=date.parse(dd1.toString()).getTime()/1000; 
    
   data_before_arr_1 +=  addtime.toString()  
         //data_before_arr_1 += json.getJSONObject("time").get("$date").toString()
         data_before_arr_1
       }
     }
   /* val RDD_fin_data =  RDD_fin.map{
     row =>
       {
         val RDD_fin_arr = row.split(",")
         val data_before_arr_4 = ArrayBuffer[String]()
         for ( i <- 0 until RDD_fin_arr.length)
         {
           if (i ==0)
          {
            data_before_arr_4 += RDD_fin_arr(i)
          }
           if (i != 1 && i !=8)
           {
             val RDD_fin_arr_arr = RDD_fin_arr(i).split(":")
             
           for (j <- 0 until RDD_fin_arr_arr.length)
           {
             if (j ==1)
             {
             data_before_arr_4 += RDD_fin_arr_arr(j).replace("\"", "")
             }
           }
           }
          if (i==8 )
          {
             val RDD_fin_arr_arr = RDD_fin_arr(i).split(":")
             
           for (j <- 0 until RDD_fin_arr_arr.length)
           {
             if (j ==2)
             {
             data_before_arr_4 += RDD_fin_arr_arr(j).replace("\"", "")
             }
           }
          }
          
         }
         data_before_arr_4
       }
       
   }*/
  /* val s =  RDD_fin_data.toArray()
     for (i <- 0 until s.length)print(s(i))*/
    RDD_fin_data.repartition(1).saveAsTextFile("/user/yzsun/11-15-pushData/finace_data_result_new_111"+System.currentTimeMillis())

   sc.stop()
  }
}