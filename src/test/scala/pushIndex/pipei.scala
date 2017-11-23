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
object pipei {
  def main(args:Array[String])=
  {
     Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
    val RDD_id = sc.textFile("/user/yzsun/11-15-pushData/juchao_state_data_state_lt/")
   //val RDD_id = sc.textFile("C:/Users/yzsun.abcft/Desktop/test_data.txt")
    val RDD_id_jiexi =  RDD_id.map{
       row =>
         {
          val data_int =  row.indexOf("data")
          val data_before = row.substring(0, data_int)
          val data_left= row.substring(data_int, row.length)
          val data_before_arr = data_before.split(",")
          val data_left_arr = data_left.split("}")
          val data_before_arr_1 = ArrayBuffer[Any]()
          val data_left_hash = data_left.hashCode()
          for (i <- 0 until data_before_arr.length)
          {
            if ( i != 0 && i != 3 && i != 4)
            {
              val data_before_arr_left = data_before_arr(i).split(":")
              for(j <- 0 until data_before_arr_left.length)
              {
                if (j == 1)
                {
                 data_before_arr_1 += data_before_arr_left(j).replace("\"", "")
                }
              }
            }
          }
          val data_before_arr_2 = ArrayBuffer[String]()
          //val data_before_arr_5 = ArrayBuffer[Any]()
          val data_before_arr_3 = ArrayBuffer[String]()
          for (i <- 0 until data_left_arr.length)
          {
            val data_left_arr_arr = data_left_arr(i).split(",")
            if (data_left_arr(i).contains("\"row\" : 0") )
            {
              val text_data_int = data_left_arr(i).indexOf("text")
              val text_data = data_left_arr(i).substring(text_data_int+7, data_left_arr(i).length).replace("\"", "")
              data_before_arr_2 += text_data
            }
            if ( data_left_arr(i).contains("\"column\" : 0"))
            {
              val text_data_int = data_left_arr(i).indexOf("text")
              val text_data = data_left_arr(i).substring(text_data_int+7, data_left_arr(i).length).replace("\"", "")
              data_before_arr_3 += text_data
            }
          }
          val data_before_arr_2_str = data_before_arr_2.toString().replaceAll(",", "")
          val data_before_arr_3_str = data_before_arr_3.toString().replaceAll(",", "")
          val str_2 = data_before_arr_2_str.substring(12, data_before_arr_2_str.length()-1)
          val str_3 = data_before_arr_3_str.substring(12, data_before_arr_3_str.length()-1)
          (data_before_arr_1(0).toString(),data_before_arr_1(1).toString(),data_left_hash,str_2,str_3)
         }
     }
   /* val s =  RDD_id_jiexi.toArray()
     for (i <- 0 until s.length)
     {
       print(s(i))
      
     }
     println("")*/
     
   
    /*val RDD_id_jiexi_dd_arr =  RDD_id_jiexi_dd.toArray()
   for (i <- 0 until RDD_id_jiexi_dd_arr.length)print(RDD_id_jiexi_dd_arr(i))*/
   RDD_id_jiexi.saveAsTextFile("/user/yzsun/11-15-pushData/merge_data_result_new_23_"+System.currentTimeMillis())
   
     sc.stop()
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  /*
  def juchao(RDD_id: RDD[String]):RDD[(String, String, ArrayBuffer[String], ArrayBuffer[String])]=
  {
    val RDD_id_jiexi =  RDD_id.map{
       row =>
         {
          
          val data_int =  row.indexOf("data")
          val data_before = row.substring(0, data_int)
          val data_left= row.substring(data_int, row.length)
          val data_before_arr = data_before.split(",")
          val data_left_arr = data_left.split("}")
          val data_before_arr_1 = ArrayBuffer[Any]()
          for (i <- 0 until data_before_arr.length)
          {
            if ( i != 0 && i != 3 && i != 4)
            {
              val data_before_arr_left = data_before_arr(i).split(":")
              for(j <- 0 until data_before_arr_left.length)
              {
                if (j == 1)
                {
                 data_before_arr_1 += data_before_arr_left(j).replace("\"", "")
                }
              }
            }
          }
          val data_before_arr_2 = ArrayBuffer[String]()
          //val data_before_arr_5 = ArrayBuffer[Any]()
          val data_before_arr_3 = ArrayBuffer[String]()
          for (i <- 0 until data_left_arr.length)
          {
            val data_left_arr_arr = data_left_arr(i).split(",")
            if (data_left_arr(i).contains("\"row\" : 0") )
            {
              val text_data_int = data_left_arr(i).indexOf("text")
              val text_data = data_left_arr(i).substring(text_data_int+7, data_left_arr(i).length).replace("\"", "")
              data_before_arr_2 += text_data
            }
            if ( data_left_arr(i).contains("\"column\" : 0"))
            {
              val text_data_int = data_left_arr(i).indexOf("text")
              val text_data = data_left_arr(i).substring(text_data_int+7, data_left_arr(i).length).replace("\"", "")
              data_before_arr_3 += text_data
            }
          }
        
         
          //data_before_arr_1 += data_before_arr_2
          //data_before_arr_1 += data_before_arr_3
          //data_before_arr_1++data_before_arr_2++data_before_arr_3
          //data_before_arr_1
          (data_before_arr_1(0).toString(),data_before_arr_1(1).toString(),data_before_arr_2,data_before_arr_3)
         }
     }
    RDD_id_jiexi
  }
  def finace(RDD_fin: RDD[String]):RDD[(String, String, String, String, String, String)]={
    val RDD_fin_data =  RDD_fin.map{
     row =>
       {
         val RDD_fin_arr = row.split(",")
         val data_before_arr_4 = ArrayBuffer[String]()
         for ( i <- 0 until RDD_fin_arr.length)
         {
           if (i ==0)
          {
            data_before_arr_4 += RDD_fin_arr(i).replace("(", "")
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
          if (i==8)
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
        (data_before_arr_4(0),data_before_arr_4(1),data_before_arr_4(2),data_before_arr_4(3),data_before_arr_4(4),data_before_arr_4(5))
       }
       
   }

for (i <- 0 until RDD_fin_data_arr.length)
{
  print(RDD_fin_data_arr(i))
}
RDD_fin_data
    
  }*/
  /*def mergedata (RDD_id_jiexi: RDD[(String, String, ArrayBuffer[String], ArrayBuffer[String])],RDD_fin_data: RDD[(String, String, String, String, String, String)])={
    val RDD_fin_data_arr =     RDD_fin_data.toArray()
    val RDD_id_jiexi_dd = RDD_id_jiexi.map{
      row =>
        {
          val data_before_arr_7 = ArrayBuffer[String]()
          val data_before_arr_title = ArrayBuffer[String]()
         val c1 =  row._1.trim().indexOf("_")
          var a =0
          var b =0
            for (i <- 0 until RDD_fin_data_arr.length)
            {
               val c1 =  row._1.trim().indexOf("_")
           
              if ((row._1.trim().substring(0, c1)).equals(RDD_fin_data_arr(i)._1.trim()))
              {
              b = b+1
                data_before_arr_7 +=("jc_"+row._1.trim(),RDD_fin_data_arr(i)._1,RDD_fin_data_arr(i)._2,RDD_fin_data_arr(i)._3,RDD_fin_data_arr(i)._4,RDD_fin_data_arr(i)._5,RDD_fin_data_arr(i)._6)
                 data_before_arr_title += row._2
              }
              else
              {
                var a =0
              }

            
        
         
          }
            
           if (data_before_arr_7.isEmpty) "" else (data_before_arr_7,row._3,row._4,data_before_arr_title)
         
        }
    }
  }*/
}