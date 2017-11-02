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

object jiexi_bson {
   
  def main(args:Array[String]){
    
     Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("YZSUN_bson")
    val sc = new SparkContext(conf)
     val localrdd = sc.textFile("/user/yzsun/10-month/juchao_tables_all1508549363176/")
     var companymapfeatures:Map[String,String] = Map()
     val tmp = ArrayBuffer[String]()
    //val writer = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/10-18-test.txt"))
     //val writer1 = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/bayes_train/part-00002.txt"))

     val ss = localrdd.map{row =>
     row.split(",") 
    
     }
    var s23 =""
    
     
    
    val tmparr1 = ArrayBuffer[String]()
    val companyAAAA =jie2(ss)
    /*val tostr = companyAAAA.map(
      row =>
        {
          row.foreach{
            case (key,value)=>{
              tmparr1 += key+","+value
            }  
          }
          tmparr1
        }   
    )*/
   //companyAAAA .repartition(1).saveAsTextFile("/user/yzsun/mongo_data_result5.txt")
    companyAAAA.saveAsTextFile("/user/yzsun/10-month/juchao_jiexi_data/_new_alldata_")
     sc.stop()
  }
  
  //根据mongo row_data数据解析方法保留数值
   def jie2(ss: RDD[Array[String]]):RDD[Map[String, String]]={
   
     val tmparr = ArrayBuffer[String]()
     val tmpre = ArrayBuffer[(String,String)]()
   var companyAAAA:Map[String,String] = Map()
  val ss2 = ss.map {
     s1 =>  
    var tmpid = ""
    companyAAAA = companyAAAA.empty
   for ( i <- 0 until s1.length)
     {
      
       val tmpall = s1(i).replace("\"","").replace("{","").replace("}","").replace("(","").replace(")","").replace("[","").replace("]","").split(":")
        //println(tmpall)
         for (k <- 0 until tmpall.length){
           
           if ( i !=0 && i !=1 && i !=3 && i !=2)
           {
             val tmp2 = tmpall(k).toString().replace(",", "").replace("[", "").replace("]", "").replace(">", "").replace("—", "").toString()
            //需要加trim()函数才可以，要删掉词2边的空格
             if (tmp2.trim() .equals("2013E") ||tmp2.trim().equals("2014E")  ||tmp2.trim().equals("2015E") ||tmp2.trim().equals("2016E") ||tmp2.trim().equals("2012E"))
             { 
               //print("========="+tmp2)
               tmparr += tmp2
             }
             tmparr += tmp2.trim().toString().replace("【", "").replaceAll("title", "").replace("】", "").replace("★", "").replace(">", "")
           }
           if (k == 1 && i ==1 ){
            
             tmpid = tmpall(k)
           }
           if (k ==1 && i ==3 && i !=2){
             tmparr += tmpall(k)
           }
           
         }
     }
      val sssss11 = tmparr.toList
     
 val sss111 = sssss11.toString().replaceAll("[\\[,\\],\\,]", "")
 val ss11111 = sss111.substring(5, sss111.length()-1) 
 tmparr.clear()
       companyAAAA += (tmpid -> (ss11111))
      /* companyAAAA.foreach{
        case (key,value)=>
          print(key,value)
      }*/
      
     
   companyAAAA
   }
   
    //return companyAAAA
   //ss2.foreach(println)
    return ss2
  }
  //根据mongo row_data数据解析方法
 /* def jie2(ss: RDD[Array[String]]):RDD[Map[String, String]]={
   
     val tmparr = ArrayBuffer[String]()
     val tmpre = ArrayBuffer[(String,String)]()
   var companyAAAA:Map[String,String] = Map()
  val ss2 = ss.map {
     s1 =>  
    var tmpid = ""
    companyAAAA = companyAAAA.empty
   for ( i <- 0 until s1.length)
     {
      
       val tmpall = s1(i).replace("\"","").replace("{","").replace("}","").replace("(","").replace(")","").replace("[","").replace("]","").split(":")
        //println(tmpall)
         for (k <- 0 until tmpall.length){
           
           if ( i !=0 && i !=1 && i !=3)
           {
             val tmp2 = tmpall(k).toString().replace(",", "").replace("[", "").replace("]", "").replace(">", "").replace("—", "").toString()
            //需要加trim()函数才可以，要删掉词2边的空格
             if (tmp2.trim() .equals("2013E") ||tmp2.trim().equals("2014E")  ||tmp2.trim().equals("2015E") ||tmp2.trim().equals("2016E") ||tmp2.trim().equals("2012E"))
             { 
               //print("========="+tmp2)
               tmparr += tmp2
             }
             tmparr += tmp2.trim().toString().replaceAll("[0-9]", "").replace(".", "").replace("【", "").replaceAll("title", "").replace("】", "").replace("★", "").replace(">", "").replace("%", "")
           }
           if (k == 1 && i ==1 && i !=3 && i !=0){
            
             tmpid = tmpall(k)
           }
           if (k ==1 && i ==3){
             tmparr += tmpall(k)
           }
           
         }
     }
      val sssss11 = tmparr.toList
     
 val sss111 = sssss11.toString().replaceAll("[\\[,\\],\\,]", "")
 val ss11111 = sss111.substring(5, sss111.length()-1) 
 tmparr.clear()
       companyAAAA += (tmpid -> (ss11111))
       companyAAAA.foreach{
        case (key,value)=>
          print(key,value)
      }
      
     
   companyAAAA
   }
   
    //return companyAAAA
   //ss2.foreach(println)
    return ss2
  }*/

  //根据mongo的data数据进行解析方法
 /* def jie2(ss: RDD[Array[String]]):RDD[Map[String, String]]={
   
     val tmparr = ArrayBuffer[String]()
     val tmpre = ArrayBuffer[(String,String)]()
   var companyAAAA:Map[String,String] = Map()
  val ss2 = ss.map {
     s1 =>  
    var tmpid = ""
    companyAAAA = companyAAAA.empty
   for ( i <- 0 until s1.length)
     {
      
       val tmpall = s1(i).replace("\"","").replace("{","").replace("}","").replace("(","").replace(")","").replace("[","").replace("]","").split(":")
        //println(tmpall)
         for (k <- 0 until tmpall.length){
           
           if (k == 1 && i !=1 && i !=3)
           {
             val tmp2 = tmpall(k).toString().replace(",", "").replace("[", "").replace("]", "").replace(">", "").toString()
            //需要加trim()函数才可以，要删掉词2边的空格
             if (tmp2.trim() .equals("2013E") ||tmp2.trim().equals("2014E")  ||tmp2.trim().equals("2015E") ||tmp2.trim().equals("2016E") ||tmp2.trim().equals("2012E"))
             { 
               //print("========="+tmp2)
               tmparr += tmp2
             }
             tmparr += tmp2.trim().toString().replaceAll("[0-9]", "").replace(".", "").replace("【", "").replace("】", "").replace("★", "").replace(">", "").replace("%", "")
           }
           if (k == 1 && i ==1 && i !=3){
            
             tmpid = tmpall(k)
           }
           if (k ==2 && i ==3){
             tmparr += tmpall(k).replace(">", "")
           }
           
         }
     }
      val sssss11 = tmparr.toList
     
 val sss111 = sssss11.toString().replaceAll("[\\[,\\],\\,]", "")
 val ss11111 = sss111.substring(5, sss111.length()-1) 
 tmparr.clear()
       companyAAAA += (tmpid -> (ss11111))
       companyAAAA.foreach{
        case (key,value)=>
          print(key,value)
      }
      
     
   companyAAAA
   }
   
    //return companyAAAA
   //ss2.foreach(println)
    return ss2
  }*/
  
  
  
  
  /*def jie(ss: RDD[Array[String]]):Map[String, String]={
   val s1 =ss.collect()
     val tmparr = ArrayBuffer[String]()
     val tmpre = ArrayBuffer[(String,String)]()
   var companyAAAA:Map[String,String] = Map()
   for ( i <- 0 until s1.length)
     {
      var tmpid = ""
       for (j <- 0 until s1(i).length){
      
         var tmpall = (s1(i))(j).replace("\"","").replace("{","").replace("}","").replace("(","").replace(")","").replace("[","").replace("]","").split(":")
        //println(tmpall)
         for (k <- 0 until tmpall.length){
           if (k == 1 && j !=1 && j !=3){
             val tmp1 =  NLPTokenizer.segment(tmpall(k)).toString()
             val tmp2 = tmpall(k).toString().replace(",", "").replace("[", "").replace("]", "").toString()
            //需要加trim()函数才可以，要删掉词2边的空格
             if (tmp2.trim() .equals("2013E") ||tmp2.trim().equals("2014E")  ||tmp2.trim().equals("2015E") ||tmp2.trim().equals("2016E") ||tmp2.trim().equals("2017"))
             { 
               //print("========="+tmp2)
               tmparr += tmp2
             }
             tmparr += tmp2.trim().toString().replaceAll("[0-9]", "").replace(".", "").replace("【", "").replace("】", "").replace("★", "")
           }
           if (k == 1 && j ==1 && j !=3){
             tmpid = tmpall(k)
           }
           if (k ==2 && j ==3){
             tmparr += tmpall(k)
           }
         }
       }
      val sssss11 = tmparr.toList
 val sss111 = sssss11.toString().replaceAll("[\\[,\\],\\,]", "")
 val ss11111 = sss111.substring(5, sss111.length()-1) 
       companyAAAA += (tmpid -> (ss11111))
      tmparr.clear()
     }
    return companyAAAA
  }*/
}