package pushIndex
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.types.StringType
import java.lang.String
import scala.collection.mutable
//import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.tools.ant.sabbus.Break
import scala.util.control._
object merge_data {
  def main(args:Array[String])={
     
      val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
     val RDD_id_juchao = sc.textFile("/user/yzsun/11-15-pushData/juchao_result_new_12-1-200-1512115073244/part-00001")
      val RDD_id_finace = sc.textFile("/user/yzsun/11-15-pushData/finance.part7/")
     /*val RDD_id_juchao = sc.textFile("C:/Users/yzsun.abcft/Desktop/juchao_merge_30.txt")
      val RDD_id_finace = sc.textFile("C:/Users/yzsun.abcft/Desktop/finace_30.txt")*/
     val RDD_id_finace_arr = RDD_id_finace.map{
        x =>
          {
         val data_arr_1 = ArrayBuffer[String]()
         if (x.length()>60)
         {
         val y= x.substring(12, x.length()-1)
         val yy = y.split(",")
          
         if (yy.length ==9)
         {
           
           for (i <- 0 until yy.length)
           {
             data_arr_1 += yy(i)
           }
         }
         else if (yy.length >9)
         {
           val aa = yy.length -9
           data_arr_1 += "5881ec69cafd5b13a90f3139"
           data_arr_1 += "jc_5881ec69cafd5b13a90f3139"
          for ( i <- 0 until 6)
           {
             data_arr_1 += "errordata"
           }
           data_arr_1 += "1480089600"
         }
           else if (yy.length < 9)
         {
             val aa = 9- yy.length
           for (i <- 0 until yy.length)
           {
             data_arr_1 += yy(i)
           }
           for ( i <- 0 until aa-1)
           {
             data_arr_1 += "errordata"
           }
           data_arr_1 += "1483027200"
         }
         }
         else
         {
           val ss = "5881ec69cafd5b13a90f3138, jc_5881ec69cafd5b13a90f3138, jc_1202851341, 第十届监事会第六次（临时）会议决议公告, 监事会公告, 全新好, 000007, 住宿和餐饮业, 1480521600"
         val s1 = ss.split(",")
         for (i <- 0 until s1.length)
         {
           data_arr_1 += s1(i)
         }
         }
          /* val yy1 = data_arr_1.toString()
           val yy2 = yy1.substring(12, yy1.length()-1)*/
            data_arr_1
          }
      }
     val RDD_id_finace_arr_arr = RDD_id_finace_arr.collect()
     
    
     val RDD_id_juchao_rdd = RDD_id_juchao.map{
       row =>
         {
           val row1 = row.substring(1, row.length()-1)
           val row1_arr = row1.split(",")
           val id = row1_arr(0).indexOf("_")
           val id_data = row1_arr(0).substring(0,id)
           val data_before_arr_2 = ArrayBuffer[String]()
           val loop = new Breaks;
           loop.breakable {
           for (i <- 0 until RDD_id_finace_arr_arr.length)
           {
             if (id_data.trim().equals(RDD_id_finace_arr_arr(i)(0).trim()))
             {
               data_before_arr_2 +="jc_"+row1_arr(0).trim()
               data_before_arr_2 +=row1_arr(0)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(2)
               data_before_arr_2 +=row1_arr(1)
               data_before_arr_2 +="juchao_tables"
               data_before_arr_2 +=row1_arr(2)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(3)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(6)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(5)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(4)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(8)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(7)
               data_before_arr_2 +=RDD_id_finace_arr_arr(i)(5)
               data_before_arr_2 +=row1_arr(3)
               data_before_arr_2 +=row1_arr(4)
              loop.break();
             }
           }
           }
           val str =data_before_arr_2.toString()
          val str1 = str.substring(12, str.length()-1)
          
            if (str1.isEmpty) "ddddd" else str1
         }
     }
    val RDD_id_juchao_rdd1 = RDD_id_juchao_rdd.filter(!_.equals("ddddd"))
    RDD_id_juchao_rdd1.saveAsTextFile("/user/yzsun/11-15-pushData/mrege_result/200-001-7")

     /*val RDD_id_juchao_rdd_arr = RDD_id_juchao_rdd1.toArray()
     for (i <- 0 until RDD_id_juchao_rdd_arr.length)print(RDD_id_juchao_rdd_arr(i))*/
     sc.stop()
  }
}