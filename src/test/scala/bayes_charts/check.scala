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
import com.hankcs.hanlp.HanLP
import java.io.Writer
object check {
  def main(args:Array[String]){
     val conf = new SparkConf().setAppName("YZSUN").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
  /*  var srcRDD = sc
  .textFile("/user/yzsun/part-00000/")
 //.map(row=> row.substring(5, row.length()-1).replace("-", "").split(">"))
  srcRDD.take(3).foreach(println)*/
    val str11 = "SolrDocument{id=sc_58889157077a8b26d9db2ea9_10_0}"
   print(str11.subSequence(16, str11.length()-1))
     val aa="123adfas8f9f"
     val a =3.0
     if (a >2){
       print("true")
     }
     var aaa = 1
     if (aaa == 1 )
     {
       aaa = aaa +1
       print("aaa",aaa)
     }
     println("aaaaaaaaa"+aaa)
     val b ="aa"
     val c ="58ed2309cafd5b5e8b921c85_80_1"
     val c1 = c.lastIndexOf("_")
     val c2 = c.indexOf("_")
     val arr_test=Array("一二","ersan","一二","手续费及佣金净收入", "手续费及佣金收入")
     val aaaaa = "公司金融业务,  个人金融业务,  资金业务, @ 外部利息净收入/支出, & 内部利息净收入/支出, "
    val aaaaaa1 =  aaaaa.split("@")
    val a_c = aaaaaa1(0)
    println(a_c)
    for ( i <- 0 until aaaaaa1.length)
    {
     if (aaaaaa1.length == 2)
     {
       val a3 = aaaaaa1(1).split("&")
       if (a3.length ==1 )
       {
         println("&",a3(0))
       }
       if (a3.length ==2 )
       {
         println("&&",a3(1))
       }
       
       
     }
     
    }
     val arr_test1 = arr_test.distinct
     for (i <- 0 until arr_test1.length)
       println(arr_test1(i))
     print(c+"-----          "+c.substring(0, c1)+"        ---------            "+c.subSequence(0, c2)+"      ---------")
     print(b.length()+"|||||||||||||")
  /* val reg =  """([0-9+] (a-z)+)""".r
    val bb =  aa.replaceAll("[^u]", "")*/
    print(aa.replaceAll("[a-z]", "")+"|||||"+aa.replace("a", "")+aa.replace("[a-z]", "")+aa.replaceAll("\\[a-z]+,", ",,"))
  sc.stop()
  }
}