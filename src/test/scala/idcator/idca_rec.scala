package idcator
import scala.reflect.runtime.universe
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
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.Statement
import java.io.PrintWriter
import java.io.File
import com.hankcs.hanlp.HanLP
import java.io.Writer
object idca_rec {

  def main(args:Array[String]){  
    val conf = new SparkConf().setAppName("YZSUN").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
     var srcRDD = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/indicator/2017-07-24idcator.txt")
  .flatMap (line =>{
    var ll = line.replaceAll(", ", " ")
            ll.split(" ")}).map(word => (word, 1))  
          val ida_result =  srcRDD.groupBy(_._1).map {  
     case (word, list) => (word.trim(), list.size) 
  
    }
                    val writer = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/indicator/2017-07-24idcator_resul.txt"))

                   val re = ida_result.collect().mkString
                    val ar = ida_result.toArray()
               val li = ar.toList sortBy ( _._2 )
                    val lli = li.reverse
                    lli.foreach{
    case (key,value) =>
     
      print(key+"="+value)
  }
                 
                   writer.print(lli)
                   writer.close()
ida_result.foreachPartition(myFun)
    sc.stop()
  }
  
   def myFun(iterator: Iterator[(String,Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into stock(name,level) values (?, ?)"
    try {
     
      conn = DriverManager.getConnection("jdbc:mysql://120.26.41.22:3306/abc_pv", 
          "abc_pv", "UrZV*HmB*Ij]U$sr")
      //插入数据
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate() 
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}