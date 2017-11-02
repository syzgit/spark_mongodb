package bayes_charts
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import java.io.PrintWriter
import java.io.File
import com.hankcs.hanlp.HanLP
import java.io.Writer
import org.apache.spark.SparkContext
object show_bson {
  case class Person(name:String,age:Int)
  def main(args:Array[String]){
     Logger.getLogger("org").setLevel(Level.ERROR)
     val a ="2014E"
    print(a.length())
		
  }
}