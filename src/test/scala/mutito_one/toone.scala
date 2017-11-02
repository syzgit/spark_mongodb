package mutito_one
import java.sql.Connection
import java.sql.DriverManager
import scala.collection.mutable.ArrayBuffer 
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import java.io.File
import com.hankcs.hanlp.HanLP
import java.io.Writer


import com.hankcs.hanlp.seg.common.Term;  
import com.hankcs.hanlp.seg.Segment
import javafx.animation.PathTransition.Segment
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.hankcs.hanlp.dictionary.CustomDictionary
import scala.io.Source
import scala.util.control._
object toone {
  //将一行转换成一列
  def main(args:Array[String]){
     val writer = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/bayes_train_result/主营.txt"))
    val arr ="C:/Users/yzsun.abcft/Desktop/bayes_train/主营.txt"
   val com13 = transpath(arr)
   writer.print(com13)
   writer.close()
   print("done")
  }
   def transpath(arr:String) : String = {
  val RDDcompany = Source.fromFile(arr,"UTF-8") //中文乱码 ，这里从文件读取的结果是一个字符串
  val arraycompany = RDDcompany.toArray
   val coarrcom = ArrayBuffer[String]() 
   val coarrcom2 = ArrayBuffer[String]()
   for (i <- 0 until arraycompany.length)
   {
     coarrcom2 += arraycompany(i).toString().replaceAll(",", "").replaceAll(" ", "")
   }
 val sssss = coarrcom2.toList
 val sss1 = sssss.toString()
 val ss111 = sss1.substring(5, sss1.length()-1)
 val tests = ss111.replace(",", "")
 val testss = tests.replace(" ", "")
 val co = testss.split(",")
 val a1b= ArrayBuffer[String]() 
 for (i <- 0 until co.length){
   a1b += (co(i).replaceAll("[\\n|\\r|\\nr]", " "))
 }
  val a11= ArrayBuffer[String]() 
 for (i <- 0 until a1b.length){
   a11 += a1b(i).replaceAll(",,", "").trim() 
 }
 val a1b1 = a11.toList
 val a1b12 = a1b1.toString()
 val a1b13 = a1b12.substring(5, a1b12.length()-1)//打印company.txt
 //print(a1b13)
       return a1b13
  }
}