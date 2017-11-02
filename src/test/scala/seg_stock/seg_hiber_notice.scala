package seg_stock
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

object seg_hiber_notice {
  
  def main(args:Array[String]){
    val writer = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/bayes_train_result/主营.txt"))
    val arr ="C:/Users/yzsun.abcft/Desktop/bayes_train/主营.txt"
   val com13 = transpath(arr)
   writer.print(com13)
    
  }
  def transpath(arr:String) : String = {
    val RDDcompany_idat = Source.fromFile(arr,"UTF-8") //中文乱码 ，这里从文件读取的结果是一个字符串
  val arraycompany_idat = RDDcompany_idat.toArray
   val coarrcom_idat = ArrayBuffer[String]() 
   val coarrcom2_idat = ArrayBuffer[String]()
   for (i <- 0 until arraycompany_idat.length)
   {
     coarrcom2_idat += arraycompany_idat(i).toString()
     
   }
 val sssss_idat = coarrcom2_idat.toList
 val sss1_idat = sssss_idat.toString()
val ss111_idat = sss1_idat.substring(5, sss1_idat.length()-1)
 val tests_idat = ss111_idat.replace(",", "")
 val testss_idat = tests_idat.replace(" ", "")
 val co_idat = testss_idat.replaceAll("[a-z]", "").replaceAll("[0-9]", "").replaceAll("\\//", "").replaceAll("\\....", "").replaceAll("\\?|\\=|\\&|\\:", "").split(",")
 
 val a1b_idat= ArrayBuffer[String]() 
 for (i <- 0 until co_idat.length){
   a1b_idat += co_idat(i)
 }
  val a11_idat= ArrayBuffer[String]() 
 for (i <- 0 until a1b_idat.length){
   
   a11_idat += a1b_idat(i).trim() 
 }
 val a1b1_idat = a11_idat.toList
 val a1b12_idat = a1b1_idat.toString()
 val a1b13_idat = a1b12_idat.substring(5, a1b12_idat.length()-1)

  val segwordst = ArrayBuffer[String]()
  val segwordstt = ArrayBuffer[String]()
  val segwordsttt = ArrayBuffer[String]()
 segwordst +=(NLPTokenizer.segment(a1b13_idat)).toString()  
 for (i <- 0 until segwordst.length)
   {
   
  segwordstt += segwordst(i);
   
   }
 for (i <- 0 until segwordstt.length)
   {
   segwordsttt += segwordstt(i).replaceAll( "[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]" , "")
   
   }
 for (i <- 0 until segwordsttt.length)print(segwordsttt(i))
  val com1 = segwordsttt.toList
         val com12 = com1.toString()
         val com13 = com12.substring(5, com12.length()-1) 
      return com13
       
  }
}