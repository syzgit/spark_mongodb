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
object zhuying {
  def main(args:Array[String]){
     Logger.getLogger("org").setLevel(Level.ERROR)
     val conf = new SparkConf().setAppName("YZSUN").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
    //取要分类的数据 
   var RDD_bson = sc
 // .textFile("/user/yzsun/result_juchao_alldata/")
  .textFile("C:/Users/yzsun.abcft/Desktop/juchao_4_test.txt")
  //.map(row=> row.replace(",", "").split(" "))
  .map(row=> row.substring(4, row.length()-1).replace("-", "").replace(">", "").split(" "))
   //RDD_bson.foreach { x => x.foreach(print) }
   //取模型  
  /*var RDD_zichan = sc
  .textFile("/user/yzsun/bayes_train_result/991.txt").map(x => x.split(" "))
  var RDD_xianjin = sc
  .textFile("/user/yzsun/bayes_train_result/992.txt").map(x => x.split(" "))
  var RDD_lirun = sc
  .textFile("/user/yzsun/bayes_train_result/993.txt").map(x => x.split(" "))*/
   var RDD_zhuying = sc
  .textFile("/user/yzsun/bayes_train_result/997.txt").map(x => x.split(" "))
  
 
  
  /*var RDD_zichan = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/991.txt").map(x => x.split(" "))
  var RDD_xianjin = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/992.txt").map(x => x.split(" "))
  var RDD_lirun = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/993.txt").map(x => x.split(" "))
  var RDD_zhuying = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/three_baobiao/997.txt").map(x => x.split(" "))
  
  */
       //将分类数据转换成id -> 数据形式
   val bson_rdd_buffer =  RDD_bson.map { 
   var m2=Map[String,ArrayBuffer[String]]()
     x => {
         var m1=Map[String,ArrayBuffer[String]]()
         val bson_buffer = ArrayBuffer[String]()
         for (i <- 0 until x.length){
          val bson0 = x(1)
          if (i !=1 && x(i).length()>=2){
              bson_buffer += x(i)
           }
          m1 += (bson0 -> bson_buffer)
         }
         m1.foreach{
               case(key,value)=>
                 {
                   m2 += (key -> value)  
                 }
             }
         m1
       }
     }
     
     //将资产负债表模型转换成数组,因为后面要跟分类数据做匹配，2个RDD不能做for循环，因此将模型数据转换成数组
   /*val zichan_rdd_arr =  RDD_zichan.toArray()(0)
   val xianjin_rdd_arr=  RDD_xianjin.toArray()(0)
   val lirun_rdd_arr =  RDD_lirun.toArray()(0)*/
 
   val zhuying_rdd_arr =  RDD_zhuying.toArray()(0)
   
   //得到分类结果
  /* val j_zichan =    model_classfy(bson_rdd_buffer,zichan_rdd_arr,1)
   val j_lirun =    model_classfy(bson_rdd_buffer,lirun_rdd_arr,2)
   val j_xianjin =    model_classfy(bson_rdd_buffer,xianjin_rdd_arr,3)*/
   val j_zhuying =    model_classfy(bson_rdd_buffer,zhuying_rdd_arr,4)
 
   //j_zichan.foreach(println)
   
  //合并结果去重
   // val san1 = j_xianjin.union(j_zichan).union(j_lirun).union(j_zhuying)
       //val san1 = j_yingli.union(j_yingyun).union(j_chengzhang).union(j_zhuying)

    val san2 = j_zhuying.distinct()
   
 
   /* j_zhuying.foreach{
     println
             }*/
    //发现不repartition的时候生产8万多个文件，在后面进行计算的时候又会生成8万多个文件，这样在生成文件的时候花费大量时间，
    //因此试试只生产1000个文件的话对后面的有没有优化作用
    san2.repartition(1).saveAsTextFile("/user/yzsun/sun_result_1/result_juchao_alldata_zhuying")
    
  sc.stop()
  }
  
  def model_classfy(bson_rdd_buffer:RDD[Map[String, ArrayBuffer[String]]],xianjin_rdd_arr: Array[String],tag:Int):RDD[Map[String, String]]={
     var xianjin_map2=Map[String,String]()
      val j_xianjin =      bson_rdd_buffer.map{
      x => 
        {
          var xianjin_map1 :Map[String,String] = Map()
          x.foreach{
          case(key,value)=>{
             var k =0
             var companymapfeatures:Map[String,String] = Map()
              var xianjin_map :Map[String,String] = Map()
               val xianjin_length = (value.length).toDouble
                   for (j <- 0 until value.length) 
                  {  
                  for (i <- 0 until xianjin_rdd_arr.length){
                    if (value(j).equals(xianjin_rdd_arr(i)))
                      {
                        k = k+1
                      }
                                                            }          
                                                          }
             val kmore = k.toDouble / xianjin_length
                   // if (k>3 ){
              if (kmore > 0.93 && k>20 && xianjin_length < 55 && tag==1){
                         
                     xianjin_map += (key ->"资产负债表")
                 
                           }
             // if (k>3 ){
              if (kmore > 0.88 && k>15 && xianjin_length < 50 && tag==2 ){
                         
                     xianjin_map += (key ->"利润表")
                 
                           }
              // if (k>3){
               if (kmore > 0.85 && k>15 && xianjin_length < 50 && tag==3 ){
                         
                     xianjin_map += (key ->"现金流量表")
                 
                           }
                   /* else
                 {
                     xianjin_map += (key ->"")
                 }*/
                 xianjin_map.foreach
                   {
                     case (key ,value)=>
                   {
                       xianjin_map1 += (key -> value)
                    }
                       }
   } 
                }
              xianjin_map1
        }
    }
     j_xianjin
  }
  
}