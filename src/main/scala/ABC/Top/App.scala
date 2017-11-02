package ABC.Top
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

/**
 * @author ${user.name}
 */

object App {
  
//case class row( text: String)

  case class stock(name: String,level:Int)
 
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
  def nonNegativeMod(x: Int, mod: Int): Int = { //根据 numFeatures 设置的哈希表容量，来设定索引号
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
   

  def indexOf(term: Any): Int = nonNegativeMod(term.##, 1 << 20) //根据分词来生成索引号

  case class RawDataRecord(text: String)
 
  def main(args : Array[String]) {
    
   val conf = new SparkConf().setAppName("YZSUN").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
    var srcRDD = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/test/new2.txt")
  .map(_.split(","))
  .map(attributes => RawDataRecord(attributes(0)))
  .toDF()
  
   var srcRDDcompany = sc
  .textFile("C:/Users/yzsun.abcft/Desktop/testwords/company.txt")
  .map(_.split(","))
  .map(attributes => RawDataRecord(attributes(0)))
  .toDF()
 
  //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcRDD)
    var companyData = tokenizer.transform(srcRDDcompany)
    wordsData.select($"text",$"words").take(1)
    //计算每个词在文档中的词频
    var hashingTF = new HashingTF()
   .setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
   var companyfeature = hashingTF.transform(companyData)
     //featurizedData.select($"text",$"rawFeatures").foreach(println)
  //companyfeature.select($"text",$"rawFeatures").take(15).foreach(println)
    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    
    var idfModel = idf.fit(featurizedData)
    var companyModel = idf.fit(companyfeature)
    
    var rescaledData = idfModel.transform(featurizedData)
    var rescaleCompany = companyModel.transform(companyfeature)
    //rescaledData.select( $"words", $"features").foreach(println)
    
  //rescaledData.select($"features"). foreach(print)
//out.txt文件从tf-idf向量里面取到ID和权重的对应关系
 val feature =rescaledData.select( $"features").toJSON
 //feature.take(10).foreach(println)
 //从tf-idf里取到中文词
 val words = rescaledData.select($"words").toJSON
 val companywords = rescaleCompany.select($"words").toJSON
 
 //从company.txt 文件里取词与ID的对应关系
val companyfreature = rescaleCompany.select( $"features").toJSON
val companywordseg = companywords.map{
     row =>row.split(",")
       val startI= row.lastIndexOf("[")
    if( startI == -1)
      print("feature Invalide startI")
    val end = row.indexOf("]",startI)
    if( end == -1)
      print("feature Invalide end")
      row.substring(startI+1,end)+","
   } 
//companywordseg.take(10).foreach(println)
 //取company.txt 文件里的ID
  val companyrrfeature = companyfreature.map{row => row.split(",")       
    val start2 = row.indexOf("[", 2)
    val end2 = row.indexOf("]")
     row.substring(start2+1, end2)+","
    } 
   //companyrrfeature.take(10).foreach(println)
   //合并company里的词与ID的关系
   val companyword_features = companywordseg.union(companyrrfeature)
  val comfeaturearr = companyrrfeature.toArray()
  val comwordsarr = companywordseg.toArray()
   var conmpanyii=0
 var companymapfeatures:Map[String,String] = Map()
   val f1_company = companyword_features.collect().mkString
 var companyAAAA:Map[String,String] = Map()
 val f2_company = f1_company.split(",")

 val companymutableArrbb = ArrayBuffer[String]();  
//将分词与权重关联起来
 for (i <- 0 until 3372)  
 {
  // print("i等于"+i+"时"+"mutableArrbb(i)="+mutableArrbb(i)+"f2(half+i).toDouble="+f2(half+i).toDouble)
   companyAAAA += (comwordsarr(i).replace("\"","").replaceAll("[\\[\\]\\,,\\|]", " ") -> comfeaturearr(i).replaceAll("[\\[\\]\\,\\|]", " "))
 }
  //根据权重排序
companyAAAA.take(10).foreach(println)
   
 //取中文单词
 val wordseg = words.map{
     row =>row.split(",")
       val startI= row.lastIndexOf("[")
    if( startI == -1)
      print("feature Invalide startI")
    val end = row.indexOf("]",startI)
    if( end == -1)
      print("feature Invalide end")
      row.substring(startI+1,end)+","
   }
//从文档里读到每行的数据存在数组
 val wordsegarry = wordseg.collect()
 val wordslist = wordsegarry.toList
 //print("wordsegarry"+wordslist.take(1))
 var n:List[(String,Int)] =List()
 var nn:List[(String,Int)] =List()
 
  val mutableArrbb = ArrayBuffer[String]();  

//从文件里按行读取先索引后排序的词，为与后面的权重相对应上
 for (i <- 0 until wordsegarry.length )
 {
   val j = wordsegarry(i).split(",").toArray
    var Bb1:Map[String,Int]= Map()
    var Bbbb:Map[String,Int]= Map()
   for (k <- 0 until j.length)
   { 
     Bbbb += (j(k) ->indexOf( j(k)))
       Bb1 = Bbbb
   } 
  val bb = Bb1.toList sortBy ( _._2 )
  
    bb.foreach {
case (key,value) =>
  mutableArrbb += key
  print("*******************"+key+"="+value)
}
 }

 //print("mutableArrbblist************"+mutableArrbb)
 //feature.take(10).foreach(println)
 //取权重
   val rrfeature = feature.map{row => row.split("")    
    val startI= row.lastIndexOf("[")
    if( startI == -1)
      print("feature Invalide startI")
    val end = row.indexOf("]",startI)
    if( end == -1)
      print("feature Invalide end")
      row.substring(startI+1,end)+","    
   } 
// rrfeature.take(10).foreach(println)
    //取ID
    val rrfeature2 = feature.map{row => row.split(",")       
    val start2 = row.indexOf("[", 2)
    val end2 = row.indexOf("]")
     row.substring(start2+1, end2)+","
    }   
    //rrfeature2.take(10).foreach(println)
   
    val rrfeaturearr1 = rrfeature2.collect().mkString
      val rrfeaturearr = rrfeaturearr1.split(",")
 

   //print("rrfeaturearr"+rrfeaturearr.length)
    //单词、ID、权重关联
  val features = rrfeature2.union(rrfeature)
  
  //print(features.collect().mkString)
 var i=0
 var mapfeatures:Map[String,String] = Map()
   val f1 = features.collect().mkString
 var A:Map[String,Double] = Map()
 val f2 = f1.split(",")
 val half = f2.length /2  
 //print("half:"+ half)
//将分词与权重关联起来
 for (i <- 0 to half-1)  
 {
  // print("i等于"+i+"时"+"mutableArrbb(i)="+mutableArrbb(i)+"f2(half+i).toDouble="+f2(half+i).toDouble)
   A += (rrfeaturearr(i) -> f2(half+i).toDouble)
 }
  //根据权重排序
  val AA = A.toList sortBy
  ( _._2 )
//前5个，list反转取前5
  var AAA = AA.reverse.take(AA.length-1)
/*AA.reverse.take(10).foreach {
case (key,value) =>
println(key + " = " + value)
}*/
  
 val removelist = ArrayBuffer[(String,Double)]();  
 val nolist = ArrayBuffer[(String,Double)]();  
 val errorkey = ArrayBuffer[(String,Double)]();  
 val goodlist = ArrayBuffer[(String,Double)]();
 val topid = ArrayBuffer[String]();
 var AAAA = AAA.toArray
 
 
 
AAAA.foreach
 {
   case(key,value) =>
     {
      
     /*  if (key.length() < 4)
       {
         removelist +=((key,value))   
       }
       else if (key.contains("?��?")){
         errorkey += ((key,value))
       }else if (key.length() >8)
       {
         nolist += ((key,value))
       }
       else */
         goodlist += ((key,value))
         topid += key.replaceAll("[\\[\\]\\,\\|]", " ")
         //print(key.replaceAll("[\\[\\]\\,\\|]", " ") +"="+value+"   ")
     }
 }
 
 val comarr = companyAAAA.toArray
 /*for (i <- 0 until 10)print("company"+comarr(i))
 for (i <- 0 until 10)print("topid"+topid(i))*/
 /*topid.append("61609")
 topid +="61609"*/
 for (i <- 0 until topid.length-1)print("=============="+topid(i))
 val topresult = ArrayBuffer[String]()
 for (j <- 0 until topid.length-1){
   for (i <- 0 until comarr.length-1)
   {
     var ch = comarr(i)._2
     var ch1 = topid(j)
     //println("********"+ch+"-------------"+topid(j))
     if (ch.contains(ch1))
     { 
       println("***********************"+ch)
       topresult += comarr(i)._1
     }
   }
 }
 
 val resultarr = ArrayBuffer[(String,Int)]()
 for (i <- 0 until topresult.length)
   {
   resultarr += ((topresult(i).replace("\"",""),i))
   //println(topresult(i))
   
   }
 //print("removelist"+removelist +"\n"+"goodlist"+goodlist)
 val falllist = goodlist.toList sortBy ( _._2 )

 val uplist = falllist.reverse

 val finalwords = ArrayBuffer[String]();
uplist.foreach{
    case (key,value) =>
     finalwords += key
      print(key+"="+value)
  }
 for ( i <- 0 until finalwords.length) println(finalwords(i))
//取股票名称
 var RDDcompany = sc
  .textFile("D:\\hanlp\\data\\dictionary\\custom\\company.txt")
  val arraycompany = RDDcompany.toArray()
  val arrcom = ArrayBuffer[(String,Int)]()
  val coarrcom = ArrayBuffer[String]()
  
  
  //test
  val arrcomtest = ArrayBuffer[(String,Double)]()
 uplist.foreach{
   case(key,value) =>
     for (j <- 0 until arraycompany.length)
    {
      
      if ((key.replace("\"","")).toUpperCase().equals(arraycompany(j)))
      {
        arrcomtest += ((key.replace("\"","").toUpperCase(),value))
        //println(i+"test"+arrcom)
      }
    }
 }

  

 val arrcomtest1 = arrcomtest.toList
  val AArdd = sc.parallelize(arrcomtest1)
  val resultsql = resultarr.toList
  val sqlDD = sc.parallelize(resultsql)
  sqlDD.foreachPartition(myFun)
  print("testtesettsettst**********")



  //AArdd.sortByKey().collect().foreach{println}
//AArdd.repartition(1).saveAsTextFile("/user/yzsun/test00"+System.currentTimeMillis())
   
  sc.stop() 
  }

}

   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   /*val rrwords = words.map{row => 
    val startI= row.indexOf("[")
    if(startI > -1)
      print( "StartI invalide" + row)
     if(startI < -1)
      print( "StartI ERROR" )
    val end = row.indexOf("]")
    if( end > -1)
      print( "EndI invalide" + row)
    row.substring(startI,end)+"|"
      
   }
    print(rrwords.collect().mkString)*/
    
  /* val r = tfidf.map{  
          case SparseVector(size, indices, values)=>  
            val words=indices.map(index=>bcWords)  
            words.zip(values).sortBy(-_._2).take(20).toSeq  
        }  
        
    titls.zip(r).saveAsTextFile("hdfs://1.185.74.124:9000/20new_result_"+System.currentTimeMillis) */ 
       
 /*  val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")  
    val sc = new SparkContext(sparkConf)  
     val sqlContext = new SQLContext(sc)
    val tablename = "pv_search"  
    val conf = HBaseConfiguration.create()  
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置  
    conf.set("hbase.zookeeper.quorum","cloudera001,cloudera002,cloudera003")  
    //设置zookeeper连接端口，默认2181  
    conf.set("hbase.zookeeper.property.clientPort", "2181")  
    conf.set(TableInputFormat.INPUT_TABLE, tablename)  
 
    // 如果表不存在则创建表  
    val admin = new HBaseAdmin(conf)  
    if (!admin.isTableAvailable(tablename)) {  
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))  
      admin.createTable(tableDesc)  
    }  
  
    //读取数据并转化成rdd  
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],  
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],  
      classOf[org.apache.hadoop.hbase.client.Result])  
  
    val count = hBaseRDD.count()  
    println(count)  
 val newRdd  = hBaseRDD.map( row => HanLP.segment(Bytes.toString((row._2).getValue("words".getBytes,"serchName".getBytes))));
    //newRdd是从hbase读取的数据
 val hashingTF = new HashingTF()  
//val mapWords=newRdd.map(w=>(hashingTF.indexOf(w),w)).collect.toMap  
val  featurizedData: RDD[Vector] =  hashingTF.transform(newRdd)
//val bcWords=featurizedData.context.broadcast(mapWords) 
// CountVectorizer也可获取词频向量  
 featurizedData.take(10).foreach{println};
val idf = new IDF()
    
      //得到tf-idf值
val idfModel = idf.fit(featurizedData);
val rescaledData: RDD[Vector] = idfModel.transform(featurizedData)  
rescaledData.take(10).foreach(println)

    sc.stop()  
    admin.close()  */
  










/*val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest")

    // 创建hbase configuration
val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE,"pv_search")

    // 创建 spark context
val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    

    // 从数据源获取数据
val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
val shop = hbaseRDD.map{ case (_, result) =>  
      val serchName = Bytes.toString(result.getValue("words".getBytes, "serchName".getBytes))  
      val createtime = Bytes.toString(result.getValue("words".getBytes, "create_date".getBytes))
     ( createtime, serchName) 
}
    import sqlContext.implicits._
    val rowrdd = shop.map(x=>tbl_test(x._1,x._2)).toDF()  
 rowrdd.registerTempTable("tbl")  
    sqlContext.sql("select * from tbl").show()

case class tbl_test(createtime:String,serchName:String)  */

