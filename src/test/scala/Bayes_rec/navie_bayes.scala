package Bayes_rec
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
object navie_bayes {
   Logger.getLogger("org").setLevel(Level.ERROR)
   case class RawDataRecord(category:String,text: String)
   //case class RawDataRecord1(category:String,text: String)
  def main(args:Array[String]){
     
     //val writer1_test = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/segtest/测试.txt"))
    // val writer1 = new PrintWriter(new File("C:/Users/yzsun.abcft/Desktop/segtest/part-00000测试结果.txt"))
     
    val conf = new SparkConf().setAppName("YZSUN")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
    var srcRDD = sc
  //.textFile("C:/Users/yzsun.abcft/Desktop/bayes_train_result/")
   .textFile("/user/yzsun/bayes_train_result/")
  .map(_.split(","))
  .map(attributes => RawDataRecord(attributes(0),attributes(1)))
  .toDF()
  
   var srcRDDtest = sc
  .textFile("/user/yzsun/20_dov/part-00000.D01")
  //.textFile("C:/Users/yzsun.abcft/Desktop/company/bson3.txt")
  //.map(_.split(","))
  .map(row=> row.substring(4, row.length()-1).replace("-", "").split(">"))
  .map(attributes => RawDataRecord(attributes(0),attributes(1)))
  .toDF()
  
 /* var srcRDDtest2 = sc
  .textFile("/user/yzsun/cutfile/part-00000.D05")
  //.map(_.split(","))
  .map(row=> row.substring(4, row.length()-1).replace("-", "").replace(">", ","))
  .map(_.split(","))
  .map(attributes => RawDataRecord(attributes(0),attributes(1)))
  .toDF()
 // srcRDDtest2.foreach {println }
var srcRDDtest = srcRDDtest2.map(_.split(",")).map(attributes => RawDataRecord(attributes(0),attributes(1)))
  .toDF()*/
  //70%作为训练数据，30%作为测试数据
    /*val splits = srcRDD.randomSplit(Array(0.9, 0.1))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()*/
  //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcRDD)
    wordsData.select($"text",$"words").take(1)
    //计算每个词在文档中的词频
    var hashingTF = new HashingTF()
   .setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
  
    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    //trainDataRdd.take(1).foreach(println)
         //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")   
 
    val idar = ArrayBuffer[String]()
     //val idar1 = ArrayBuffer[String]()
    var testwordsData1 = tokenizer.transform(srcRDDtest)
    var testfeaturizedData1 = hashingTF.transform(testwordsData1)
    var testrescaledData1 = idfModel.transform(testfeaturizedData1)
    //writer1_test.print(testrescaledData1.collect().mkString)
   val id =  testrescaledData1.select($"category").collect()
   for (i <- 0 until id.length)
     {
     val jj = id(i).toString().replace("[","").replace("]","")
     idar += jj
     //idar1 += jj
     }

    var testDataRdd1 = testrescaledData1.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint((label.replace("_", "9").replaceAll("[a-z]", "")).toDouble, Vectors.dense(features.toArray))
    }
    //对测试数据集使用训练模型进行分类预测
     testDataRdd1.map(p => (println(p.features)))
    val testpredictionAndLabel1 = testDataRdd1.map(p => (model.predict(p.features), (p.label)))
    print("测试的结果为：")
   //testpredictionAndLabel1.foreach(println)
    //writer1_test_res.println(testpredictionAndLabel12.collect().mkString)
   val one = ArrayBuffer[Double]()
    
    
    val testre = testpredictionAndLabel1.collect()
    val testli = testre.toList
    
    var companymapfeatures:Map[Int,String] = Map()
    companymapfeatures += (2 -> "个股-财务预测")
    companymapfeatures += (3 -> "无法分类")
    companymapfeatures += (4 -> "宏观数据信息")
    companymapfeatures += (5 -> "信用评级")
    companymapfeatures += (6 -> "标准评级")
    companymapfeatures += (7 -> "人员信息")
    companymapfeatures += (9 -> "股票业务评级")
    companymapfeatures += (8 -> "公司评级")
    companymapfeatures += (10 -> "个股-行情数据")
    companymapfeatures += (11 -> "利润表")
    companymapfeatures += (12 -> "资产负债表")
    companymapfeatures += (13 -> "现金流量表")
    companymapfeatures += (14 -> "资产负债表")
    companymapfeatures += (15 -> "资产负债表")
    companymapfeatures += (16 -> "资产负债表")
    companymapfeatures += (17 -> "现金流量表")
    companymapfeatures += (18 -> "现金流量表")
    companymapfeatures += (19 -> "现金流量表")
     companymapfeatures += (63 -> "利润表")
     companymapfeatures += (64 -> "利润表")
     companymapfeatures += (66 -> "利润表")
  /*  companymapfeatures += (11 -> "个股-利润表")
    companymapfeatures += (12 -> "个股-资产负债表-商业银行")
    companymapfeatures += (13 -> "个股-现金流量表-商业银行")
    companymapfeatures += (14 -> "个股-资产负债表-证券公司")
    companymapfeatures += (15 -> "个股-资产负债表-保险公司")
    companymapfeatures += (16 -> "个股-资产负债表-一般企业")
    companymapfeatures += (17 -> "个股-现金流量表-证券公司")
    companymapfeatures += (18 -> "个股-现金流量表-保险公司")
    companymapfeatures += (19 -> "个股-现金流量表-一般企业")
     companymapfeatures += (63 -> "利润表(银行)")
     companymapfeatures += (64 -> "利润表(保险)")
     companymapfeatures += (66 -> "利润表(证券)")*/
    companymapfeatures += (22 -> "新能源")
    companymapfeatures += (23 -> "ETL")
    companymapfeatures += (24 -> "银行利润表项--证券评估")
    companymapfeatures += (25 -> "营业收入及增速")
    companymapfeatures += (26 -> "行业周报-医药生物行业")
    companymapfeatures += (27 -> "面向企业和个人 CDs 有利于负债结构稳定 ")
    companymapfeatures += (28 -> "机构对股票意见 ")
     companymapfeatures += (29 -> "证券对公司业绩预增分析 ")
    /*  companymapfeatures += (31 -> "人物基本表")
   companymapfeatures += (32 -> "资产负债表(银行)")
     companymapfeatures += (33 -> "资产负债表(保险)")
     companymapfeatures += (34 -> "资产负债表(一般)")
     companymapfeatures += (35 -> "资产负债表(证券)")
     companymapfeatures += (36 -> "现金流量表(银行)")
     companymapfeatures += (37 -> "现金流量表(保险)")
     companymapfeatures += (38 -> "现金流量表(一般)")
     companymapfeatures += (39 -> "现金流量表(证券)")*/
     companymapfeatures += (40 -> "公司基本信息表")
     companymapfeatures += (41 -> "公司基本信息表2")
     companymapfeatures += (42 -> "com_dividend_fields")
     companymapfeatures += (43 -> "财务指标分析")
     companymapfeatures += (44 -> "业绩预告")
     companymapfeatures += (45 -> "公司首发基本信息表")
     companymapfeatures += (46 -> "公司首发配售明细表")
     companymapfeatures += (47 -> "公司首发计划表")
     companymapfeatures += (48 -> "公司首发发行结果表")
     companymapfeatures += (49 -> "公司高管任职情况表2")
     companymapfeatures += (50 -> "公司高管任职情况表")
     companymapfeatures += (51 -> "公司高管持股及薪酬表2")
     companymapfeatures += (52 -> "公司高管持股及薪酬表")
     companymapfeatures += (53 -> "公司高管持股变动情况表")
     companymapfeatures += (54 -> "com_news_fields")
     companymapfeatures += (55 -> "公告表")
     companymapfeatures += (56 -> "业绩快报")
     companymapfeatures += (57 -> "公司主营构成")
     companymapfeatures += (58 -> "公司职工构成表")
     companymapfeatures += (59 -> "行业基本信息表")
     companymapfeatures += (60 -> "机构基本信息表")
     companymapfeatures += (61 -> "人物信息表")
     companymapfeatures += (62 -> "人物基本表2")
    
     //companymapfeatures += (65 -> "利润表(一般)")
     
    // companymapfeatures += (67 -> "region_fields")
     companymapfeatures += (68 -> "行业报告基本信息表")
     companymapfeatures += (69 -> "证券基本信息表")
     companymapfeatures += (70 -> "大宗交易表")
     companymapfeatures += (71 -> "机构持股表")
     companymapfeatures += (72 -> "股东户数")
     companymapfeatures += (73 -> "证券所属行业表")
      companymapfeatures += (74 -> "龙虎榜交易信息明细表")
     companymapfeatures += (75 -> "龙虎榜交易信息主表")
      companymapfeatures += (76 -> "公司前十大流通股东")
     companymapfeatures += (77 -> "公司前十大股东")
      companymapfeatures += (78 -> "融资融券交易明细表")
     companymapfeatures += (79 -> "融资融券交易汇总表")
      companymapfeatures += (80 -> "股票简称变动表")
     companymapfeatures += (81 -> "股票日行情表")
      companymapfeatures += (82 -> "股票1分钟行情表")
     companymapfeatures += (83 -> "股票5分钟行情表")
       companymapfeatures += (84 -> "股票30分钟行情表")
     companymapfeatures += (85 -> "股票60分钟行情表")
       companymapfeatures += (86 -> "股票月行情表")
     companymapfeatures += (87 -> "股票周行情表")
       companymapfeatures += (88 -> "股票限售流通明细表")
     companymapfeatures += (89 -> "股票限售解禁时间表")
       companymapfeatures += (90 -> "股票状态变动表")
     companymapfeatures += (91 -> "股本结构变动表")
       companymapfeatures += (92 -> "停牌复牌表")
     companymapfeatures += (93 -> "定期报告预约披露时间表")
       companymapfeatures += (94 -> "常量表")
      companymapfeatures += (96 -> "交易日历表")
    val testarr = ArrayBuffer[Double]()
    var inshe:Map[Double,String] = Map()
    var two:Map[Double,String] = Map()
    
   val one1 = testpredictionAndLabel1.map{
        case (key,value)=>
          {
            
            one += value
          }
     }
   
    
    /*one1.take(3).foreach(println)
    one.take(3).foreach(println)*/
    testli.foreach{
        case (key,value)=>{
          //testarr取结果的值，取到id
         
          testarr += value
         
        }
          
    }
    //val one2 = one1.collect()(0)
    
   /*one1.foreach{
     var i = 0
      x =>{
        two += (x(0) -> idar(i))
      }
      i = i+1
     
    }
    two.foreach(println)*/
    /*for ( i <- 0 until one2.length){
      two += (one2(i) -> idar(i))
    }*/
     //two.take(3).foreach(println)
     //inshe是一个id与真实_id的映射关系
    for (i <- 0 until testarr.length){
      inshe += (testarr(i) -> idar(i))
    }
    /*inshe.foreach{
    
        case (key,value)=>{
          
          println(key+"="+value)
        }
          
    }*/
   // testpredictionAndLabel1.take(3).foreach(println)
    //companymapfeatures.take(3).foreach(println)
     
    
    val three1 = testpredictionAndLabel1.map{
      case (key1,value1) =>{
        var m1=Map[Double,String]()
        //print(key1,value1)
       var three6:Map[Double,String] = Map()
        companymapfeatures.foreach{
          case (key,value)=>{
             var three=  ArrayBuffer[(Double,String)]()
             if (key1.toInt == key){
              // print(value)
           three +=(value1 -> value)    
         }
             three.foreach{
               case(key,value)=>
                 {
                   m1 += (value1 -> value)  
                 }
             }
          
        }
          
        }
        m1
      }
        
    }
   //three1.foreach(println)
   /* var into:Map[Double,String] = Map()
    companymapfeatures.foreach{
   case(key,value) =>
    testli.foreach{
    
        case (key1,value1)=>{
         if (key1.toInt == key){
           into +=(value1 -> value)    
         }
        }
    }
    
 }*/
    
     
  val three2 = three1.map{row =>
      {var m2=Map[String,String]()
        row.foreach{
          
          case (key1 ,value1)=>
            {
              
             inshe.foreach{
               case (key ,value)=>
                 {
                    var four:Map[String,String] = Map()
                    if (key1 == key){
                   //print(value)
                   four += (value -> value1.toString())
                 }
                     four.foreach{
               case(key,value)=>
                 {
                   m2 += (key -> value)  
                 }
             }
                    
                 }
                 
             }
             
        }
           
      }
         m2
      }
      
     }
     
    three2.foreach(println)
 /*    val three3 = three2.collect()
     val three4 = three3(three3.length-1)*/
     //print(three4)
    //three2.foreach(println)
    
  /*  val three3 =  three2.collect()
    for (i <- 0 until three3.length){
      three3(i).foreach{
        case (key ,value)=>
          {
            print(key ,value)
          }
      }
    }*/
   //val three2 = three1.collect()
/*   for (i <- 0 until three2.length){
     //println("three2"+three2(i))
     three2(i).foreach{
       case (key1,value1)=>
         //println(key1,value1)
         two.foreach{
           case(key,value)=>
             {
               //print(key,value)
                 if (key1 == key){
                   //print(value)
                   four +=(value -> value1.toString())
                 }
             }
         }
     }
     }*/
   /*three2.map(
       row =>
         row.foreach{
           case (key ,value)=>
             {
               print(key +"= " +value)
             }
         }
         
           )*/
         
     
     /*three2.foreach{
      case (key,value)=>
        {
          println(key +" 分类为  " + value)
        }
    }*/
     
/*    var resultall:Map[String,String] = Map()
    inshe.foreach{
   case(key,value) =>
    into.foreach{
    
        case (key1,value1)=>{
         // print(key1+"|"+value1)
         if (key1 == key){
          resultall +=(value -> value1)
           //println(value +"  分类为     "+value1)
          
         }
        }
          
    }
    
 }
    resultall.foreach{
      case (key,value)=>
        {
          writer1.print(key+","+value)
          //println(key +" ||||  " + value)
        }
    }*/
   // writer1.close()
 // val five = sc.parallelize(three4.toList) 
   //val result = sc.parallelize(resultall.toList) 
 three2.saveAsTextFile("/user/yzsun/result_file/d_20_test")
   // five.saveAsTextFile("/user/yzsun/34")
   // result.foreachPartition(myFun)
    //测试数据集，做同样的特征表示及格式转换
  /*  var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
    testpredictionAndLabel.take(50).foreach(println)
    //testpredictionAndLabel.repartition(1).saveAsTextFile("/user/yzsun/bayes_testdata/testbayes"+System.currentTimeMillis())
    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)*/
    sc.stop()
  }
   def changeto_(arr:String,x:Int, y :Int):String={
    println(arr)
     val arr1 = arr.toList
     val arrre = ArrayBuffer[String]()
     for (i <- 0 until arr1.length){
       if (i == x || i==y){
        arrre += "_"
       }
       else 
       arrre += arr1(i).toString()
     }
     val com1_ida = arrre.toList
         val com12_ida = com1_ida.toString().replace(",", "").replace(" ", "")
         val com13_ida = com12_ida.substring(5, com12_ida.length()-1)
     return com13_ida
   }
   
    case class stock(name: String,level:String)
 
 /* def myFun(iterator: Iterator[(String,String)]): Unit = {
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
        ps.setString(2, data._2)
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
  }*/
}