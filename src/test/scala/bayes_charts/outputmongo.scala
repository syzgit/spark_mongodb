package bayes_charts
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.hadoop.BSONFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import scala.collection.mutable.ArrayBuffer
object outputmongo {
   def getValue(dbo: BSONObject, key: String) = {
    val value = dbo.get(key)
    if (value eq null) "" else value.asInstanceOf[String]
  }
      def main(args:Array[String]){
      val conf = new SparkConf().setAppName("yzsun_in_mongo")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

   // val data = sc.parallelize(List(("Tom", "31"), ("Jack", "22"), ("Mary", "25")))

    val config = new Configuration()
    val sURI =format("mongodb://%s:%s@%s:%d/%s", "bigdata", "bre5Uc#yu_hu", "10.11.255.122", 27017, "cr_data.juchao_tables")

    config.set("mongo.input.uri", sURI)

   // config.set("mongo.input.fields", """{"_id_":1, "classify_type":1}""")
   config.set("mongo.auth.uri", sURI)
    config.set("mongo.input.noTimeout", "true")
    //config.set("mongo.input.query", "{'state' : { "$gte":0,"$lt":1}}")  //当全部导出的时候不需要加这句话

   // config.set("mongo.output.uri", "mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")
    //config.set("mongo.job.input.format", "com.mongodb.hadoop.BSONFileInputFormat");
  //config.set("mongo.input.query", "$or: [ {'state': 0},{'state': 2}]")
    //config.set("mongo.input.query", "{[$and:{'export_version':6}]}")
   //config.set("mongo.input.query","{'state':2}")
   config.set("mongo.input.fields", """{'title':1,'data':1}""")
    //config.set("mongo.input.fields", """{'src_id':1,'title':1,'type':1,'stock_name':1,'stock_code':1,'industry':1,'time':1}""")
    //config.set("mongo.input.fields", """{'product_1':1}""")
    val documentRDD = sc.newAPIHadoopRDD(
      config,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject])
    val userRDD = documentRDD.map { case (_, doc) =>
      val item_id = getValue(doc, "_id").toString().split("_")(0)
      val int1 =  getValue(doc, "_id").toString().indexOf("_")
      val str1 =  getValue(doc, "_id").toString().substring(0, int1)
      val data_temp = getValue(doc, "data")
      val table_column_title = ArrayBuffer[String]()
      val table_row_title = ArrayBuffer[String]()
      val title_arr = ArrayBuffer[String]()
      val data_left_arr = data_temp.split("}")
       val data_before_arr_2 = ArrayBuffer[String]()
        val data_before_arr_3 = ArrayBuffer[String]()
      for (i <- 0 until data_left_arr.length)
          {
            val data_left_arr_arr = data_left_arr(i).split(",")
            if (data_left_arr(i).contains("\"row\" : 0") )
            {
              val text_data_int = data_left_arr(i).indexOf("text")
              val text_data = data_left_arr(i).substring(text_data_int+7, data_left_arr(i).length).replace("\"", "")
              data_before_arr_2 += text_data
            }
            if ( data_left_arr(i).contains("\"column\" : 0"))
            {
              val text_data_int = data_left_arr(i).indexOf("text")
              val text_data = data_left_arr(i).substring(text_data_int+7, data_left_arr(i).length).replace("\"", "")
              data_before_arr_3 += text_data
            }
          }
      val data_before_arr_2_str = data_before_arr_2.toString().replaceAll(",", "")
          val data_before_arr_3_str = data_before_arr_3.toString().replaceAll(",", "")
          val str_2 = data_before_arr_2_str.substring(12, data_before_arr_2_str.length()-1)
          val str_3 = data_before_arr_3_str.substring(12, data_before_arr_3_str.length()-1)
     val table_row_title_str_hash = str_2.hashCode()
      val table_column_title_hash = str_3.hashCode()
      val data_temp_hash = data_temp.hashCode()
      
      val title_str = getValue(doc, "title").toString()
     (str1,getValue(doc, "_id").toString,title_str.replaceAll(",", ""),str_2,table_row_title_str_hash,str_3,table_column_title_hash,data_temp_hash)
    }
    
    userRDD.saveAsTextFile("/user/yzsun/11-15-pushData/all_juchao_data/new_juchao_all_data_xianxia")
    //    RDD data is a KV pair,so it can use saveAsNewAPIHadoopFile
    //rdd.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)
      sc.stop()
      }
   

}














/*package bayes_charts
 import com.mongodb.spark._
    import org.apache.spark.{SparkConf, SparkContext}
    import org.bson._
    import org.bson.Document
object outputmongo {
      def main(args:Array[String]){
         val conf = new SparkConf()
      .setMaster("local")
      .setAppName("inmongo")
      //同时还支持mongo驱动的readPreference配置, 可以只从secondary读取数据
      //.set("spark.mongodb.input.uri", " mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")
      .set("mongodb.output.uri", " mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")
//spark.mongodb.output.uri=mongodb:spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info
    val sc = new SparkContext(conf)
    // 创建rdd
   // val originRDD = MongoSpark.load(sc)

    // 构造查询
    val dateQuery = new BsonDocument()
      .append("$gte", new BsonDateTime(start.getTime))
      .append("$lt", new BsonDateTime(end.getTime))
    val matchQuery = new Document("$match", BsonDocument.parse("{\"type\":\"1\"}"))
val RDD = sc.textFile("/user/yzsun/33").map { x =>
      {
        val splits = x.substring(1, x.length()-1).split(",")
        
        Document.parse(s"{_id_: splits(0),classify_type:splits(1)}")
      }
    }
//val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))

MongoSpark.save(RDD)
sc.stop()
      }
  
}*/