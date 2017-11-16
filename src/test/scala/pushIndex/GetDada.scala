package pushIndex
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.hadoop.BSONFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
object GetDada {
  def main(args:Array[String])={
      def main(args:Array[String]){
      val conf = new SparkConf().setAppName("yzsun_in_mongo")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)
    val config = new Configuration()
    val sURI =format("mongodb://%s:%s@%s:%d/%s", "bigdata", "bre5Uc#yu_hu", "10.11.255.122", 27017, "cr_data.juchao_tables")

    config.set("mongo.input.uri", sURI)

   // config.set("mongo.input.fields", """{"_id_":1, "classify_type":1}""")
   config.set("mongo.auth.uri", sURI)
    config.set("mongo.input.noTimeout", "true")
    //config.set("mongo.input.query", "{'table_version' : { $gt: 14}}")  //当全部导出的时候不需要加这句话

   // config.set("mongo.output.uri", "mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")
    //config.set("mongo.job.input.format", "com.mongodb.hadoop.BSONFileInputFormat");
  config.set("mongo.input.query", "$or: [ {'state': 0},{'state': 2}]")
    //config.set("mongo.input.query", "{[$and:{'state':4}]}")
   //config.set("mongo.input.query","{'state':1}")
    config.set("mongo.input.fields", """{'_id':1,'title':1,'data':1}""")
    //config.set("mongo.input.fields", """{'product_1':1}""")
    val documentRDD = sc.newAPIHadoopRDD(
      config,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject])
    /*val bsonRDD = sc.newAPIHadoopFile(path = "hdfs://nameservice1/user/yzsun/hb_talbes",
    classOf[BSONFileInputFormat].asSubclass(classOf[FileInputFormat[Object, BSONObject]]),
    classOf[Object],
    classOf[BSONObject],
    config)*/
    //print( bsonRDD.count()+"---------------------------------------------------------")
    
    documentRDD.saveAsTextFile("/user/yzsun/11-15-pushData/juchao_state_data")
    //    RDD data is a KV pair,so it can use saveAsNewAPIHadoopFile
    //rdd.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)
      sc.stop()
      }
   
  }
}