package bayes_charts
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}
import com.mongodb.hadoop.io.MongoUpdateWritable
import com.mongodb.hadoop.MongoOutputFormat
import scala.collection.mutable.ArrayBuffer

//import scala.collection.mutable.ArrayBuffer

object shurumongo {
  def main(args:Array[String]){
        val conf = new SparkConf().setAppName("yzsun_in_mongo")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val config = new Configuration()

    config.set("mongo.input.uri", "mongodb://bigdata:bre5Uc#yu_hu@10.11.255.122:27017/cr_data.table_product_stock")

    config.set("mongo.input.fields", """{"_id_":1, "classify_type":1}""")

    config.set("mongo.input.noTimeout", "true")

    config.set("mongo.output.uri", "mongodb://bigdata:bre5Uc#yu_hu@10.11.255.122:27017/cr_data.table_product_stock")
    /*val RDD = sc.textFile("/user/yzsun/sun_result_1/1").map { x =>
      {
        if(x.length<10){
     var a=0
        }
        val splits = x.substring(4, x.length()-1).replace("-", "").split(">")
        val obj = new BasicBSONObject()
      //val obj1 = new   MongoUpdateWritable()
        obj.put("table_id",splits(0))
        obj.put("state","2")
        obj.put("table_source","hb_tables")
        obj.put("classify_type",splits(splits.length-1).trim())
       (null, obj)
      }
    }*/

val RDD = sc.textFile("/user/yzsun/10-month/juchao_pipei_result/*").map { x =>
      {
         val splits = x.substring(4, x.length()-1).replace("-", "").replace(">", ",").split(",")
          val obj = new BasicBSONObject()
        if(x.length< 91){
     var a=0
        }
      //val obj1 = new   MongoUpdateWritable()
        obj.put("table_id",splits(0).trim())
        obj.append("state",0)
        obj.put("file_id",splits(1).trim())
        obj.put("stock_name",splits(2).trim())
        obj.put("stock_code",splits(3).trim())
        obj.put("table_source","juchao_tables")
      
        //按数组形式存入
       
     val splist2 =   splits.map { x => 
       x.trim()
       }
         val splits1 =  splist2.slice(4, splits.length-1)
          obj.append("products",splits1)
        obj.put("classify_type",splits(splits.length-1).trim())
       (null, obj)
      }
    }

     
  RDD.saveAsNewAPIHadoopFile("/user/yzsun/bson3.txt", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)

   /* RDD.saveAsNewAPIHadoopFile(
    "/user/yzsun/mongozhuanyong",
    classOf[Object],
    classOf[BSONObject],
    classOf[MongoOutputFormat[Object, BSONObject]],
    config)*/
  
  
/*RDD.saveAsNewAPIHadoopFile(
    "/user/yzsun/mongozhuanyong",
    classOf[Object],
    classOf[MongoUpdateWritable],
    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
    config)*/
   sc.stop()
      }
   
}