package bayes_charts
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.bson.{BSONObject, BasicBSONObject}
//import com.mongodb.hadoop.io.MongoUpdateWritable
import com.mongodb.hadoop.MongoOutputFormat
import com.mongodb.spark.sql.fieldTypes.api.java.ObjectId
import com.mongodb.hadoop.io.MongoUpdateWritable
import org.bson.BasicBSONObject

object updatemongo {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("yzsun_update_mongo")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val config = new Configuration()

    config.set("mongo.input.uri", "mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")

    config.set("mongo.input.fields", """{"_id_":1, "classify_type":1}""")

    config.set("mongo.input.noTimeout", "true")

    config.set("mongo.output.uri", "mongodb://spider:Serwe-8dfgre@120.26.41.22:27017/cr_data.table_external_info")
val RDD = sc.textFile("/user/yzsun/sun_result_1/1").map { x =>
      {
        val splits = x.substring(4, x.length()-1).replace("-", "").split(">")
        val query = new BasicBSONObject()
        if(x.length<10 || splits(0).trim().length()>15){
     var a=0
        }
      query.append("table_id",splits(0).trim())
       val update = new BasicBSONObject()
      update.append("$set", new BasicBSONObject().append("state", 2))
      //update.append("$set", new BasicBSONObject().append("classify_type", splits(splits.length-1).trim()).append("state", 2))
       val muw = new MongoUpdateWritable(query, update, false, false, false)
         (null, muw)
      }
    }
  
  /*   items.map(x => {
      val mongo_id = new ObjectId(x("id").toString)
      val query = new BasicBSONObject()
      query.append("_id", mongo_id)
      val update = new BasicBSONObject()

      update.append("$set", new BasicBSONObject().append("field_name", x("new_value")))
      val muw = new MongoUpdateWritable(query,update,false,true)
      (null, muw)
    })*/
    RDD.saveAsNewAPIHadoopFile(
       "/user/yzsun/mongozhuanyong",
       classOf[Any],
       classOf[Any],
       classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]],
       config
     )
     
  
  
/*RDD.saveAsNewAPIHadoopFile(
    "/user/yzsun/mongozhuanyong",
    classOf[Object],
    classOf[MongoUpdateWritable],
    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
    config)*/
   sc.stop()
      }
  
}