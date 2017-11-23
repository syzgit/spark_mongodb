package spark_hive

object spark_to_hive {
  case class juchao(id:String,table_title:String,table_data_hash:String,table_row_title:String,table_column_title:String)
def main(args:Array[String]):Unit={
   /* // val sparkConf = new SparkConf().setAppName("HBaseTest") 
    //val sc = new SparkContext(sparkConf)  
    val sc = new org.apache.spark.SparkContext   
   // val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("use DataBaseName")
    val data = sc.textFile("path").map(row=>row.substring(1,  row.length()-1)).map(x=>x.split(",")).map(x=>juchao(x(0),x(1),x(2),x(3),x(4)))
   // data.toDF().registerTempTable("table1")
    hiveContext.sql("insert into table2 partition(date='2015-04-02') select name,col1,col2 from table1")
*/}
}