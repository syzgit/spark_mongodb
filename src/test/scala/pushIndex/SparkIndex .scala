package pushIndex
import org.apache.solr.client.solrj.beans.Field  
import org.apache.solr.client.solrj.impl.HttpSolrClient  
import org.apache.spark.rdd.RDD  
import org.apache.spark.{SparkConf, SparkContext}  
import java.util  
import scala.annotation.meta.field  
import org.apache.http.impl.client.SystemDefaultHttpClient
import scala.collection.mutable.ArrayBuffer
import org.apache.solr.common.SolrInputDocument

case class Record(  
    
    
                   @(Field@field)("id") id:String,
                   @(Field@field)("table_id") table_id:String,
                   @(Field@field)("table_title") table_title:String,
                   @(Field@field)("table_data_hash")  table_data_hash:String,  
                   @(Field@field)("table_row_title") table_row_title:String,  
                   @(Field@field)("table_column_title") table_column_title:String,  
                   @(Field@field)("table_source") table_source:String 
           
                   /*@(Field@field)("id") id:String,
                   @(Field@field)("table_id") table_id:String,
                   @(Field@field)("src_id") src_id:String,
                   @(Field@field)("table_title")  table_title:String,  
                   @(Field@field)("table_source") table_source:String,  
                   @(Field@field)("table_data_hash") table_data_hash:String,  
                   @(Field@field)("title") title:String,
                   @(Field@field)("stockcode") stockcode:String,  
                   @(Field@field)("company") company:String,
                   @(Field@field)("type")  $type:String,  
                   @(Field@field)("time") time:String,  
                   @(Field@field)("industry_name")industry_name:String,  
                   @(Field@field)("author") author:String,  
                   @(Field@field)("table_row_title")table_row_title:String,
                   @(Field@field)("table_column_title") table_column_title:String*/
                 
                  
                 )  
object SparkIndex  {
  //solr客户端  
  val httpClient = new SystemDefaultHttpClient();
  val client = new HttpSolrClient("http://10.11.255.126:8983/solr/core_table", httpClient);
  //val client= new HttpSolrClient("http://10.1.1.248:9081/solr/fin_report");  
  //批提交的条数  
  val batchCount=10000;  
  val inputDoc = new SolrInputDocument();
  
  
  /*** 
    * 迭代分区数据（一个迭代器集合），然后进行处理 
    * @param lines 处理每个分区的数据 
    */  
  def  indexPartition(lines:scala.Iterator[String] ): Unit ={  
          //初始化集合，分区迭代开始前，可以初始化一些内容，如数据库连接等  
          val datas = new util.ArrayList[Record]()  
          //迭代处理每条数据，符合条件会提交数据  
          lines.foreach(line=>indexLineToModel(line,datas))  
          //操作分区结束后，可以关闭一些资源，或者做一些操作，最后一次提交数据  
          commitSolr(datas,true);  
  }  
  
  /*** 
    *  提交索引数据到solr中 
    * 
    * @param datas 索引数据 
    * @param isEnd 是否为最后一次提交 
    */  
  def commitSolr(datas:util.ArrayList[Record],isEnd:Boolean): Unit ={  
          //仅仅最后一次提交和集合长度等于批处理的数量时才提交  
          if ((datas.size()>0&&isEnd)||datas.size()==batchCount) {  
            client.addBeans(datas);  
            client.commit(); //提交数据  
            datas.clear();//清空集合，便于重用  
          }  
  }  
  
  
  /*** 
    * 得到分区的数据具体每一行，并映射 
    * 到Model，进行后续索引处理 
    * 
    * @param line 每行具体数据 
    * @param datas 添加数据的集合，用于批量提交索引 
    */  
  def indexLineToModel(line:String,datas:util.ArrayList[Record]): Unit ={  
    //数组数据清洗转换  
    val fields=line.split(",").map(row=>row.trim()).map(field =>etl_field(field))
    //将清洗完后的数组映射成Tuple类型  
    val tuple=buildTuble(fields)  
    //将Tuple转换成Bean类型  
    val recoder=Record.tupled(tuple)   
    //将实体类添加至集合，方便批处理提交  
    datas.add(recoder);  
    //datas.replaceAll(re)
    //提交索引到solr  
    commitSolr(datas,false);  
  }  
  
  
  /*** 
    * 将数组映射成Tuple集合，方便与Bean绑定 
    * @param array field集合数组 
    * @return tuple集合 
    */  
  def buildTuble(array: Array[String]):(String, String,String, String, String, String, String )={  
     array match {  
       case Array(s1, s2, s3, s4, s5, s6,s7) => (s1, s2, s3, s4, s5, s6,s7)  
     }  
  }  
  
  
  /*** 
    *  对field进行加工处理 
    * 空值替换为null,这样索引里面就不会索引这个字段 
    * ,正常值就还是原样返回 
    * 
    * @param field 用来走特定规则的数据 
    * @return 映射完的数据 
    */  
  def etl_field(field:String):String={  
    field match {  
      case "" => ""  
      case _ => field  
    }  
  }  
  
  /*** 
    * 根据条件清空某一类索引数据 
    * @param query 删除的查询条件 
    */  
  def deleteSolrByQuery(query:String): Unit ={  
    client.deleteByQuery(query);  
    client.commit()  
    println("删除成功!")  
  }  
 /* def updatesolr(query:String):Unit ={
    client.
  }*/
  
  
  def main(args: Array[String]) {  
    
      val conf = new SparkConf().setAppName("YZSUN")
      val sc = new SparkContext(conf)
   val RDD_id_juchao = sc.textFile("/user/yzsun/11-15-pushData/juchao_result_new_23_1511513053334/")
  //val RDD_id_juchao = sc.textFile("/user/yzsun/11-15-pushData/mrege_result/*")
  //val RDD_id_juchao = sc.textFile("C:/Users/yzsun.abcft/Desktop/juchao_parts000000.txt")
   val rdd_juchao = RDD_id_juchao.map{
        row =>
          {
            val data_before_arr_2 = ArrayBuffer[String]()
            val row1 = row.substring(1, row.length())
    
           
            val arr1 = row1.split(",")
             if (arr1(1).length()>80 || arr1(3).length()>220 || arr1(4).length()>220 )
            {
            val id = "jc_"+arr1(0).trim()
            val table_id = arr1(0)
            val table_title = "error_data"
            val table_data_hash = "-1902946765"
            val table_row_title = "error_data"
            val table_column_title = "error_data"
            val table_source="juchao_tables"
             (id+","+table_id+","+table_title+","+table_data_hash+","+table_row_title+","+table_column_title+","+table_source)

            }
           
             else
             {
                val chid= ArrayBuffer[String]()
               for (i <- 0 until arr1(0).length)
               {
                  val ch = arr1(0).charAt(i);
                 
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chid +=ch.toString()
        }
               }
               val chtitle = ArrayBuffer[String]()
                for (i <- 0 until arr1(1).length)
               {
                  val ch = arr1(1).charAt(i);
                  
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chtitle +=ch.toString()
        }
               }
                val chhash = ArrayBuffer[String]()
                   for (i <- 0 until arr1(2).length)
               {
                  val ch = arr1(2).charAt(i);
                  
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chhash +=ch.toString()
        }
               }
                   val chrow = ArrayBuffer[String]()
                      for (i <- 0 until arr1(3).length)
               {
                  val ch = arr1(3).charAt(i);
                  
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chrow +=ch.toString()
        }
               } 
                      val chcol = ArrayBuffer[String]()
         for (i <- 0 until arr1(4).length)
               {
                  val ch = arr1(4).charAt(i);
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
                 
         chcol +=ch.toString()
        }
               }
                      val aachcol = chcol.toString()
                      val aachrow = chrow.toString()
                      val aachhash = chhash.toString()
                      val aachtitle = chtitle.toString()
                      val aachid = chid.toString()
            val id = "jc_"+aachid.substring(12, aachid.length()-1).replaceAll(",", "").replaceAll(" ", "")
            val table_id = aachid.substring(12, aachid.length()-1).replaceAll(",", "").replaceAll(" ", "")
            val table_title =aachtitle.substring(12, aachtitle.length()-1).replaceAll(",", "").replaceAll(" ", "")
            val table_data_hash = aachhash.substring(12, aachhash.length()-1).replaceAll(",", "").replaceAll(" ", "")
            val table_row_title = aachrow.substring(12, aachrow.length()-1).replaceAll(",", "").replaceAll(" ", "")
            val table_column_title = aachcol.substring(12, aachcol.length()-1).replaceAll(",", "-").replaceAll(" ", "")
            val table_source="juchao_tables"
             (id+","+table_id+","+table_title+","+table_data_hash+","+table_row_title+","+table_column_title+","+table_source)

             }
          }
      }
   
   
    //通过rdd构建索引  
    indexRDD(rdd_juchao);  
    //关闭索引资源  
    client.close();  
    print("success")
    //关闭SparkContext上下文  
    sc.stop();  
  
  
  }  
  
  
  /*** 
    * 处理rdd数据，构建索引 
    * @param rdd 
    */  
  def indexRDD(rdd:RDD[String]): Unit ={  
    //遍历分区，构建索引  
    rdd.foreachPartition(line=>indexPartition(line));  
  }  
  
  
  
}