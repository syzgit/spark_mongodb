package pushIndex
import org.apache.solr.client.solrj.beans.Field  
import org.apache.solr.client.solrj.impl.HttpSolrClient  
import org.apache.spark.rdd.RDD  
import org.apache.spark.{SparkConf, SparkContext}  
import java.util  
import scala.annotation.meta.field  
import org.apache.http.impl.client.SystemDefaultHttpClient
import scala.collection.mutable.ArrayBuffer
case class Record(  
           /* val id = arr1(0)
            val table_id = arr1(1)
            val src_id = arr1(2)
            val table_title = chid333
            val table_source=arr1(4)
            val table_data_hash = arr1(5)
            val title = chtitle666
            val stockcode =arr1(7)
            val company = arr1(8)
            val $type= arr1(9)
            val time = arr1(10)
            val industry_name = arr1(11)
            val author =arr1(12)
            val table_row_title = chhash131313
            val table_column_title = chrow141414*/
                   @(Field@field)("id") id:String,
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
                   @(Field@field)("table_column_title") table_column_title:String
                 
                  
                 )  
object SparkIndex  {
  //solr客户端  
  val httpClient = new SystemDefaultHttpClient();
val client = new HttpSolrClient("http://10.11.255.126:8983/solr/core_table", httpClient);
  //val client= new HttpSolrClient("http://10.1.1.248:9081/solr/fin_report");  
  //批提交的条数  
  val batchCount=10000;  
  
/*  def main(args: Array[String]) {  
  
    val d1=new Record("row1","title","content","1","01","57");  
    val d2=new Record("row2","title","content","1","01","57");  
    val d3=new Record("row3","title","content","1","01",null);  
    client.addBean(d1);  
    client.addBean(d2)  
    client.addBean(d3)  
    client.commit();  
    println("提交成功！")  
  
  
  } */ 
  
  
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
  def buildTuble(array: Array[String]):(String, String,String, String, String, String, String, String, String, String, String, String, String, String, String )={  
     array match {  
       case Array(s1, s2, s3, s4, s5, s6,s7,s8,s9,s10,s11,s12,s13,s14,s15) => (s1, s2, s3, s4, s5, s6,s7,s8,s9,s10,s11,s12,s13,s14,s15)  
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
    //根据条件删除一些数据  
    //deleteSolrByQuery("t1:03")  
    //远程提交时，需要提交打包后的jar  
    //val jarPath = "target\\spark-build-index-1.0-SNAPSHOT.jar";  
    //远程提交时，伪装成相关的hadoop用户，否则，可能没有权限访问hdfs系统  
    //System.setProperty("user.name", "webmaster");  
    //初始化SparkConf  
    //val conf = new SparkConf().setMaster("spark://192.168.1.187:7077").setAppName("build index ");  
    //上传运行时依赖的jar包  
    //val seq = Seq(jarPath) :+ "D:\\tmp\\lib\\noggit-0.6.jar" :+ "D:\\tmp\\lib\\httpclient-4.3.1.jar" :+ "D:\\tmp\\lib\\httpcore-4.3.jar" :+ "D:\\tmp\\lib\\solr-solrj-5.1.0.jar" :+ "D:\\tmp\\lib\\httpmime-4.3.1.jar"  
    //conf.setJars(seq)  
    //初始化SparkContext上下文  
    //val sc = new SparkContext(conf);  
    //此目录下所有的数据，将会被构建索引,格式一定是约定好的  
    //val rdd = sc.textFile("hdfs://192.168.1.187:9000/user/monitor/gs/"); 
      val conf = new SparkConf().setAppName("YZSUN").setMaster("local")
    val sc = new SparkContext(conf)
   //val RDD_id = sc.textFile("/user/yzsun/11-15-pushData/finace_data_result_new_1111511232895074/")
  //val RDD_id_juchao = sc.textFile("/user/yzsun/11-15-pushData/mrege_result/merge_data_result_00000/")
  val RDD_id_juchao = sc.textFile("C:/Users/yzsun.abcft/Desktop/part-new")
   val rdd_juchao = RDD_id_juchao.map{
        row =>
          {
            val data_before_arr_2 = ArrayBuffer[String]()
            val arr1 = row.split(",")
             if (arr1.length !=15 || arr1(3).length()>80 || arr1(6).length()>220  || arr1(13).length()>220 || arr1(14).length()>220)
            {
            val id = arr1(0)
            val table_id = arr1(1)
            val src_id = arr1(2)
            val table_title = "error_data"
            val table_source="juchao_tables"
            val table_data_hash = "-1902946765"
            val title = "error_data"
            val stockcode ="error_data"
            val company = "error_data"
            val $type= "error_data"
            val time = "1480089600"
            val industry_name = "error_data"
            val author ="error_data"
            val table_row_title = "error_data"
            val table_column_title = "error_data"
            
             (id+","+table_id+","+src_id+","+table_title+","+table_source+","+table_data_hash+","+title+","+
                 stockcode+","+company+","+$type+","+time+","+industry_name+","+author+","+table_row_title+","+table_column_title)

            }
           
             else
             {
                val chid3= ArrayBuffer[String]()
               for (i <- 0 until arr1(3).length)
               {
                  val ch = arr1(3).charAt(i);
                 
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chid3 +=ch.toString()
        }
               }
               val chtitle6 = ArrayBuffer[String]()
                for (i <- 0 until arr1(6).length)
               {
                  val ch = arr1(6).charAt(i);
                  
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chtitle6 +=ch.toString()
        }
               }
                val chhash13 = ArrayBuffer[String]()
                   for (i <- 0 until arr1(13).length)
               {
                  val ch = arr1(13).charAt(i);
                  
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chhash13 +=ch.toString()
        }
               }
                   val chrow14 = ArrayBuffer[String]()
                      for (i <- 0 until arr1(14).length)
               {
                  val ch = arr1(14).charAt(i);
                  
                 if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
         chrow14 +=ch.toString()
        }
               } 
      
              
                   val chid33 = chid3.toString()
                   val chtitle66= chtitle6.toString()
                   val chhash1313 = chhash13.toString()
                   val chrow1414 = chrow14.toString()
            val chid333 = chid33.substring(12, chid33.length()-1).replaceAll(",", "").replaceAll(" ", "")
            val chtitle666 = chtitle66.substring(12, chtitle66.length()-1).replaceAll(",", "").replaceAll(" ", "")
           val chhash131313 =chhash1313.substring(12, chhash1313.length()-1).replaceAll(",", "").replaceAll(" ", "")
             val chrow141414 = chrow1414.substring(12, chrow1414.length()-1).replaceAll(",", "-").replaceAll(" ", "")
           
            val id = arr1(0)
            val table_id = arr1(1)
            val src_id = arr1(2)
            val table_title = chid333
            val table_source=arr1(4)
            val table_data_hash = arr1(5)
            val title = chtitle666
            val stockcode =arr1(7)
            val company = arr1(8)
            val $type= arr1(9)
            val time = arr1(10)
            val industry_name = arr1(11)
            val author =arr1(12)
            val table_row_title = chhash131313
            val table_column_title = chrow141414
            
             (id+","+table_id+","+src_id+","+table_title+","+table_source+","+table_data_hash+","+title+","+
                 stockcode+","+company+","+$type+","+time+","+industry_name+","+author+","+table_row_title+","+table_column_title)
             

             }
          }
      }
   
   
   
   
   //finace表RDD处理
   /* val rdd = RDD_id.map{
        x =>
          {
         val data_arr_1 = ArrayBuffer[String]()
         if (x.length()>60)
         {
         val y= x.substring(12, x.length()-1)
         val yy = y.split(",")
          
         if (yy.length ==9)
         {
           
           for (i <- 0 until yy.length)
           {
             data_arr_1 += yy(i)
           }
         }
         else if (yy.length >9)
         {
           val aa = yy.length -9
           data_arr_1 += "5881ec69cafd5b13a90f3139"
           data_arr_1 += "jc_5881ec69cafd5b13a90f3139"
          for ( i <- 0 until 6)
           {
             data_arr_1 += "errordata"
           }
           data_arr_1 += "1480089600"
         }
           else if (yy.length < 9)
         {
             val aa = 9- yy.length
           for (i <- 0 until yy.length)
           {
             data_arr_1 += yy(i)
           }
           for ( i <- 0 until aa-1)
           {
             data_arr_1 += "errordata"
           }
           data_arr_1 += "1483027200"
         }
         }
         else
         {
           val ss = "5881ec69cafd5b13a90f3138, jc_5881ec69cafd5b13a90f3138, jc_1202851341, 第十届监事会第六次（临时）会议决议公告, 监事会公告, 全新好, 000007, 住宿和餐饮业, 1480521600"
         val s1 = ss.split(",")
         for (i <- 0 until s1.length)
         {
           data_arr_1 += s1(i)
         }
         }
           val yy1 = data_arr_1.toString()
           val yy2 = yy1.substring(12, yy1.length()-1)
            yy2
          }
      }*/
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