package ALS_Recomment
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}  
import org.apache.hadoop.hbase.client.HBaseAdmin  
import org.apache.hadoop.hbase.mapreduce.TableInputFormat  

import org.apache.hadoop.hbase.client.HTable  
import org.apache.hadoop.hbase.client.Put  
import org.apache.hadoop.hbase.util.Bytes  
import org.apache.hadoop.hbase.io.ImmutableBytesWritable  
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat  
import org.apache.hadoop.mapred.JobConf  
import org.apache.hadoop.io._   
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.log4j.{Level, Logger}
object Rec_1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
   def main(args : Array[String]) {
   val sparkConf = new SparkConf().setAppName("recomment").setMaster("local")  
    val sc = new SparkContext(sparkConf)  
      
 // Load and parse the data
val data = sc.textFile("C:/Users/yzsun.abcft/Desktop/recomment/test.txt")
val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
  Rating(user.toInt, item.toInt, rate.toDouble)
})

// Build the recommendation model using ALS
// numBlocks是用于并行化计算的分块个数（设置为-1时 为自动配置）；

// rank是模型中隐性因子的个数；隐性因子指的是用户可以对产品进行评级的定义，例如通过打分，喜好或者停留时间
val rank = 10
val numIterations = 10  //迭代次数
// alpha是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准。
val alpha =0.01
val model = ALS.train(ratings, rank, numIterations, alpha)

// Evaluate the model on rating data
val usersProducts = ratings.map { case Rating(user, product, rate) =>
  (user, product)
}
val predictions =
  model.predict(usersProducts).map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }
val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
  ((user, product), rate)
}.join(predictions)
val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}.mean()
//ratesAndPreds.foreach(println)
print("打印结果*******************")
predictions.foreach(println)
println("方差 = " + MSE)
//为用户推荐产品
val users=data.map(_.split(",") match {    case Array(user, product, rate) => (user)  })
.distinct().collect() 
//users: Array[String] = Array(4, 2, 3, 1) 
users.foreach(  user => {       
  //依次为用户推荐商品   
  var rs = model.recommendProducts(user.toInt, numIterations)   
  var value = ""
  var key = 0
  //拼接推荐结果   
  rs.foreach(r => {      
    key = r.user     
    value = value + r.product + ":" + r.rating + ","      }
  )  
  //打印输出用户 推荐结果 预测值
  val valuearr = value.split(",")
    println(key.toString+"   " + valuearr(0))   
})

//对预测结果按预测的评分排序 
    predictions.collect.sortBy(_._2)
    //对预测结果 按用户进行分组，然后合并推荐结果
    predictions.map{ case ((user, product), rate) => (user, (product,rate) )}.groupByKey.collect
    
val formatedRatesAndPreds = ratesAndPreds
.map {   case ((user, product), (rate, pred)) => user + "," + product + "," + rate + "," + pred}
  
   formatedRatesAndPreds.foreach(println)
    sc.stop()
   }
}