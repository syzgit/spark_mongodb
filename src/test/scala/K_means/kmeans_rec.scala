package K_means
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
object kmeans_rec {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]){
    
  val sparkConf = new SparkConf().setAppName("recomment").setMaster("local")  
    val sc = new SparkContext(sparkConf)  
  //训练数据
  val data = sc.textFile("C:/Users/yzsun.abcft/Desktop/recomment/train.txt")
val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 5
val numIterations = 6
var clusterIndex = 0
val clusters = KMeans.train(parsedData, numClusters, numIterations)
/*val ks:Array[Int] = Array(1,2,3,4,5)
ks.foreach(cluster => {
 val model = KMeans.train(parsedData, cluster,6)
 val ssd = model.computeCost(parsedData)
 println("sum of squared distances of points to their nearest center when k=" + cluster + " -> "+ ssd)
})*/


clusters.clusterCenters.foreach(x => 
  {
    println("中心距离 " + clusterIndex + ":")
    println(x)
    clusterIndex += 1
  }
  
  )
  //测试数据
   val testdata = sc.textFile("C:/Users/yzsun.abcft/Desktop/recomment/test.txt")
   val parsedTestData = testdata.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
   parsedTestData.collect().foreach(testDataLine => { val predictedClusterIndex:
    Int = clusters.predict(testDataLine)
    println(testDataLine.toString + " 属于簇 " +
    predictedClusterIndex) })
// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)




println("方差= " + WSSSE)

// Save and load model

}
}