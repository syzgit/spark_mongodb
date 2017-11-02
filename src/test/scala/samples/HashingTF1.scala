package samples
import scala.collection.mutable
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
class HashingTF1 (val numFeatures: Int) extends Serializable{
  def this() = this(1 << 20)

  def nonNegativeMod(x: Int, mod: Int): Int = { //根据 numFeatures 设置的哈希表容量，来设定索引号
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
  def indexOf(term: Any): Int = nonNegativeMod(term.##, numFeatures) //根据分词来生成索引号

  def transform(document: Iterable[_]): Vector = {
    //每篇文章一个hash表，记录每篇文章中的词频
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = indexOf(term)
      //map中的getOrElse(i, 0.0)函数表示如果找到i位置的值就返回，否则就默认为0.0
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)//注意这里有加1计数操作
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }
  def transform[D <: Iterable[_]](dataset: RDD[D]): RDD[Vector] = {
    dataset.map(this.transform)
  }
  
}