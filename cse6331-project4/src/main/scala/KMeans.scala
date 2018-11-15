import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object KMeans {
type Point = (Double,Double)
var centroids: Array[Point] = Array[Point]()
def find_centroid(point:Point,cent:Array[Point]):Point=
  {   
    var distance:Double=1111.00
    var CentroidMain: Point=(0,0)
    for(Centroid_point<-cent)
    {  
      val x1 = Centroid_point._1
      val y1 = Centroid_point._2
      val x2 = point._1
      val y2 = point._2
      val temp1 = x1 - x2
      val temp2 = temp1 * temp1
      val temp3 = y1 - y2
      val temp4 = temp3 * temp3
      val temp5 = temp2 + temp4
      var Distance = scala.math.sqrt(temp5)
      if(Distance < distance)
      {
        distance = Distance
        CentroidMain = Centroid_point
      }
    }
    return CentroidMain
  }
  def main(args: Array[ String ])
  {
    val conf = new SparkConf().setAppName("Kmeans")
    val sc = new SparkContext(conf)
    val Centroids = sc.textFile(args(1)).collect
    centroids = Centroids.map( line => { val a = line.split(",")
                                           (a(0).toDouble,a(1).toDouble) } )
    val input_points = sc.textFile(args(0)).collect
    val indi_points = input_points.map( line => { val b = line.split(",")
                                            (b(0).toDouble,b(1).toDouble) } )
    for ( i <- 1 to 5 )
    {
       val fff = indi_points.map(X=>{((find_centroid(X,centroids)),(X._1,X._2))})
       val rr = sc.parallelize(fff)
       val counter = rr.mapValues(value=>(value,1))
       val centroids2  = counter.reduceByKey{case((point1,iterator1),(point2,iterator2))=>(((point1._1+point2._1),(point1._2+point2._2)),(iterator1+iterator2))}.mapValues{case(final_point,iterator)=>(final_point._1/iterator, final_point._2/iterator) }.collect
       centroids = centroids2.map(X=>(X._2))
    }
    centroids.foreach(X=>println("("+X._1+","+X._2+")"))
  }
}
