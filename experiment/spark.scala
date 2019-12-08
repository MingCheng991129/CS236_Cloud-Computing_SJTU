import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object Spark_code {
  
  // this part defines pagerank and sssp


   // sssp
  val SSSP = (graph: Graph[Int,Int], dist_id: Int) => {

    val initial_g = graph.mapVertices((id, _) =>

      if (dist_id == id) 0.0 
      else Double.PositiveInfinity)

    // use Pregel to implement sssp
    initial_g.pregel(Double.PositiveInfinity)(

      (id, dist, new_dist) => math.min(dist, new_dist),

      triplet => {

        if (triplet.srcAttr + triplet.attr >= triplet.dstAttr) 
        {
          Iterator.empty
        } 

        else 
        {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        }
      },

      (a, b) => math.min(a, b)
    )
  }

  // pagerank


  // without random reset probability !!!!!!!!
  


  val PageRank = (graph: Graph[Int,Int])=>{

    var initial_g = graph.mapVertices((id,_) => (1.0,0))

    // use aggregateMessages

    // outdegree
    val degree = initial_g.aggregateMessages[(Double,Int)](triplet => {triplet.sendToSrc((1.0,1))}, (a, b)=>(1.0, a._2 + b._2))
    
    initial_g = Graph(degree, initial_g.edges)
    // loop for 100 times to converge
    for(i <- 1 to 100) {

      // indegree
      initial_g = Graph(initial_g.aggregateMessages[(Double, Int)](triplet => {

        triplet.sendToDst((triplet.srcAttr._1 / triplet.srcAttr._2, 0));
        triplet.sendToSrc((0.0, 1))
      },
        (a, b) => (a._1 + b._1, a._2 + b._2)), initial_g.edges)
    }

    initial_g
  }


  

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("GraphX API").master("local").getOrCreate()
    // load wiki data
    val wiki_data = GraphLoader.edgeListFile(spark.sparkContext, "wiki-Vote.txt")

    // print start information
    println("SSSP starts now:")

    val sssp = SSSP(wiki_data, 172028)

    println(sssp.vertices.collect.mkString("\n"))
    println("SSSP ends.")   // print end information of sssp


    // load wiki data
    val wiki = GraphLoader.edgeListFile(spark.sparkContext, "wiki-Vote.txt")

    val page_rank = PageRank(wiki)
    // sort for 20 candidates
    page_rank.vertices.sortBy(x => (-x._2._1)).take(20).foreach(println)  

    // print end information of pagerank 
    println("PageRank ends.")   



    spark.stop()

  }
}