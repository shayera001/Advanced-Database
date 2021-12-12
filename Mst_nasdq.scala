package main.scala
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
object Mst_nasdq {
   type EdgeType = ((VertexId, VertexId), Double)

  def minSpanningTree[VD : ClassTag] (g : Graph[VD, Double]) = {

   //initialize the set of edges
    var graph = g.mapEdges(e => (e.attr, false))
    for (i <- 1L to g.vertices.count - 1) {
      val unavailableEdges =
        graph.outerJoinVertices(
          graph.subgraph(_.attr._2)
          .connectedComponents.vertices
        )((vid, vd, cid) => (vd, cid)).subgraph(et =>
          // an edge is spanned iff both of its endpoints are contained in the same tree
          (et.srcAttr._2, et.dstAttr._2) match {
            case (Some(c1), Some(c2)) => c1 == c2
            case _ => false}).edges
        // convert edges
        .map(e => ((e.srcId, e.dstId), e.attr))

      // find the smallest edge 
      val smallestEdge =
        graph.edges
        // convert edges
        .map(e => ((e.srcId, e.dstId), e.attr))
        // join the unavailable edges 
        .leftOuterJoin(unavailableEdges)
        // edge isn’t already in the result set of edges
        .filter(x => !x._2._1._2 && x._2._2.isEmpty)
        // convert edges 
        .map(x => (x._1, x._2._1._1))
        // select the minimal edge from the available edges
        .min()(new Ordering[EdgeType]() {
          override def compare (a : EdgeType, b : EdgeType) = {
            val r = Ordering[Double].compare(a._2, b._2)
            if (r != 0) r
            else // make the result
              Ordering[Long].compare(a._1._1, b._1._1)
          }
        })

      // add the smallest edge 
      graph = graph.mapTriplets(et => (et.attr._1,
        et.attr._2 || (et.srcId == smallestEdge._1._1 && et.dstId == smallestEdge._1._2)))
    }
    // remove the augmented attribute
    graph.subgraph(_.attr._2).mapEdges(_.attr._1)
  }
 def toGexf[VD,ED](g:Graph[VD,ED]) ={
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "    <nodes>\n" +
    g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
                        v._2 + "\" />\n").collect.mkString +
    "    </nodes>\n" +
    "    <edges>\n" +
    g.edges.map(e => "      <edge source=\"" + e.srcId +
                     "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
                     "\" />\n").collect.mkString +
    "    </edges>\n" +
    "  </graph>\n" +
    "</gexf>"
}
 def time[A](f: => A) = {
  val s = System.nanoTime
  val ret = f
  println("time: " + (System.nanoTime - s) / 1e9 + " seconds")
  ret
}
  def main(args:Array[String]){
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Mst_nasdq"))
    val dist3 = sc.textFile("nasdq1symbols.txt") 
val verts3 = dist3.map(_.split(",")(0)).distinct. 
    map(x => (x.hashCode.toLong,x)) 
val edges3 = dist3.map(x => x.split(",")). 
    map(x => Edge(x(0).hashCode.toLong,
                 x(1).hashCode.toLong,  
                 x(2).toDouble)) 
val distg3 = Graph(verts3, edges3) 
val mst3 = minSpanningTree(distg3)
println(time(minSpanningTree(distg3)))
val pw3 = new java.io.PrintWriter("nasdq1symbols.gexf") 
pw3.write(toGexf(mst3)) 
pw3.close  

}
}