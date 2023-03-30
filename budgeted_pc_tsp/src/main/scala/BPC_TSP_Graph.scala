import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.io.StdIn.readLine

case class BPC_TSP_Graph(s: SparkSession, files: (String, String) = ("","")) {
  val spark: SparkSession = s
  val (g: Graph[Int, Int], capacity: Int, startNode: VertexId) = build_graph(files)

  private def build_graph(files: (String, String)): (Graph[Int, Int], Int, VertexId) = {
    if (files == ("","")) {
      buildGraphFromPrompts
    } else {
      buildGraphFromPrompts
    }
  }

  private def buildGraphFromPrompts: (Graph[Int, Int], Int, VertexId) = {
    val (numNodes, unconnected, pRange, ewRange) = getGraphBuildParams
    val g = rConstructGraph(numNodes, unconnected, pRange, ewRange)
    println("Graph constructed!")
    val minsum = getMinInboundSum(g)
    println(s"The sum of all minimum inbound edges is $minsum")
    val (cap, startV) = getGraphMeta(numNodes)
    (g, cap, startV)
  }

  private def getGraphMeta(numNodes: Int): (Int, VertexId) = {
    val rand = new scala.util.Random
    print("What is the graph's capacity? => ")
    val cap = readLine().toInt
    val startV = rand.nextInt(numNodes).toInt
    println(s"Starting on node $startV")
    (cap, startV)
  }

  private def getGraphBuildParams: (Int, Int, Int, Int) = {
    print("Please enter the number of nodes => ")
    val numNodes = readLine().toInt
    print("Please enter the percentage of nodes pairs with no edges between them => ")
    val unconnected = readLine().toInt
    print("Please enter the prize range => ")
    val pRange = readLine().toInt
    print("Please enter the edge weight range => ")
    val ewRange = readLine().toInt
    (numNodes, unconnected, pRange, ewRange)
  }

  private def rConstructGraph(numNodes: Int, unconnected: Int,
                             pRange: Int, ewRange: Int): Graph[Int, Int] = {
    println("Stage A")
    val prizes = rConstructPrizes(numNodes, pRange, Nil)
    println("Stage A1")
    val rawEdges = rConstructEdges(numNodes, unconnected, ewRange)
    println("Stage B")
    val lVertices: List[(VertexId, Int)] = prizes.zipWithIndex.map{ case (e, i) => (i, e)}
    println("Stage B1")
    val lEdges = rawEdges map {case (src, dest, weight) => Edge(src, dest, weight)}
    println("Stage C")
    val vertices = lVertices.toSeq
    println("Stage C1")
    val edges = lEdges.toSeq
    println("Stage D")
    val rddVertices: RDD[(VertexId, Int)] = spark.sparkContext.parallelize(vertices)
    println("Stage D1")
    val rddEdges: RDD[Edge[Int]] = spark.sparkContext.parallelize(edges)
    println("Stage D2")
    Graph(rddVertices, rddEdges)
  }

  @tailrec
  private def rConstructPrizes(numNodes: Int, pRange: Int, accum: List[Int]): List[Int] = {
    val rand = new scala.util.Random
    if (numNodes <= 0) {
      accum.toSeq
    } else {
      val current = rand.between(1, pRange + 1)
      rConstructPrizes(numNodes - 1, pRange, current::accum)
    }
  }

  private def rConstructEdges(nodeLim: Int, unconnected: Int, maxWeight: Int):  List[(VertexId, VertexId, Int)] = {
    val rand = new scala.util.Random
    val unfiltered = for ( s <- 1 to nodeLim ; d <- 1 to nodeLim) yield {
      if (s == d) {
        (s.asInstanceOf[VertexId], d.asInstanceOf[VertexId], -1)
      } else if (rand.between(1, 101) <= unconnected) {
        (s.asInstanceOf[VertexId], d.asInstanceOf[VertexId], rand.between(1, maxWeight + 1))
      } else {
        (s.asInstanceOf[VertexId], d.asInstanceOf[VertexId], -1)
      }
    }
    val unfList = unfiltered.toList
    unfList.filter({case (_, _, w) => w >= 0})
  }

  private def getMinInboundSum(g: Graph[Int, Int]): Int = {
    val inb = g.vertices.map({ case (vid, _) => g.edges.filter(e => e.dstId == vid).collect })
    val minb = inb.map(edges => edges.reduce((a, b) => if (a.attr < b.attr) a else b))
    minb.map(e => e.attr).reduce((a, b) => a + b)
  }

//  private def build_graph(vertexFile: String, edgeFile: String): Graph[Int, Int] = {
//    val vertices: RDD[(VertexId, Int)] = buildVertsFromCSV()
//    val edges: RDD[Edge[Int]] = buildEdgesFromCSV()
//    Graph(vertices, edges)
//  }
//
//  private def buildVertsFromCSV(vFile: String): RDD[(VertexId, Int)] = {
//    val raw_read = spark.sparkContext.textFile(vFile)
//    val split_raw = raw_read.map(line => line.split(","))
//    split_raw.map(a => a collect {case Array(i, w) => Tuple2(i, w)})
//  }
//
//  private def buildEdgesFromCSV(eFile: String): RDD[Edge[Int]] = {
//    val raw_read = spark.sparkContext.textFile(eFile)
//    raw_read.map(line => line.split(","))
//  }
}
