import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.mutable

class BranchBoundSolver(btg: BPC_TSP_Graph, f: (BPC_TSP_Graph, SearchNode) => Float = (_, _) => Float.PositiveInfinity) {
  private val btgraph: BPC_TSP_Graph = btg
  private val root: SearchNode = SearchNode(vertex=btgraph.startNode, profit=getVertexProfit(btgraph.startNode),
    visited=Set(btgraph.startNode), path=List(btgraph.startNode),
    capacity=btgraph.capacity, bound=Float.PositiveInfinity)

  def getVertexProfit(v: VertexId): Int = {
    val filtered = btgraph.g.vertices filter {case (vert: VertexId, _) => vert == v}
    val (_, w) = filtered.first
    w
  }

  private def constructSearchNode(curNode: SearchNode, toTraverse: Edge[Int]): SearchNode = {
    val newCap = curNode.capacity - toTraverse.attr
    val newVert = toTraverse.dstId
    val newVisited = curNode.visited + newVert
    val newPath = newVert :: curNode.path
    val newProfit = getVertexProfit(newVert)
    val initial_sn = SearchNode(newVert, newProfit, newVisited, newPath, newCap)
    val newBound = f(btg, initial_sn)
    initial_sn.update_bound(newBound)
  }

  def solve(): (Int, List[VertexId]) = {
    val pq: mutable.PriorityQueue[SearchNode] = new mutable.PriorityQueue[SearchNode]()(Ordering.by({case n: SearchNode => n.bound}))
    pq.enqueue(root)

    val (a, b) = solve_body(pq, (root.profit, root.path))
    (a, b.reverse)
  }

  @tailrec
  private def solve_body(pq: mutable.PriorityQueue[SearchNode], curBest: (Int, List[VertexId])): (Int, List[VertexId]) = {
    if (pq.isEmpty) {
      curBest
    } else {
      val sn = pq.dequeue()
      if (sn.capacity < 0 || sn.bound < curBest._1) {
        solve_body(pq, curBest)
      } else {
        val nextCurBest = if (sn.profit > curBest._1) (sn.profit, sn.path) else curBest
        val outgoing = btgraph.g.edges filter { e => e.srcId == sn.vertex }
        val newSNs = outgoing map { e => constructSearchNode(sn, e) }
        newSNs.collect foreach { nsn => pq.enqueue(nsn) }
        solve_body(pq, nextCurBest)
      }

    }
/*    if (sn.capacity < 0 || sn.bound < currentBestProfit ||) {
      (sn.profit, sn.path)
    } else {
      if (sn.profit > currentBestProfit) currentBestProfit = sn.profit.asInstanceOf[AtomicInteger] else _
      val outgoing = btgraph.g.edges filter { e => e.srcId == sn.vertex }
      val newSNs = outgoing map { e => constructSearchNode(sn, e) }

    }*/
  }
}


//class BacktrackSolver(btg: BPC_TSP_Graph, f: (BPC_TSP_Graph, SearchNode) => Float = (_, _) => Float.PositiveInfinity) {
//  private val btgraph: BPC_TSP_Graph = btg
//  private val root: SearchNode = SearchNode(vertex=btgraph.startNode, profit=getVertexProfit(btgraph.startNode),
//    visited=Set(btgraph.startNode), path=List(btgraph.startNode),
//    capacity=btgraph.capacity, bound=Float.PositiveInfinity)
//  private var currentBestProfit: AtomicInteger = root.profit.asInstanceOf[AtomicInteger]
//
//  private def getVertexProfit(v: VertexId): Int = {
//    val filtered = btgraph.g.vertices filter {case (vert: VertexId, _) => vert == v}
//    val (_, w) = filtered.first
//    w
//  }
//
//  private def constructSearchNode(curNode: SearchNode, toTraverse: Edge[Int]): SearchNode = {
//    val newCap = curNode.capacity - toTraverse.attr
//    val newVert = toTraverse.dstId
//    val newVisited = curNode.visited + newVert
//    val newPath = newVert::curNode.path
//    val newProfit =  getVertexProfit(newVert)
//    val initial_sn = SearchNode(newVert, newProfit, newVisited, newPath, newCap)
//    val newBound = f(btg, initial_sn)
//    initial_sn.update_bound(newBound)
//  }
//
//  def solve(): (Int, List[VertexId]) = {
//    val (a, b) = solve_body()
//    (a, b.reverse)
//  }
//
//  private def solve_body(sn: SearchNode = root): (Int, List[VertexId]) = {
//    if (sn.capacity < 0 || sn.bound < currentBestProfit) {
//      (sn.profit, sn.path)
//    } else {
//      val _ = if (sn.profit > currentBestProfit) currentBestProfit = sn.profit.asInstanceOf[AtomicInteger] else _
//      val outgoing = btgraph.g.edges filter { e => e.srcId == sn.vertex }
//      val newSNs = outgoing map { e => constructSearchNode(sn, e) }
//      val results = newSNs map { nsn => solve(nsn) }
//      results reduce { (a, b) => if (a._1 > b._1) a else b }
//    }
//  }
//}


