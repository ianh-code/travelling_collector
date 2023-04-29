import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.mutable


class ShortPathBBSolver(btg: BPC_TSP_Graph,
                        f: (BPC_TSP_Graph, SearchNode) => Float =
                        (_, _) => Float.PositiveInfinity) extends BranchBoundSolver(btg, f) {
  private val btgraph: BPC_TSP_Graph = btg
  private val root: SearchNode = SearchNode(vertex = btgraph.startNode, profit = getVertexProfit(btgraph.startNode),
    visited = Set(btgraph.startNode), path = List(btgraph.startNode),
    capacity = btgraph.capacity, bound = Float.PositiveInfinity)

  private type ShortPDatum = (VertexId, (Float, List[List[VertexId]]))
  private type ShortPData = RDD[ShortPDatum]
  private type ShortPRawJoined = (VertexId, (Option[Int], (Float, List[List[VertexId]])))
  private type PathList = List[List[VertexId]]
  private val inf: Float = Float.PositiveInfinity
  private val emptyVertList: List[VertexId] = Nil
  private val emptyPathList: List[List[VertexId]] = Nil

  ///////////////////
  // Shortest Path //
  ///////////////////
  // Build table of shortest paths from
  private def shortestPath(srcNode: VertexId): ShortPData = {
    val v_pairs: ShortPData = btg.g.vertices map { case (v, _) =>
      (v, (if (v == srcNode) (0, emptyPathList) else (inf, emptyPathList)))
    }

    val raw_vertices = btg.g.vertices.map({case (v, _) => v}).collect

    shortestPath(srcNode, Nil, raw_vertices.toSet, v_pairs)
  }

  @tailrec
  private def shortestPath(currentNode: VertexId,
                           currentPaths: PathList,
                           unvisited: Set[VertexId],
                           spTable: ShortPData): ShortPData = {

    // Get all unvisited adjacent vertices and the lengths of their inbound edges
    val outgoingUnvisited = btg.g.edges filter { e => e.srcId == currentNode && unvisited.contains(e.srcId) }
    val adjUnv = outgoingUnvisited map { e => (e.dstId, e.attr) }
    // Update table with new minimum lengths
    val joined = adjUnv.rightOuterJoin(spTable)
    val new_table = joined map { row => cleanTableRow(currentPaths, row) }

    // Mark current node as visited
    val new_unvisited = unvisited - currentNode

    if (new_unvisited.isEmpty) { // When all nodes are visited, we're done! Just need to reverse constructed paths
      // and return the table
      new_table map { case (id, (d, plist)) => (id, (d, reversePathsInList(plist))) }
    } else { // Otherwise, traverse to the nearest unvisited node, collecting inbound path,
      // and repeat with those and updated table
      val closestRow = new_table reduce { (a, b) => getMinStructure(a, b) }
      val closestPath = closestRow._2._2
      val closestVertex = closestRow._1
      shortestPath(closestVertex, closestPath, new_unvisited, new_table)
    }
  }

  private def getMinStructure(a: ShortPDatum,
                              b: ShortPDatum): ShortPDatum = {
    val (_, (da, _)) = a
    val (_, (db, _)) = b
    if (da < db) a else b
  }

  private def cleanTableRow(cnPaths: PathList,
                            x: ShortPRawJoined): ShortPDatum = {
    val (vert, (dist_unknown, (best_dist, best_paths))) = x
    if (dist_unknown.isEmpty) {
      (vert, (best_dist, best_paths))
    } else {
      val Some(new_dist) = dist_unknown
      val updated_paths = update_pathlist(cnPaths, vert, Nil)
      if (new_dist < best_dist) {
        (vert, (new_dist, updated_paths))
      } else if (new_dist == best_dist) {
        (vert, (best_dist, best_paths ++ updated_paths))
      } else {
        (vert, (best_dist, best_paths))
      }
    }
  }

  private def reversePathsInList(plist: PathList): PathList = {
    plist map { p => p.reverse }
  }

  @tailrec
  private def update_pathlist(cnPaths: PathList,
                              nextVert: VertexId,
                              acc: PathList): PathList = {
    val head :: tail = cnPaths
    val updP = head :+ nextVert
    cnPaths match {
      case Nil => acc
      case _ => update_pathlist(tail, nextVert, updP :: acc)
    }
  }

  ///////////////////////
  // End Shortest Path //
  ///////////////////////

  ///////////
  // Solve //
  ///////////

  private def constructSearchNodes(curNode: SearchNode, pathsRow: ShortPDatum): List[SearchNode] = {
    val (vid, (dist, plist)) = pathsRow

    val newCap = curNode.capacity - dist
    val newVert = vid

    val newVisitedSets = plist map { p => curNode.visited ++ p }
    val newPaths = plist map { p => curNode.path ++ p }

    val filteredPaths = plist map { p => p filter { v => !curNode.visited.contains(v) } }
    val unaggregatedProfits = filteredPaths map { p => p map { v => getVertexProfit(v) } }
    val newProfits = unaggregatedProfits map { p => p.sum }

    val bundled = newVisitedSets.lazyZip(newPaths).lazyZip(newProfits).toList
    val initial_newNodes = bundled map { case (vSet, pth, prf) => SearchNode(newVert, prf, vSet, pth, newCap.asInstanceOf[Int]) }
    val newBounds = initial_newNodes map { n => f(btgraph, n) }
    (initial_newNodes zip newBounds) map { case (inn, nb) => inn.update_bound(nb) }
  }

  override def solve(): (Int, List[VertexId]) = {
    val pq: mutable.PriorityQueue[SearchNode] = new mutable.PriorityQueue[SearchNode]()(Ordering.by({ case n: SearchNode => n.bound }))
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
        val shortestTable = shortestPath(sn.vertex)
        val newSNs = shortestTable flatMap { p => constructSearchNodes(sn, p) }
        newSNs foreach { n => pq.enqueue(n) }

        solve_body(pq, nextCurBest)
      }
    }
  }
}