import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.mutable

object BoundTest {

  /////////////////
  // Inbound Sum //
  /////////////////

  private def getMinInbound(btgraph: BPC_TSP_Graph, v: VertexId): Int = {
    val inboundEdges = btgraph.g.edges filter { e => e.srcId == v }
    inboundEdges.collect.minBy(e => e.attr).attr
  }

  def inbound_sum(btgraph: BPC_TSP_Graph, sn: SearchNode): Float = {
    val mbound: (BPC_TSP_Graph, VertexId) => Int = getMinInbound
    val pq: mutable.PriorityQueue[(VertexId, Int)] =
      new mutable.PriorityQueue[(VertexId, Int)]()(Ordering.by({case (i, w) => w / mbound(btgraph, i)}))
    val vertices = (btgraph.g.vertices filter {case (id: VertexId, _: Int) => !sn.visited(id)}).collect

    vertices foreach {x => pq.enqueue(x)}
    inbound_sum(btgraph, pq, sn.capacity, 0)
  }

  @tailrec
  private def inbound_sum(btgraph: BPC_TSP_Graph, pq: mutable.PriorityQueue[(VertexId, Int)], cap: Int, acc: Float): Float = {
    if (pq.isEmpty || cap == 0) {
      acc
    } else {
      val next_vert = pq.dequeue()
      val minInbound = getMinInbound(btgraph, next_vert._1)
      if (minInbound > cap) {
        val fractionMultiplier: Float = cap / minInbound
        val lastProf: Float = next_vert._2 * fractionMultiplier
        acc + lastProf
      } else {
        inbound_sum(btgraph, pq, cap - minInbound, acc + next_vert._2)
      }
    }
  }


}
