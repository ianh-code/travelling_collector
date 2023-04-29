import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class SearchNode(vertex: VertexId, profit: Int,
                      visited: Set[VertexId], path: List[VertexId],
                      capacity: Int, bound: Float = Float.PositiveInfinity) {

  def update_bound(newB: Float): SearchNode = {
    SearchNode(vertex, profit, visited, path, capacity, newB)
  }
}