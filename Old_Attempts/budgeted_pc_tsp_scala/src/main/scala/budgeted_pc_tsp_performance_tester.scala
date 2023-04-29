import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object budgeted_pc_tsp_performance_tester extends App {
  val spark = SparkSession.builder.master("local[1]").appName("BPC_TSP").getOrCreate

  val graph = BPC_TSP_Graph(spark)

  // Testing brute force
  val bfSolve = new BranchBoundSolver(graph)
  val bfStartTime = System.nanoTime
  val (bfProfit: Int, bfPath: List[VertexId]) = bfSolve.solve()
  val bfDuration = (System.nanoTime - bfStartTime) / 1e9d

  println(s"Brute force took $bfDuration seconds. Results were:")
  println(s"Profit: $bfProfit")
  println(s"Path: ${bfPath.mkString(" -> ")}")

  // Testing branch and bound
  val bbSolve = new BranchBoundSolver(graph, BoundTest.inbound_sum)
  val bbStartTime = System.nanoTime
  val (bbProfit: Int, bbPath: List[VertexId]) = bbSolve.solve()
  val bbDuration = (System.nanoTime - bbStartTime) / 1e9d

  println(s"Branch and bound took $bbDuration seconds. Results were:")
  println(s"Profit: $bbProfit")
  println(s"Path: ${bbPath.mkString(" -> ")}")

  val bbspSolve = new ShortPathBBSolver(graph, BoundTest.inbound_sum)
  val bbspStartTime = System.nanoTime
  val (bbspProfit: Int, bbspPath: List[VertexId]) = bbspSolve.solve()
  val bbspDuration = (System.nanoTime - bbspStartTime) / 1e9d

  println(s"Branch and bound took $bbspDuration seconds. Results were:")
  println(s"Profit: $bbspProfit")
  println(s"Path: ${bbspPath.mkString(" -> ")}")
}

//val t1 = System.nanoTime
//
///* your code */
//
//val duration = (System.nanoTime - t1) / 1e9d