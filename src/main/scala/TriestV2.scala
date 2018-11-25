package streaming

import scala.collection.mutable._

class TriestV2(graphRecords: ListBuffer[ListBuffer[Int]], M: Int = 90) {
  var t: Int = 0 // timestamp
  var T: Double = 0 // triangles count
  var S: Map[Int, ListBuffer[Int]] = HashMap() // samples
  var E: ListBuffer[ListBuffer[Int]] = ListBuffer() // edges
  var triangles: Map[Int, Double] = HashMap()

  /**
    * main constructor: loop though all graph records as an incoming stream
    */
  (0 until graphRecords.size) foreach { i =>
    t += 1
    updateCounters(EdgeOperator.Plus, graphRecords(i))
    if (sampleEdge(graphRecords(i))) {
      appendEdge(graphRecords(i))
    }
  }

  /**
    * this function is to implement reservoir sampling
    * @param edge the new coming edge
    * @param t is the timestamp
    * @return
    */
  def sampleEdge(edge: ListBuffer[Int]): Boolean = {
    if (t <= M)
      return true
    else if(Coin.flip(M.toFloat / t.toFloat) == Coin.Head) {
      val randomEdge: ListBuffer[Int] = E((Math.random * E.size).toInt)
      removeEdge(randomEdge)
      return true
    }
    return false
  }

  /**
    * Add edge to the sampling list
    * @param edge
    * @return
    */
  def appendEdge(edge: ListBuffer[Int]) = {
    var edgeNeighbors: ListBuffer[ListBuffer[Int]] = initEdgeNeighbors(edge)
    edgeNeighbors(0) += edge(1)
    edgeNeighbors(1) += edge(0)
    S.put(edge(0), edgeNeighbors(0))
    S.put(edge(1), edgeNeighbors(1))
    E += edge
  }

  /**
    * this function is to update local and global counter
    * see "the TRIEST: Counting Local and Global Triangles in Fully-Dynamic Streams with Fixed Memory Size"
    * this version of algorithm uses performs a weighted increase of the counters using
    * η(t) = max{1, (t − 1)(t − 2)/(M (M − 1))}
    * @param edgeOperator
    * @param edge
    */
  def updateCounters(edgeOperator: EdgeOperator.Value, edge: ListBuffer[Int]) = {
    var edgeNeighbors: ListBuffer[ListBuffer[Int]] = initEdgeNeighbors(edge)
    var neighborsIntersection: ListBuffer[Int] = edgeNeighbors(0).intersect(edgeNeighbors(1))
    val weightedIncrease: Double = Math.max(1.0, ((t - 1) * (t - 2)) / (M * (M - 1)))
    (0 until neighborsIntersection.size) foreach { i =>
      if(edgeOperator == EdgeOperator.Plus) {
        T += weightedIncrease
        addOrUpdate(triangles, neighborsIntersection(i), weightedIncrease,v => v + weightedIncrease)
        addOrUpdate(triangles, edge(0), weightedIncrease, v => v + weightedIncrease)
        addOrUpdate(triangles, edge(1), weightedIncrease, v => v + weightedIncrease)
      } else {
        T -= weightedIncrease
        addOrUpdate(triangles, neighborsIntersection(i), weightedIncrease, v => v - weightedIncrease)
        addOrUpdate(triangles, edge(0), weightedIncrease, v => v - weightedIncrease)
        addOrUpdate(triangles, edge(1), weightedIncrease, v => v - weightedIncrease)
      }
    }
  }

  /**
    * remove edge from S, the sampling set
    * @param edge edge to remove from S
    */
  def removeEdge(edge: ListBuffer[Int]): Unit = {
    S -= edge(0)
    S -= edge(1)
    E -= edge
  }

  /**
    * helper function for used in both appendEdge function and updateCounters
    * to initialize the neighbors of a node
    * @param edge
    * @return
    */
  private def initEdgeNeighbors(edge: ListBuffer[Int]) = {
    var edgeNeighbors: ListBuffer[ListBuffer[Int]] = ListBuffer.fill(edge.size)(ListBuffer())
    (0 until edge.size) foreach { i =>
      val value = if (S.keySet.exists(_ == edge(i))) S(edge(i)) else null
      if (value != null) {
        edgeNeighbors(i) = value
      }
    }
    edgeNeighbors
  }

  /**
    * helper function that used in updateCounters
    * to check whether the kv is there or not
    * if there, do change the value, otherwise create new kv pair where v = 1
    * @param m
    * @param k
    * @param f
    */
  private def addOrUpdate(m: collection.mutable.Map[Int, Double], k: Int, weightedIncrease: Double,f: Double => Double) {
    m.get(k) match {
      case Some(e) => m.update(k, f(e))
      case None    => m.put(k,weightedIncrease)
    }
  }

  /**
    * this function calculates the estimation
    * the formula is: ξ(t) = T * max(1, (t(t−1)(t−2)) / (M(M−1)(M−2)))
    * @return estimation
    */
  def calculateEstimation: Double = {
    T
  }
}
