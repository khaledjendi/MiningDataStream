package streaming

object Main {
  def main(args: Array[String]): Unit = {
    println()

    val reader = new Reader()
    val correctTriangleCount = 312
    time {
      println("triestV1 >>>")
      val triestV1: TriestV1 = new TriestV1(reader.graph)
      val estimationTriangleCount = triestV1.calculateEstimation
      val errorPercentage = (Math.abs(correctTriangleCount.toDouble - estimationTriangleCount) / correctTriangleCount.toDouble) * 100
      println(s"Estimation of triangles is: ${estimationTriangleCount}")
      println(s"Error percentage is: ${BigDecimal(errorPercentage).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble}%")
    }

    println("\n=============================\n")
    time {
      println("triestV2 >>>")
      val triestV2: TriestV2 = new TriestV2(reader.graph)
      val estimationTriangleCount = triestV2.calculateEstimation
      val errorPercentage = (Math.abs(correctTriangleCount.toDouble - estimationTriangleCount) / correctTriangleCount.toDouble) * 100
      println(s"Estimation of triangles is: ${estimationTriangleCount}")
      println(s"Error percentage is: ${BigDecimal(errorPercentage).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble}%")
    }
  }

  def time[R](block: => R): R = {
    // http://biercoff.com/easily-measuring-code-execution-time-in-scala/
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val diff = (t1 - t0)
    println(s"Elapsed time is: ${diff.toString} ns ≈ ${(diff / 1000000).toString} ms")
    result
  }
}