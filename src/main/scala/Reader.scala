package streaming

import java.io._
import java.nio.charset.CodingErrorAction

import scala.collection.mutable._

class Reader(filename: String = "./dataset/out.moreno_zebra_zebra") {
  /**
    * primary constructor in scala:
    * fetches file as a stream
    */

  var graph: ListBuffer[ListBuffer[Int]] = ListBuffer()

  implicit val codec = io.Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.IGNORE)
  codec.onUnmappableCharacter(CodingErrorAction.IGNORE)

  for (line <- scala.io.Source.fromFile(filename).getLines) {
    if (!line.startsWith("%")) {
      var es: ListBuffer[Int] = line.split("\\s").map(_.toInt).to[ListBuffer]
      graph += es
    }
  }
}
