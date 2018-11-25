package streaming

/**
  * biased coin
  */
object Coin extends Enumeration {
  val Head, Tail = Value
  def flip(bias: Double): Coin.Value = if (Math.random < bias) Head else Tail
}