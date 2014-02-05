package net.tqft.citationsearch

object SearchApp extends App {
 val s = "Subfactors with index at most 5, part 1: the odometer, morrison, snyder, cmp"
   while(true) {
  for (q <- s.reverse.tails.toSeq.reverse.map(_.reverse)) {
    println(q)
    println(Search.query(q))
  }
   }
}