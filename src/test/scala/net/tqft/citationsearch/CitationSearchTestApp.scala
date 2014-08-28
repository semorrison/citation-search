package net.tqft.citationsearch

object CitationSearchTestApp extends App {
  import argonaut._, Argonaut._
  println(Search.query("morrison haagerup").asJson.spaces2)

  println(Search.query("Perturbation theory, stability, boundedness and asymptotic behaviour for second order evolution equation in discrete time - Castro, A., Cuevas, C.").asJson.spaces2)
  
  //  val s = "Subfactors with index at most 5, part 1: the odometer, morrison, snyder, cmp"
  //  for (q <- s.reverse.tails.toSeq.reverse.map(_.reverse)) {
  //    Search.query(q)
  //  }
}