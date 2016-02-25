package net.tqft.citationsearch

object CitationSearchTestApp extends App {
  import argonaut._, Argonaut._
//  println(Search.query("morrison haagerup").asJson.spaces2)

//  println(Search.query("Perturbation theory, stability, boundedness and asymptotic behaviour for second order evolution equation in discrete time - Castro, A., Cuevas, C.").asJson.spaces2)
//  println(Search.query("A note on regular rings, kovacs").asJson.spaces2)
//  println(Search.query("503--518  Math. USSR Sbornik  72  Yu.V. Kuzmin and Ralph Stöhr Kovacs Homology of free abelianized extensions of groups").asJson.spaces2)
  
//  println(Search.query("index for subfactors").asJson.spaces2)
  
//  println(Search.query("On a tensor category for the exceptional Lie groups - Cohen, A.M. and de Man, R. "))
//  println(Search.query("Some indications that the exception groups form a series - Cohen, A. M."))
  println(Search.query("Equations Différentielles à points singuliers réguliers deligne"))
  
//    val s = "Subfactors with index at most 5, part 1: the odometer, morrison, snyder, cmp"
//    for (q <- s.reverse.tails.toSeq.reverse.map(_.reverse)) {
//      println(Search.query(q))
//    }
}