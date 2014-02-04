package net.tqft.citationsearch

import scala.io.Source
import java.io.PrintStream
import java.io.FileOutputStream

object FixIndexApp extends App {
  val newIndex = scala.collection.mutable.Map[String, scala.collection.mutable.Set[Int]]()

  for ((t, ws) <- Search.index; s <- Search.tokenize(t); if MRIdentifier.unapply(s).isEmpty) {
    val set = newIndex.getOrElseUpdate(s, scala.collection.mutable.Set.empty)
    set ++= ws
  }

  val out = new PrintStream("terms.new", "UTF-8")
  for ((t, ws) <- newIndex) {
    out.println(t)
    out.println(ws.mkString(","))
  }
}