package net.tqft.citationsearch

import scala.io.Source
import java.io.PrintStream
import java.io.FileOutputStream

object FixIndexApp extends App {
  val oldIndex = Source.fromFile("terms").getLines.grouped(2).map({ pair =>
    pair(0) -> pair(1).split(",").map(_.toInt).toSet
  })

  val newIndex = scala.collection.mutable.Map[String, scala.collection.mutable.Set[Int]]()

  for ((t, ws) <- oldIndex; s <- Search.tokenize(t); if MRIdentifier.unapply(s).isEmpty) {
    val set = newIndex.getOrElseUpdate(s, scala.collection.mutable.Set.empty)
    set ++= ws
  }

  val out = new PrintStream(new FileOutputStream("terms.new"))
  for ((t, ws) <- newIndex) {
    out.println(t)
    out.println(ws.mkString(","))
  }
}