package net.tqft.citationsearch

import scala.io.Source
import scala.collection.mutable.ListBuffer
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.net.URL
import scala.io.Codec

object Search {

  println("Starting up search; loading data...")
  
  def indexData = {
//    new URL("https://s3.amazonaws.com/citation-search/terms.gz").openStream() 
    new FileInputStream("terms.gz")
  }
  
  val index: Map[String, Set[Int]] = Source.fromInputStream(new GZIPInputStream(indexData))(Codec.UTF8).getLines.grouped(2).map({ pair =>
    pair(0) -> pair(1).split(",").map(_.toInt).toSet
  }).toMap

  println(" .. loaded index")
  
//  val cites: Map[Int, String] = Source.fromFile("cites").getLines.grouped(2).map({ pair =>
//    require(pair(0).startsWith("MR"))
//    pair(0).drop(2).toInt -> pair(1)
//  }).toMap
//
//  println(" .. loaded cites")
  
  val N = 650000

  def tokenize(words: String): Seq[String] = {
    words
      .replaceAll("\\p{P}", " ")
      .split("[-꞉:/⁄ _]")
      .filter(_.nonEmpty)
      .map(org.apache.commons.lang3.StringUtils.stripAccents)
      .map(_.toLowerCase)
  }

  def idf(term: String): Option[Double] = {
    index.get(term) match {
      case None => None
      case Some(documents) => {
        val n = documents.size
        val r = scala.math.log((N - n + 0.5) / (n + 0.5))
        if (r < 0) {
          None
        } else {
          Some(r)
        }
      }
    }
  }

  def query(searchString: String): Seq[(Int, Double)] = {
    val terms = tokenize(searchString)
    val idfs: Seq[(String, Double)] = terms.map(t => t -> idf(t)).collect({ case (t, Some(q)) => (t, q) }).sortBy(p => -p._2)

    println(idfs)
    
    def score(documents: Set[Int]): Seq[(Int, Double)] = {
      val scores: Iterator[(Int, Double)] = (for (d <- documents.iterator) yield {
        d -> (for ((t, q) <- idfs) yield {
          if (index(t).contains(d)) q else 0.0
        }).sum
      })

      scores.toVector.sortBy({ p => -p._2 }).take(5)
    }

    lazy val mr = {
      terms.filter(_.startsWith("mr")).collect({ case MRIdentifier(k) => k }).headOption
    }

    (if (idfs.isEmpty) {
      Seq.empty
    } else if (mr.nonEmpty) {
      Seq((mr.get, 1000.0))
    } else {
      val sets = idfs.iterator.map({ case (t, q) => ((t, q), index(t)) }).toStream

      val intersections = sets.tail.scanLeft(sets.head._2.toSet)(_ intersect _._2)
      val j = intersections.indexWhere(_.isEmpty) match {
        case -1 => intersections.size
        case j => j
      }

      val qsum = idfs.drop(j).map(_._2).sum
      if (idfs(j - 1)._2 >= qsum) {
        // the other search terms don't matter
        score(intersections(j - 1))
      } else {

        // start scoring the unions, until we have a winner
        val unions = sets.scanLeft(Set[Int]())(_ union _._2)
        val diff = sets.map(_._2).zip(unions).map(p => p._1 diff p._2)
        var scored = scala.collection.mutable.Buffer[(Int, Double)]()

        val tailQSums = idfs.map(_._2).tails.map(_.sum).toStream
        var k = 0
        while (k < idfs.size && (scored.size < 2 || (scored(0)._2 - scored(1)._2 < tailQSums(k)))) {
          scored ++= score(diff(k))
          scored = scored.sortBy(p => -p._2)
          k += 1
        }

        scored.take(5).toSeq
      }
    })//.map({ case (i, q) => (cites(i), q) })
  }
}

object Int {
  def unapply(s: String): Option[Int] = try {
    Some(s.toInt)
  } catch {
    case _: java.lang.NumberFormatException => None
  }
}

object MRIdentifier {
  def unapply(s: String): Option[Int] = {
    if (s.startsWith("mr")) {
      s.drop(2) match {
        case Int(id) => Some(id)
        case _ => None
      }
    } else {
      None
    }
  }
}
