package net.tqft.citationsearch

import scala.io.Source
import scala.collection.mutable.ListBuffer
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.net.URL
import scala.io.Codec
import java.io.File
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.util.concurrent.TimeUnit
import net.tqft.mathscinet.Article
import com.google.common.cache.LoadingCache
import net.tqft.util.pandoc

object Search {

  import org.mapdb._

  def indexLines = {
    val file = new File("terms.gz")
    val stream = if (file.exists) {
      println(" ... from local file")
      new FileInputStream(file)
    } else {
      println(" ... from S3")
      new URL("https://s3.amazonaws.com/citation-search/terms.gz").openStream()
    }
    Source.fromInputStream(new GZIPInputStream(stream))(Codec.UTF8).getLines
  }

  val db = DBMaker.newFileDB(new File("db"))
    .closeOnJvmShutdown
    //    .asyncWriteEnable
    //    .mmapFileEnableIfSupported
    .make

  val store = db.getHashMap[String, Array[Int]]("terms")

  if (store.isEmpty) {
    println("Starting up search; loading data...")
    var count = 0
    for (pair <- indexLines.grouped(2)) {
      count += 1
      if (count % 1000 == 0) { println(s"Loaded $count terms"); db.commit }
      store.put(pair(0), pair(1).split(",").map(_.toInt))
    }
    db.commit
    println(" .. loaded index")
  }

  val index = {
    import scala.collection.JavaConverters._
    store.asScala
  }

  val cachedIndex = {
    val loader =
      new CacheLoader[String, Option[Array[Int]]]() {
        override def load(key: String) = {
          index.get(key)
        }
      }

    CacheBuilder.newBuilder()
      .maximumSize(20000)
      .expireAfterAccess(2, TimeUnit.MINUTES)
      .build(loader)
  }

  def get(t: String) = cachedIndex.getUnchecked(t).get

  val idf = {
    val loader =
      new CacheLoader[String, Option[Double]]() {
        override def load(term: String) = {
          cachedIndex.getUnchecked(term) match {
            case None => None
            case Some(documents) => {
              val n = documents.length
              val r = scala.math.log((N - n + 0.5) / (n + 0.5))
              if (r < 0) {
                None
              } else {
                Some(r)
              }
            }
          }
        }
      }

    CacheBuilder.newBuilder()
      .maximumSize(20000)
      .expireAfterAccess(2, TimeUnit.MINUTES)
      .build(loader)
  }

  case class Citation(title: String, authors: String, cite: String)

  val citationCache: LoadingCache[Int, Citation] = {
    val loader =
      new CacheLoader[Int, Citation]() {
        override def load(identifier: Int) = {
          val a = Article(identifier)
          Citation(a.sanitizedTitle,
            a.authors.map(a => pandoc.latexToText(a.name)).mkString(" and "),
            pandoc.latexToText(a.citation))
        }
      }

    CacheBuilder.newBuilder()
      .maximumSize(200)
      .expireAfterAccess(2, TimeUnit.MINUTES)
      .build(loader).asInstanceOf[LoadingCache[Int, Citation]]
  }

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

  def query(searchString: String): Seq[(Citation, Double)] = {
    val terms = tokenize(searchString).distinct
    val idfs: Seq[(String, Double)] = terms.map(t => t -> idf(t)).collect({ case (t, Some(q)) => (t, q) }).sortBy(p => -p._2)

    println(idfs)

    def score(documents: Array[Int]) = {
      val scores = (for (d <- documents) yield {
        d -> (for ((t, q) <- idfs) yield {
          if (get(t).has(d)) q else 0.0
        }).sum
      })

      scores.seq.sortBy({ p => (-p._2, -p._1) }).take(5)
    }

    lazy val mr = {
      terms.filter(_.startsWith("mr")).collect({ case MRIdentifier(k) => k }).headOption
    }

    val ids = (if (idfs.isEmpty) {
      Seq.empty
    } else if (mr.nonEmpty) {
      Seq((mr.get, 1000.0))
    } else {
      val sets = idfs.iterator.map({ case (t, q) => ((t, q), get(t)) }).toStream

      val intersections = sets.tail.scanLeft(sets.head._2)(_ intersect _._2)
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
        val unions = sets.scanLeft(Array.empty[Int])(_ union _._2)
        val diff = sets.map(_._2).zip(unions).map(p => p._1 diff p._2)
        var scored = scala.collection.mutable.Buffer[(Int, Double)]()

        val tailQSums = idfs.map(_._2).tails.map(_.sum).toStream
        var k = 0
        while (k < idfs.size && (scored.size < 2 || (scored(0)._2 - scored(1)._2 < tailQSums(k)))) {
          scored ++= score(diff(k))
          scored = scored.sortBy(p => (-p._2, -p._1))
          k += 1
        }

        scored.take(5).toSeq
      }
    })

    ids.map({ case (i, q) => (citationCache(i), q) })

  }

  implicit class SetArray(array: Array[Int]) {
    def has(x: Int): Boolean = {
      var a = 0
      var b = array.length
      while (b > a + 1) {
        val m = (b + a) / 2
        val am = array(m)
        if (am > x) {
          b = m
        } else if (am < x) {
          a = m
        } else {
          a = m
          b = m
        }
      }
      array(a) == x
    }
    def intersect(other: Array[Int]): Array[Int] = {
      var i = 0
      var j = 0
      var k = 0
      val result = new Array[Int](scala.math.min(array.length, other.length))
      while (i < array.length && j < other.length) {
        val ai = array(i)
        val aj = other(j)
        if (ai < aj) {
          i += 1
        } else if (ai > aj) {
          j += 1
        } else {
          result(k) = ai
          i += 1
          j += 1
          k += 1
        }
      }
      result.take(k)
    }
    def union(other: Array[Int]): Array[Int] = {
      var i = 0
      var j = 0
      var k = 0
      val result = new Array[Int](array.length + other.length)
      while (i < array.length && j < other.length) {
        val ai = array(i)
        val aj = other(j)
        if (ai < aj) {
          result(k) = ai
          i += 1
        } else if (ai > aj) {
          result(k) = aj
          j += 1
        } else {
          result(k) = ai
          i += 1
          j += 1
        }
        k += 1
      }
      while (i < array.length) {
        result(k) = array(i)
        i += 1
        k += 1
      }
      while (j < other.length) {
        result(k) = other(j)
        j += 1
        k += 1
      }
      result.take(k)
    }
    def diff(other: Array[Int]): Array[Int] = {
      var i = 0
      var j = 0
      var k = 0
      val result = new Array[Int](array.length)
      while (i < array.length && j < other.length) {
        val ai = array(i)
        val aj = other(j)
        if (ai < aj) {
          result(k) = ai
          i += 1
          k += 1
        } else if (ai > aj) {
          j += 1
        } else {
          i += 1
          j += 1
        }
      }
      while (i < array.length) {
        result(k) = array(i)
        i += 1
        k += 1
      }
      result.take(k)
    }
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
