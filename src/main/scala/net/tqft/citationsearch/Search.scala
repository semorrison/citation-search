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
import com.google.common.cache.LoadingCache
import scala.slick.driver.MySQLDriver.simple._
import scala.collection.JavaConverters._
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global

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
      .expireAfterAccess(5, TimeUnit.MINUTES)
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
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .build(loader)
  }

  case class Citation(MRNumber: Int, title: String, authors: String, cite: String, url: Option[String])

  val citationStore = db.getHashMap[Int, Option[Citation]]("articles").asScala

  val citationCache: LoadingCache[Int, Option[Citation]] = {
    val loader =
      new CacheLoader[Int, Option[Citation]]() {
        override def load(identifier: Int) = {
          import scala.collection.JavaConverters._
          val result = citationStore.getOrElseUpdate(identifier,
            SQL { implicit session =>
              (for (a <- TableQuery[MathscinetBIBTEX]; aux <- TableQuery[MathscinetAux]; if a.MRNumber === identifier; if aux.MRNumber === identifier) yield (a.url, a.doi, aux.wikiTitle, aux.textAuthors, aux.textCitation)).firstOption.map {
                case (url, doi, t, a, c) => Citation(identifier, t, a, c, doi.map("http://dx.doi.org/" + _).orElse(url))
              }
            })
          future { db.commit }
          result
        }
        override def loadAll(identifiers: java.lang.Iterable[_ <: Int]): java.util.Map[Int, Option[Citation]] = {
          val result = scala.collection.mutable.Map[Int, Option[Citation]]()
          val toLookup = scala.collection.mutable.ListBuffer[Int]()
          for (identifier <- identifiers.asScala) {
            citationStore.get(identifier) match {
              case Some(cite) => {
                result(identifier) = cite
              }
              case None => {
                toLookup += identifier
              }
            }
          }

          val records = SQL { implicit session =>
            val sql = (for (a <- TableQuery[MathscinetBIBTEX]; if a.MRNumber.inSet(toLookup); aux <- TableQuery[MathscinetAux]; if a.MRNumber === aux.MRNumber) yield (a.MRNumber, a.url, a.doi, aux.wikiTitle, aux.textAuthors, aux.textCitation))
            //              println(sql.selectStatement)
            sql.list
          }

          for (
            (identifier, url, doi, t, a, c) <- records
          ) {
            val cite = Citation(identifier, t, a, c, doi.map("http://dx.doi.org/" + _).orElse(url))
            result(identifier) = Some(cite)
          }
          future {
            for (
              (identifier, url, doi, t, a, c) <- records
            ) {
              val cite = Citation(identifier, t, a, c, doi.map("http://dx.doi.org/" + _).orElse(url))
              citationStore.put(identifier, Some(cite))
            }
            db.commit
          }

          for (id <- identifiers.asScala) {
            if (!result.contains(id)) {
              result.put(id, None)
            }
          }
          result.asJava
        }
      }

    CacheBuilder.newBuilder()
      .maximumSize(2000)
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .build(loader).asInstanceOf[LoadingCache[Int, Option[Citation]]]
  }

  val N = 656000

  def tokenize(words: String): Seq[String] = {
    words
      .replaceAll("\\p{P}", " ")
      .split("[-꞉:/⁄ _]")
      .filter(_.nonEmpty)
      .map(org.apache.commons.lang3.StringUtils.stripAccents)
      .map(_.toLowerCase)
  }

  def query(searchString: String): Seq[(Citation, Double)] = {
    println(searchString)

    val terms = tokenize(searchString).distinct
    lazy val idfs: Seq[(String, Double)] = terms.map(t => t -> idf(t)).collect({ case (t, Some(q)) => (t, q) }).sortBy(p => -p._2)

    println(idfs)

    val score: Int => Double = {
      val cache = scala.collection.mutable.Map[Int, Double]()

      { d: Int =>
        cache.getOrElseUpdate(d, (for ((t, q) <- idfs) yield {
          if (get(t).has(d)) q else 0.0
        }).sum)
      }
    }

    def scores(documents: Array[Int]) = {
      (for (d <- documents) yield {
        d -> score(d)
      }).sortBy({ p => (-p._2, -p._1) }).take(5).toSeq
    }

    lazy val mr = {
      terms.filter(_.startsWith("mr")).collect({ case MRIdentifier(k) => k }).headOption
    }

    val ids = (if (terms.isEmpty) {
      Seq.empty
    } else if (mr.nonEmpty) {
      Seq((mr.get, 1000.0))
    } else if (idfs.isEmpty) {
      Seq.empty
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
        scores(intersections(j - 1))
      } else {

        // start scoring the unions, until we have a winner
        val unions = sets.scanLeft(Array.empty[Int])(_ union _._2)
        val diff = sets.map(_._2).zip(unions).map(p => p._1 diff p._2)
        var scored = scala.collection.mutable.Buffer[(Int, Double)]()

        val tailQSums = idfs.map(_._2).tails.map(_.sum).toStream
        var k = 0
        while (k < idfs.size && (scored.size < 2 || (scored(0)._2 - scored(1)._2 < tailQSums(k)))) {
          scored ++= scores(diff(k))
          scored = scored.sortBy(p => (-p._2, -p._1))
          k += 1
        }

        scored.take(5).toSeq
      }
    })

    val citations = citationCache.getAll(ids.map(_._1).asJava).asScala
    val results = ids.map({ case (i, q) => (citations(i), q) }).collect({ case (Some(c), q) => (c, q) })

    for (r <- results) println(r)

    results

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

object SQL {
  def apply[A](closure: slick.driver.MySQLDriver.backend.Session => A): A = Database.forURL("jdbc:mysql://mysql.tqft.net/mathematicsliteratureproject?user=readonly1&password=readonly", driver = "com.mysql.jdbc.Driver") withSession closure
}

class MathscinetAux(tag: Tag) extends Table[(Int, String, String, String, String)](tag, "mathscinet_aux") {
  def MRNumber = column[Int]("MRNumber", O.PrimaryKey)
  def textTitle = column[String]("textTitle")
  def wikiTitle = column[String]("wikiTitle")
  def textAuthors = column[String]("textAuthors")
  def textCitation = column[String]("textCitation")
  def * = (MRNumber, textTitle, wikiTitle, textAuthors, textCitation)
}

class MathscinetBIBTEX(tag: Tag) extends Table[(Int, String, Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])](tag, "mathscinet_bibtex") {
  def MRNumber = column[Int]("MRNumber", O.PrimaryKey)
  def `type` = column[String]("type")
  def title = column[Option[String]]("title")
  def booktitle = column[Option[String]]("booktitle")
  def author = column[Option[String]]("author")
  def editor = column[Option[String]]("editor")
  def doi = column[Option[String]]("doi")
  def url = column[Option[String]]("url")
  def journal = column[Option[String]]("journal")
  def fjournal = column[Option[String]]("fjournal")
  def issn = column[Option[String]]("issn")
  def isbn = column[Option[String]]("isbn")
  def volume = column[Option[String]]("volume")
  def issue = column[Option[String]]("issue")
  def year = column[Option[String]]("year")
  def pages = column[Option[String]]("pages")
  def mrclass = column[Option[String]]("mrclass")
  def number = column[Option[String]]("number")
  def address = column[Option[String]]("address")
  def edition = column[Option[String]]("edition")
  def publisher = column[Option[String]]("publisher")
  def series = column[Option[String]]("series")
  def * = (MRNumber, `type`, title, booktitle, author, editor, doi, url, journal, fjournal, issn, isbn, volume, issue, year, pages, mrclass, number, address, edition, publisher, series)

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
