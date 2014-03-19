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
import org.apache.commons.io.FileUtils
import argonaut._
import Argonaut._

// TODO cache idfs

case class Citation(MRNumber: Int, title: String, authors: String, cite: String, url: String, pdf: Option[String], free: Option[String]) {
  def best = free.orElse(pdf).getOrElse(url)
  require(url != "")
}
object Citation {
  implicit def CitationScoreCodecJson = {
    // oh, the boilerplate
    def toCitation(MRNumber: Int, title: String, authors: String, cite: String, url: String, pdf: Option[String], free: Option[String], best: String) = Citation(MRNumber, title, authors, cite, url, pdf, free)
    casecodec8(toCitation, { c: Citation => Some((c.MRNumber, c.title, c.authors, c.cite, c.url, c.pdf, c.free, c.best)) })("MRNumber", "title", "authors", "cite", "url", "pdf", "free", "best")
  }
}
case class CitationScore(citation: Citation, score: Double)
object CitationScore {
  implicit def CitationScoreCodecJson =
    casecodec2(CitationScore.apply, CitationScore.unapply)("citation", "score")
}

case class Result(query: String, results: List[CitationScore])
object Result {
  implicit def ResultCodecJson =
    casecodec2(Result.apply, Result.unapply)("query", "results")
}

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

  val dbFile = new File("db")
  val dbpFile = new File("db.p")

  try {
    if (!dbFile.exists) {
      FileUtils.copyURLToFile(new URL("https://s3.amazonaws.com/citation-search/db"), dbFile)
    }
    if (!dbpFile.exists) {
      FileUtils.copyURLToFile(new URL("https://s3.amazonaws.com/citation-search/db.p"), dbpFile)
    }
  } catch {
    case e: Exception =>
  }

  val db = DBMaker.newFileDB(dbFile)
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

  val citationStore = db.getHashMap[Int, Option[Citation]]("articles").asScala

  val citationCache: LoadingCache[Int, Option[Citation]] = {
    def check(o: Option[String]) = {
      o match {
        case Some("-") => None
        case Some("") => None
        case o => o
      }
    }

    val loader =
      new CacheLoader[Int, Option[Citation]]() {

        private def correctURL(url: Option[String]) = {
          if (url.nonEmpty && url.get.startsWith("http://projecteuclid.org/getRecord?id=")) {
            Some(url.get.replaceAllLiterally("http://projecteuclid.org/getRecord?id=", "http://projecteuclid.org/"))
          } else {
            url
          }
        }

        override def load(identifier: Int) = {
          import scala.collection.JavaConverters._
          val result = citationStore.getOrElseUpdate(identifier,
            SQL { implicit session =>
              (for (a <- TableQuery[MathscinetBIBTEX]; aux <- TableQuery[MathscinetAux]; if a.MRNumber === identifier; if aux.MRNumber === identifier) yield (a.url, a.doi, aux.wikiTitle, aux.textAuthors, aux.textCitation, aux.pdf, aux.free)).firstOption.map {
                case (url, doi, t, a, c, pdf, free) => Citation(identifier, t, a, c, doi.map("http://dx.doi.org/" + _).orElse(correctURL(url)).getOrElse("http://www.ams.org/mathscinet-getitem?mr=" + identifier), check(pdf), check(free))
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

          if (toLookup.nonEmpty) {
            val records = SQL { implicit session =>
              val sql = (for (a <- TableQuery[MathscinetBIBTEX]; if a.MRNumber.inSet(toLookup); aux <- TableQuery[MathscinetAux]; if a.MRNumber === aux.MRNumber) yield (a.MRNumber, a.url, a.doi, aux.wikiTitle, aux.textAuthors, aux.textCitation, aux.pdf, aux.free))
              //              println(sql.selectStatement)
              sql.list
            }

            val cites = for (
              (identifier, url, doi, t, a, c, pdf, free) <- records
            ) yield (identifier, Citation(identifier, t, a, c, doi.map("http://dx.doi.org/" + _).orElse(correctURL(url)).getOrElse("http://www.ams.org/mathscinet-getitem?mr=" + identifier), check(pdf), check(free)))

            for ((identifier, cite) <- cites) {
              result(identifier) = Some(cite)
            }
            future {
              for ((identifier, cite) <- cites) {
                citationStore.put(identifier, Some(cite))
              }
              db.commit
            }
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

  private val N = 669000

  def tokenize(words: String): Seq[String] = {
    words
      .replaceAll("\\p{P}", " ")
      .split("[-꞉:/⁄ _]")
      .map(org.apache.commons.lang3.StringUtils.stripAccents)
      .map(_.toLowerCase)
      .filter(_.nonEmpty)
  }

  private val queryCache = CacheBuilder.newBuilder()
    .maximumSize(2000)
    .expireAfterAccess(6, TimeUnit.HOURS)
    .build(new CacheLoader[String, Result]() {
      override def load(identifier: String) = {
        _query(identifier)
      }
    })

  def query(searchString: String): Result = {
    queryCache.getUnchecked(searchString)
  }

  private def _query(searchString: String): Result = {
    println(searchString)

    val terms = tokenize(searchString).distinct
    lazy val idfs: Seq[(String, Double)] = terms.map(t => t -> idf.getUnchecked(t)).collect({ case (t, Some(q)) => (t, q) }).sortBy(p => -p._2)

    println(terms)
    println(idfs)

    val score: Int => Double = {
      val cache = scala.collection.mutable.Map[Int, Double]()

      def _score(d: Int) = (for ((t, q) <- idfs) yield {
        if (get(t).has(d)) q else 0.0
      }).sum

      { d: Int =>
        cache.getOrElseUpdate(d, _score(d))
      }
    }

    def scores(documents: Array[Int]): List[(Int, Double)] = {
      (for (d <- documents) yield {
        d -> score(d)
      }).sortBy({ p => (-p._2, -p._1) }).take(10).toList
    }

    lazy val mr = {
      terms.filter(_.startsWith("mr")).collect({ case MRIdentifier(k) => k }).headOption
    }
    val sumq = idfs.map(_._2).sum

    val ids = (if (terms.isEmpty) {
      Nil
    } else if (mr.nonEmpty) {
      List((mr.get, sumq))
    } else if (idfs.isEmpty) {
      Nil
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
//          println("scoring " + idfs(k))

          scored ++= scores(diff(k))
          scored = scored.sortBy(p => (-p._2, -p._1))
          k += 1
        }

        scored.take(10).toSeq
      }
    })

    val citations = citationCache.getAll(ids.map(_._1).asJava).asScala

    def rescore(p: CitationScore) = {
      val titleTokens = tokenize(p.citation.title)
      if (titleTokens == terms) {
        p.score + 20
      } else {
        val bonus = titleTokens.sliding(2).count(pair => terms.sliding(2).contains(pair))
        p.score + bonus
      }
    }

    val results = ids.map({ case (i, q) => (citations(i), q) }).collect({ case (Some(c), q) => CitationScore(c, q / sumq) }).sortBy(p => -rescore(p)).toList

    for (r <- results) println(r)

    Result(searchString, results)

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

class MathscinetAux(tag: Tag) extends Table[(Int, String, String, String, String, Option[String], Option[String])](tag, "mathscinet_aux") {
  def MRNumber = column[Int]("MRNumber", O.PrimaryKey)
  def textTitle = column[String]("textTitle")
  def wikiTitle = column[String]("wikiTitle")
  def textAuthors = column[String]("textAuthors")
  def textCitation = column[String]("textCitation")
  def pdf = column[Option[String]]("pdf")
  def free = column[Option[String]]("free")
  def * = (MRNumber, textTitle, wikiTitle, textAuthors, textCitation, pdf, free)
  def citationData = (MRNumber, textTitle, wikiTitle, textAuthors, textCitation)
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
    if (s.toLowerCase.startsWith("mr")) {
      s.drop(2) match {
        case Int(id) => Some(id)
        case _ => None
      }
    } else {
      None
    }
  }
}
