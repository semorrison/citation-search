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
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.google.common.cache.LoadingCache
import slick.jdbc.MySQLProfile.api._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.commons.io.FileUtils
import argonaut._
import Argonaut._
import scala.reflect.ClassTag
import java.sql.Date

// TODO cache idfs

//sealed trait Identifier
//case class MathSciNet(mrnumber: Int) {
//  override def toString = "MR" + mrnumber
//}
//case class arXiv(identifier: String) {
//  override def toString = "arXiv:" + identifier
//}
//case class Scopus(identifier: String) {
//  override def toString = "scopus:" + identifier
//}
//
//object Identifier {
//  implicit val ordering = Ordering.by[Identifier, String]({ s: Identifier => s.toString }) 
//}

object Tokenize {
  def apply(string: String): Seq[String] = {
    val words = string.split(" ").toSeq
    val (dois, others) = words.partition({ w =>
      w.startsWith("DOI:") || w.startsWith("doi:") || w.startsWith("http://dx.doi.org/") || w.startsWith("10.") && w.matches("""10\.[0-9]{4}/.*""")
    })

    dois.map(_.stripPrefix("DOI:").stripPrefix("doi:").stripPrefix("http://dx.doi.org/")) ++
      others.mkString(" ")
      .replaceAll("\\p{P}", " ")
      .split("[-꞉:/⁄ _]")
      .map(org.apache.commons.lang3.StringUtils.stripAccents)
      .map(_.toLowerCase)
      .filter(_.nonEmpty)
  }

}

case class Citation(
    MRNumber: Option[Int],
    arXiv: Option[String],
    Scopus: Option[String],
    title: String,
    authors: String,
    citation_text: String,
    citation_markdown: String,
    citation_html: String,
    url: String,
    pdf: Option[String],
    free: Option[String]) {
  def best = free.orElse(pdf).getOrElse(url)
  private def fixOption[A](o: Option[A]) = {
    if (o.isEmpty) {
      //      require(o match {
      //        case None => true
      //        case Some(_) => true
      //        case _ => false
      //      })
      None
    } else {
      Some(o.get)
    }
  }
  def fix = Citation(fixOption(MRNumber), fixOption(arXiv), fixOption(Scopus), title, authors, citation_text, citation_markdown, citation_html, url, fixOption(pdf), fixOption(free))
  require(url != "")
}
object Citation {
  implicit def CitationScoreCodecJson = {
    // oh, the boilerplate
    def toCitation(MRNumber: Option[Int], arXiv: Option[String], Scopus: Option[String], title: String, authors: String, citation_text: String, citation_markdown: String, citation_html: String, url: String, pdf: Option[String], free: Option[String], best: String) = Citation(MRNumber, arXiv, Scopus, title, authors, citation_text, citation_markdown, citation_html, url, pdf, free).fix
    casecodec12(toCitation, { c: Citation => Some((c.MRNumber, c.arXiv, c.Scopus, c.title, c.authors, c.citation_text, c.citation_markdown, c.citation_html, c.url, c.pdf, c.free, c.best)) })("MRNumber", "arXiv", "Scopus", "title", "authors", "citation_text", "citation_markdown", "citation_html", "url", "pdf", "free", "best")
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

  def ensureDatabasePresent {
    try {
      if (!dbFile.exists) {
        println(" ... copying db from S3")
        FileUtils.copyURLToFile(new URL("https://s3.amazonaws.com/citation-search/db"), dbFile)
      }
      if (!dbpFile.exists) {
        println(" ... copying db.p from S3")
        FileUtils.copyURLToFile(new URL("https://s3.amazonaws.com/citation-search/db.p"), dbpFile)
      }
    } catch {
      case e: Exception =>
    }
  }

  ensureDatabasePresent

  val db = DBMaker.newFileDB(dbFile)
    .closeOnJvmShutdown
    .make

  lazy val index = {
    val store = db.getHashMap[String, Map[String, Array[String]]]("terms")
    if (store.isEmpty) {
      println("Starting up search; loading data...")
      var count = 0
      for (pair <- indexLines.grouped(2)) {
        count += 1
        if (count % 1000 == 0) { println(s"Loaded $count terms"); db.commit }
        val ids = pair(1).split(",")
        val map = Map("mathscinet" -> ids.filter(_.startsWith("MR")), "arxiv" -> ids.filter(_.startsWith("arXiv:")))
        store.put(pair(0), map)
      }
      db.commit
      println(" .. loaded index")
    }

    import scala.collection.JavaConverters._
    store.asScala
  }

  val cachedIndex = {
    val loader =
      new CacheLoader[String, Option[Map[String, Array[String]]]]() {
        override def load(key: String) = {
          index.get(key)
        }
      }

    CacheBuilder.newBuilder()
      .maximumSize(200000)
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .build(loader)
  }

  def get(t: String, identifierTypes: Array[String]): Array[String] = {
    val map = cachedIndex.getUnchecked(t).get
    identifierTypes.map(t => map(t)).flatten
  }

  val idf = {
    val loader =
      new CacheLoader[String, Option[Double]]() {
        override def load(term: String) = {
          cachedIndex.getUnchecked(term) match {
            case None => None
            case Some(documents: Map[String, Array[String]]) => {
              val n = documents.values.map(_.length).sum
              if (n == 0) {
                None
              } else {
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
      }

    CacheBuilder.newBuilder()
      .maximumSize(200000)
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .build(loader)
  }

  val citationStore = db.getHashMap[String, Option[Citation]]("articles").asScala

  val citationCache: LoadingCache[String, Option[Citation]] = {
    def check(o: Option[String]) = {
      o match {
        case Some("-") => None
        case Some("")  => None
        case o         => o
      }
    }

    val loader =
      new CacheLoader[String, Option[Citation]]() {

        private def correctURL(url: Option[String]) = {
          if (url.nonEmpty && url.get.startsWith("http://projecteuclid.org/getRecord?id=")) {
            Some(url.get.replaceAllLiterally("http://projecteuclid.org/getRecord?id=", "http://projecteuclid.org/"))
          } else {
            url
          }
        }

        override def load(identifier: String) = {
          if (identifier.startsWith("MR")) {
            loadMathSciNet(identifier.stripPrefix("MR").toInt)
          } else if (identifier.startsWith("arXiv:")) {
            loadArXiv(identifier.stripPrefix("arXiv:"))
          } else {
            ???
          }
        }

        def loadArXiv(identifier: String) = {
          ???
        }

        def loadMathSciNet(identifier: Int) = {
          import scala.collection.JavaConverters._
          val result = citationStore.getOrElseUpdate("MR" + identifier,
            (SQL {
              (for (
                a <- TableQuery[MathscinetBIBTEX];
                aux <- TableQuery[MathscinetAux];
                if a.MRNumber === identifier;
                if aux.MRNumber === identifier
              ) yield (a.url,
                a.doi,
                aux.wikiTitle,
                aux.textAuthors,
                aux.textCitation,
                aux.markdownCitation,
                aux.htmlCitation,
                aux.pdf,
                aux.free))
            }).headOption.map {
              // TODO fill in other identifiers if available
              case (url, doi, title, authors, citation_text, citation_markdown, citation_html, pdf, free) =>
                Citation(
                  Some(identifier),
                  None,
                  None,
                  title,
                  authors,
                  citation_text,
                  citation_markdown,
                  citation_html,
                  doi.map("http://dx.doi.org/" + _).orElse(correctURL(url)).getOrElse("http://www.ams.org/mathscinet-getitem?mr=" + identifier),
                  check(pdf),
                  check(free))
            }).map(_.fix)
          Future { db.commit }
          result
        }
        override def loadAll(identifiers: java.lang.Iterable[_ <: String]): java.util.Map[String, Option[Citation]] = {
          val result = scala.collection.mutable.Map[String, Option[Citation]]()
          val toLookupMathSciNet = scala.collection.mutable.ListBuffer[Int]()
          val toLookupArXiv = scala.collection.mutable.ListBuffer[String]()
          for (identifier <- identifiers.asScala) {
            citationStore.get(identifier) match {
              case Some(cite) => {
                result(identifier) = cite.map(_.fix)
              }
              case None => {
                if (identifier.startsWith("MR")) {
                  toLookupMathSciNet += identifier.stripPrefix("MR").toInt
                } else if (identifier.startsWith("arXiv:")) {
                  toLookupArXiv += identifier.stripPrefix("arXiv:")
                }
              }
            }
          }

          if (toLookupMathSciNet.nonEmpty) {
            val records = SQL {
              (for (a <- TableQuery[MathscinetBIBTEX]; if a.MRNumber.inSet(toLookupMathSciNet); aux <- TableQuery[MathscinetAux]; if a.MRNumber === aux.MRNumber) yield (a.MRNumber, a.url, a.doi, aux.wikiTitle, aux.textAuthors, aux.textCitation, aux.markdownCitation, aux.htmlCitation, aux.pdf, aux.free))
            }

            // TODO fill in the arXiv identifier if available
            val cites = for (
              (identifier, url, doi, title, authors, citation_text, citation_markdown, citation_html, pdf, free) <- records
            ) yield (
              identifier,
              Citation(
                Some(identifier),
                None,
                None,
                title,
                authors,
                citation_text,
                citation_markdown,
                citation_html,
                doi.map("http://dx.doi.org/" + _).orElse(correctURL(url)).getOrElse("http://www.ams.org/mathscinet-getitem?mr=" + identifier),
                check(pdf),
                check(free)))

            for ((identifier, cite) <- cites) {
              result("MR" + identifier) = Some(cite)
            }
            Future {
              for ((identifier, cite) <- cites) {
                citationStore.put("MR" + identifier, Some(cite))
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
      .build(loader).asInstanceOf[LoadingCache[String, Option[Citation]]]
  }

  private val N = {
    val s = scala.collection.mutable.Set[String]()
    for ((_, m) <- index; (_, v) <- m) {
      s ++= v
    }
    //    println(s"Counting index --- there are ${s.size} bibtex records")
    s.size
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
    queryCache.get(searchString)
  }

  def goodMatch(searchString: String) = {
    val matches = query(searchString).results.sortBy(-_.score)

    matches.headOption.filter(s => s.score > 0.89).orElse(
      matches.sliding(2).filter(p => p.size == 2 && p(0).score > 0.48 && scala.math.pow(p(0).score, 1.75) > p(1).score).toStream.headOption.map(_.head))
  }

  private def _query(searchString: String, identifierTypes: Array[String] = Array("mathscinet")): Result = {
    //    println(s"searchString = $searchString")

    val allTerms = Tokenize(searchString)
    val terms = allTerms.distinct
    lazy val idfs: Seq[(String, Double)] = terms.map(t => t -> idf.getUnchecked(t)).collect({ case (t, Some(q)) => (t, q) }).sortBy(p => -p._2)

    print("idfs: " + idfs)

    //    println(s"terms = $terms")
    //    println(s"idfs = $idfs")

    val score: String => Double = {
      val cache = scala.collection.mutable.Map[String, Double]()

      //      implicit val tag = scala.reflect.classTag[String] 

      def _score(d: String) = (for ((t, q) <- idfs) yield {
        if (get(t, identifierTypes).has(d)) q else 0.0
      }).sum

      { d: String =>
        cache.getOrElseUpdate(d, _score(d))
      }
    }

    def scores(documents: Array[String]): List[(String, Double)] = {
      (for (d <- documents) yield {
        d -> score(d)
      }).sortBy({ p => (-p._2, p._1) }).take(10).toList
    }

    lazy val mr = {
      terms.filter(_.startsWith("mr")).collect({ case MRIdentifier(k) => k }).headOption
    }
    val sumq = idfs.map(_._2).sum

    val ids = (if (terms.isEmpty) {
      Nil
    } else if (mr.nonEmpty) {
      List(("MR" + mr.get, sumq))
    } else if (idfs.isEmpty) {
      Nil
    } else {
      val sets = idfs.iterator.map({ case (t, q) => ((t, q), get(t, identifierTypes)) }).toStream

      val intersections = sets.tail.scanLeft(sets.head._2)(_ intersect _._2)
      //      for(i <- intersections) {
      //        println(i.toList)
      //      }

      //      println(s"intersections = ${intersections.toList.map(_.toList)}")

      val j = intersections.indexWhere(_.isEmpty) match {
        case -1 => intersections.size
        case j  => j
      }

      val qsum = idfs.drop(j).map(_._2).sum
      if (idfs(j - 1)._2 >= qsum) {
        // the other search terms don't matter
        scores(intersections(j - 1))
      } else {

        // start scoring the unions, until we have a winner
        val unions = sets.scanLeft(Array.empty[String])(_ union _._2)
        val diff = sets.map(_._2).zip(unions).map(p => p._1 diff p._2)
        var scored = scala.collection.mutable.Buffer[(String, Double)]()

        val tailQSums = idfs.map(_._2).tails.map(_.sum).toStream
        var k = 0
        while (k < idfs.size && (scored.size < 2 || (scored(0)._2 - scored(1)._2 < tailQSums(k)))) {
          //          println("scoring " + idfs(k))

          scored ++= scores(diff(k))
          scored = scored.sortBy(p => (-p._2, p._1))
          k += 1
        }

        scored.take(10).toSeq
      }
    })

    //    print(s"ids = $ids")

    val citations = citationCache.getAll(ids.map(_._1).asJava).asScala

    def rescore(p: CitationScore) = {
      //      println(s"rescoring $p")
      val titleTokens = Tokenize(p.citation.title)
      val newScore = if (titleTokens == allTerms.take(titleTokens.size)) {
        //        println("   contains all title tokens!")
        1 - (1 - p.score) / 2
      } else {
        val bonus = titleTokens.sliding(2).count(pair => terms.sliding(2).contains(pair)).toDouble / titleTokens.size
        //        println(s"    bonus = $bonus")
        p.score + bonus * (1 - p.score) / 2
      }
      CitationScore(p.citation, newScore)
    }

    val results = ids.map({ case (i, q) => (citations(i), q) }).collect({ case (Some(c), q) => rescore(CitationScore(c, q / (sumq + 1))) }).sortBy(p => -p.score).toList

    //    for (r <- results) println(r)

    Result(searchString, results)

  }

  implicit class SetArray[I: Ordering: ClassTag](array: Array[I]) {

    import Ordered.orderingToOrdered
    def has(x: I): Boolean = {
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
    def intersect(other: Array[I]): Array[I] = {
      var i = 0
      var j = 0
      var k = 0
      val result = new Array[I](scala.math.min(array.length, other.length))
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
    def union(other: Array[I]): Array[I] = {
      var i = 0
      var j = 0
      var k = 0
      val result = new Array[I](array.length + other.length)
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
    def diff(other: Array[I]): Array[I] = {
      var i = 0
      var j = 0
      var k = 0
      val result = new Array[I](array.length)
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
  val db = {
    import slick.jdbc.MySQLProfile.api._
    Database.forURL("jdbc:mysql://mysql.tqft.net/mathematicsliteratureproject?user=mathscinetbot&password=zytopex", driver = "com.mysql.jdbc.Driver")
  }

  def apply[R](x: slick.lifted.Rep[Int]) = Await.result(db.run(x.result), Duration.Inf)
  def apply[R](x: slick.lifted.Query[Any, R, Seq]): Seq[R] = Await.result(db.run(x.result), Duration.Inf)
  def stream[R](x: slick.lifted.Query[Any, R, Seq]): slick.basic.DatabasePublisher[R] = db.stream(x.result)
  def apply[R, S <: NoStream, E <: Effect](x: slick.sql.SqlAction[R, S, E]): R = Await.result(db.run(x), Duration.Inf)
}

class Arxiv(tag: Tag) extends Table[(String, Date, Option[Date], String, String, String, Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], String)](tag, "arxiv") {
  def arxivid = column[String]("arxivid", O.PrimaryKey)
  def created = column[Date]("created")
  def updated = column[Option[Date]]("updated")
  def authors = column[String]("authors")
  def title = column[String]("title")
  def categories = column[String]("categories")
  def comments = column[Option[String]]("comments")
  def proxy = column[Option[String]]("proxy")
  def reportno = column[Option[String]]("reportno")
  def mscclass = column[Option[String]]("mscclass")
  def acmclass = column[Option[String]]("acmclass")
  def journalref = column[Option[String]]("journalref")
  def doi = column[Option[String]]("doi")
  def license = column[Option[String]]("license")
  def `abstract` = column[String]("abstract")
  def * = (arxivid, created, updated, authors, title, categories, comments, proxy, reportno, mscclass, acmclass, journalref, doi, license, `abstract`)
}

class ArxivAux(tag: Tag) extends Table[(String, String, String)](tag, "arxiv_aux") {
  def arxivid = column[String]("arxivid", O.PrimaryKey)
  def textTitle = column[String]("textTitle")
  def textAuthors = column[String]("textAuthors")
  def * = (arxivid, textTitle, textAuthors)
}

class MathscinetAux(tag: Tag) extends Table[(Int, String, String, String, String, String, String, Option[String], Option[String])](tag, "mathscinet_aux") {
  def MRNumber = column[Int]("MRNumber", O.PrimaryKey)
  def textTitle = column[String]("textTitle")
  def wikiTitle = column[String]("wikiTitle")
  def textAuthors = column[String]("textAuthors")
  def textCitation = column[String]("textCitation")
  def markdownCitation = column[String]("markdownCitation")
  def htmlCitation = column[String]("htmlCitation")
  def pdf = column[Option[String]]("pdf")
  def free = column[Option[String]]("free")
  def * = (MRNumber, textTitle, wikiTitle, textAuthors, textCitation, markdownCitation, htmlCitation, pdf, free)
  def citationData = (MRNumber, textTitle, wikiTitle, textAuthors, textCitation, markdownCitation, htmlCitation)
}

class MathscinetBIBTEX(tag: Tag) extends Table[(Int, String, Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], (Option[String], Option[String]))](tag, "mathscinet_bibtex") {
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
  def note = column[Option[String]]("note")
  def * = (MRNumber, `type`, title, booktitle, author, editor, doi, url, journal, fjournal, issn, isbn, volume, issue, year, pages, mrclass, number, address, edition, publisher, (series, note))

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
        case _       => None
      }
    } else {
      None
    }
  }
}
