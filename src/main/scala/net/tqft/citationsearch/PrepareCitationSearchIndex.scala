package net.tqft.citationsearch

import java.io.PrintStream
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream
import java.io.File
import org.mapdb.DBMaker
import net.tqft.toolkit.amazon.S3
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.commons.lang3.StringEscapeUtils

object PrepareCitationSearchIndex extends App {
  import slick.jdbc.MySQLProfile.api._

  // Make sure to run SQLAuxApp first

  val index = scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]]()

  var articleCount = 0

  val step = 5000

  def articlesPage(k: Int) = {
    println("retrieving page " + k)
    SQL {
      (for (
        aux <- TableQuery[MathscinetAux];
        p <- TableQuery[MathscinetBIBTEX];
        if aux.MRNumber === p.MRNumber;
        if aux.textTitle =!= "Publications: Transactions of the American Mathematical Society" // these aren't worth showing in search results, and confuse the scoring algorithm
      ) yield {
        (aux.MRNumber, aux.textTitle ++ " - " ++ aux.textAuthors ++ " - " ++ aux.textCitation ++ " " ++ p.doi.getOrElse("") ++ " " ++ p.fjournal.getOrElse(""))
      }).drop(k * step).take(step).result
    }
  }
  def articlesPaged = Iterator.from(0).map(articlesPage).takeWhile(_.nonEmpty).flatten

  def arXivPage(k: Int): Seq[(String, String)] = {
    try {
      println("retrieving page " + k)
      (SQL {
        (for (
          a <- TableQuery[Arxiv];
          p <- TableQuery[ArxivAux];
          if p.arxivid === a.arxivid
        ) yield {
          (a.arxivid, p.textTitle ++ " - " ++ p.textAuthors ++ " - " ++ a.journalref.getOrElse("") ++ " " ++ a.doi.getOrElse(""))
        }).drop(k * step).take(step)
      })
    } catch {
      case e: Exception => {
        e.printStackTrace
        arXivPage(k)
      }
    }
  }
  def arXivPaged = Iterator.from(0).map(arXivPage).takeWhile(_.nonEmpty).flatten

  for ((identifier, citation) <- arXivPaged) {
    println("arXiv:" + identifier)
    println(citation)
    articleCount = articleCount + 1
    try {
      for (w <- Tokenize(citation); if MRIdentifier.unapply(w).isEmpty) {
        index.getOrElseUpdate(w, scala.collection.mutable.Set[String]()) += "arXiv:" + identifier
      }
    } catch {
      case e: Exception => println("Exception while preparing citation for:\narXiv:" + identifier); e.printStackTrace()
    }
  }
  for ((identifier, citation) <- articlesPaged) {
    articleCount = articleCount + 1
    try {
      for (w <- Tokenize(citation); if MRIdentifier.unapply(w).isEmpty) {
        index.getOrElseUpdate(w, scala.collection.mutable.Set[String]()) += "MR" + identifier
      }
    } catch {
      case e: Exception => println("Exception while preparing citation for:\nMR" + identifier); e.printStackTrace()
    }
  }

  println(index.size)

  val dbFile = new File("db")
  val dbpFile = new File("db.p")
  val termsFile = new File("terms.gz")

  dbFile.delete
  dbpFile.delete
  termsFile.delete

  val db = DBMaker.newFileDB(dbFile)
    .closeOnJvmShutdown
    .make

  val store = db.getHashMap[String, Map[String, Array[String]]]("terms")

  var count = 0

  val out = new PrintStream(new GZIPOutputStream(new FileOutputStream(termsFile)))
  for ((term, documents) <- index; if term.size > 1) {
    out.println(term)
    out.println(documents.toSeq.sorted.mkString(","))
    count += 1
    if (count % 1000 == 0) { println(s"Loaded $count terms"); db.commit }
    val ids = documents.toSeq.sorted.toArray
    val map = Map("mathscinet" -> ids.filter(_.startsWith("MR")), "arxiv" -> ids.filter(_.startsWith("arXiv:")))
    store.put(term, map)
  }
  out.close

  db.commit

  println(s" .. rebuilt index: $articleCount articles")

  val bucket = S3.bytes("citation-search")
  bucket("terms.gz") = Files.readAllBytes(Paths.get("terms.gz"))
  bucket("db") = Files.readAllBytes(Paths.get("db"))
  bucket("db.p") = Files.readAllBytes(Paths.get("db.p"))

}