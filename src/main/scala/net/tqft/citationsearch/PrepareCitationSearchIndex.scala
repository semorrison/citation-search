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
  import scala.slick.driver.MySQLDriver.simple._

  // Make sure to run SQLAuxApp first

  val index = scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]]()

  var articleCount = 0

  val step = 10000

  SQL { implicit session =>
    def articlesPage(k: Int) = {
      println("retrieving page " + k)
      (for (
        aux <- TableQuery[MathscinetAux];
        p <- TableQuery[MathscinetBIBTEX];
        if aux.MRNumber === p.MRNumber;
        if aux.textTitle =!= "Publications: Transactions of the American Mathematical Society" // these aren't worth showing in search results, and confuse the scoring algorithm
      ) yield {
        (aux.MRNumber, aux.textTitle ++ " - " ++ aux.textAuthors ++ " - " ++ aux.textCitation ++ " " ++ p.doi.getOrElse("") ++ " " ++ p.fjournal.getOrElse(""))
      }).drop(k * step).take(step).list
    }
    def articlesPaged = Iterator.from(0).map(articlesPage).takeWhile(_.nonEmpty).flatten

    def arXivPage(k: Int) = {
      println("retrieving page " + k)
      (for (
        a <- TableQuery[Arxiv]
      ) yield {
        (a.arxivid, a.title ++ " - " ++ a.authors ++ " - " ++ a.journalref.getOrElse("") ++ " " ++ a.doi.getOrElse(""))
      }).drop(k * step).take(step).list
    }.map({
      case (i, t) => (i, 
          pandoc.latexToText(
              StringEscapeUtils.unescapeHtml4(
                  t.replaceAllLiterally("<author>", "")
                  .replaceAllLiterally("<keyname>", "")
                  .replaceAllLiterally("<forenames>", "")
                  .replaceAllLiterally("</author>", " ")
                  .replaceAllLiterally("</keyname>", " ")
                  .replaceAllLiterally("</forenames>", " ").replaceAll("<affiliation>[^<]*</affiliation>", "")).replaceAllLiterally("\n","")))
    })
    def arXivPaged = Iterator.from(0).map(arXivPage).takeWhile(_.nonEmpty).flatten

//    for ((identifier, citation) <- arXivPaged) {
//      println("arXiv:" + identifier)
//      println(citation)
//      articleCount = articleCount + 1
//      try {
//        for (w <- Tokenize(citation); if MRIdentifier.unapply(w).isEmpty) {
//          index.getOrElseUpdate(w, scala.collection.mutable.Set[String]()) += "arXiv:" + identifier
//        }
//      } catch {
//        case e: Exception => println("Exception while preparing citation for:\narXiv:" + identifier); e.printStackTrace()
//      }
//    }
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

  val store = db.getHashMap[String, Array[String]]("terms")

  var count = 0 

  val out = new PrintStream(new GZIPOutputStream(new FileOutputStream(termsFile)))
  for ((term, documents) <- index) {
    out.println(term)
    out.println(documents.toSeq.sorted.mkString(","))
    count += 1
    if (count % 1000 == 0) { println(s"Loaded $count terms"); db.commit }
    store.put(term, documents.toSeq.sorted.toArray)
  }
  out.close

  db.commit

  println(s" .. rebuilt index: $articleCount articles")

  val bucket = S3.bytes("citation-search")
  bucket("terms.gz") = Files.readAllBytes(Paths.get("terms.gz"))
  bucket("db") = Files.readAllBytes(Paths.get("db"))
  bucket("db.p") = Files.readAllBytes(Paths.get("db.p"))

}