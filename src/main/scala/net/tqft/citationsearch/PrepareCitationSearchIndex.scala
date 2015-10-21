package net.tqft.citationsearch

import java.io.PrintStream
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream
import java.io.File
import org.mapdb.DBMaker
import net.tqft.toolkit.amazon.S3
import java.nio.file.Files
import java.nio.file.Paths

object PrepareCitationSearchIndex extends App {
  import scala.slick.driver.MySQLDriver.simple._

  // Make sure to run SQLAuxApp first

  val index = scala.collection.mutable.Map[String, scala.collection.mutable.Set[Int]]()

  var articleCount = 0

  val step = 5000
  
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

    for ((identifier, citation) <- articlesPaged) {
      articleCount = articleCount + 1
      try {
        for (w <- Search.tokenize(citation); if MRIdentifier.unapply(w).isEmpty) {
          index.getOrElseUpdate(w, scala.collection.mutable.Set[Int]()) += identifier
        }
      } catch {
        case e: Exception => println("Exception while preparing citation for:\n" + identifier); e.printStackTrace()
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

  val store = db.getHashMap[String, Array[Int]]("terms")

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