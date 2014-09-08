package net.tqft.citationsearch

import java.io.PrintStream
import java.io.FileOutputStream

object PrepareCitationSearchIndex extends App {
  import scala.slick.driver.MySQLDriver.simple._

  // Make sure to run SQLAuxApp first

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
      }).drop(k * 1000).take(1000).list
    }
    def articlesPaged = Iterator.from(0).map(articlesPage).takeWhile(_.nonEmpty).flatten

    val index = scala.collection.mutable.Map[String, scala.collection.mutable.Set[Int]]()

    for ((identifier, citation) <- articlesPaged) {
      try {
        for (w <- Search.tokenize(citation); if MRIdentifier.unapply(w).isEmpty) {
          index.getOrElseUpdate(w, scala.collection.mutable.Set[Int]()) += identifier
        }
      } catch {
        case e: Exception => println("Exception while preparing citation for:\n" + identifier); e.printStackTrace()
      }
    }

    println(index.size)

    val out = new PrintStream(new FileOutputStream("terms"))
    for ((term, documents) <- index) {
      out.println(term)
      out.println(documents.toSeq.sorted.mkString(","))
    }
  }

}