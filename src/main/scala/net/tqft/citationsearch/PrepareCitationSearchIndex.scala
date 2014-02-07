package net.tqft.citationsearch

import java.io.PrintStream
import java.io.FileOutputStream

object PrepareCitationSearchIndex extends App {
  import scala.slick.driver.MySQLDriver.simple._

  SQL { implicit session =>
    def articlesPage(k: Int) = {
      println("retrieving page " + k)
      (for (
        aux <- TableQuery[MathscinetAux]
      ) yield (aux.MRNumber, aux.textTitle ++ " - " ++ aux.textAuthors ++ " - " ++ aux.textCitation)).drop(k * 1000).take(1000).list
    }
    def articlesPaged = Iterator.from(0).map(articlesPage).takeWhile(_.nonEmpty).flatten

    val index = scala.collection.mutable.Map[String, scala.collection.mutable.Set[Int]]()

      for ((identifier, citation) <- articlesPaged) {
        try {
          for (w <- Search.tokenize(citation)) {
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
      out.println(documents.mkString(","))
    }
  }

}