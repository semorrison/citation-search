package net.tqft.citationsearch

import org.jboss.netty.handler.codec.http.{ HttpRequest, HttpResponse }
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.{ Http, Response }
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.InetSocketAddress
import util.Properties
import java.net.URI
import org.jboss.netty.handler.codec.http.QueryStringDecoder
import java.net.URL
import scala.io.Source
import argonaut._
import Argonaut._

object Web {
  def main(args: Array[String]) {
    val port = Properties.envOrElse("PORT", "8080").toInt
    try {
      println("Starting on port: " + port)

      ServerBuilder()
        .codec(Http())
        .name("citationsearch")
        .bindTo(new InetSocketAddress(port))
        .build(new ResolverService)

      println("Started citation-search.")

    } catch {
      case e: Throwable => e.printStackTrace
    }

    try {
      println("Starting https on port: 443")

      ServerBuilder()
        .tls("./cert", "./key")
        .codec(Http())
        .name("citationsearch over https")
        .bindTo(new InetSocketAddress(443))
        .build(new ResolverService)

      println("Started citation-search over https.")

    } catch {
      case e: Throwable => e.printStackTrace
    }

  }

}

object Throttle {
  private var lastQuery = System.currentTimeMillis()
  def apply() {
    if (System.currentTimeMillis() - lastQuery < 500) Thread.sleep(1000)
  }
  lastQuery = System.currentTimeMillis()
}

class ResolverService extends Service[HttpRequest, HttpResponse] {
  //  Future(Search.query("warming up ..."))

//  Throttle()
  
  def apply(req: HttpRequest): Future[HttpResponse] = {
    val response = Response()

    val parameters = new QueryStringDecoder(req.getUri()).getParameters
    import scala.collection.JavaConverters._
    val callback = Option(parameters.get("callback")).map(_.asScala.headOption).flatten
    val query = Option(parameters.get("q")).map(_.asScala.headOption).flatten.getOrElse("")
    val results = Search.query(query)

    response.setStatusCode(200)

    val json = {
      import argonaut._, Argonaut._

      try {
        results.asJson.spaces2
      } catch {
        case e: Exception => {
          println("Except while writing JSON for: ")
          println(results)
          throw e
        }
      }
    }
    //    val json = results.map({ case (c, q) => f"""   { "MRNumber": ${c.MRNumber}, "title": "${c.title}", "authors": "${c.authors}", "cite": "${c.cite}", "url": "${c.url}", ${c.pdf.map(p => f""""pdf": "$p", """).getOrElse("")}${c.free.map(f => f""""free": "$f", """).getOrElse("")}"best": "${c.best}", "score": $q } """.replaceAllLiterally("\\", "\\\\") }).mkString(s"""{ "query": "$query",\n  "results": [\n""", ",\n", "  ]\n}")
    callback match {
      case Some(c) => {
        response.setContentType("application/javascript")
        response.contentString = c + "(" + json + ");"
      }
      case None => {
        response.headers.set("Access-Control-Allow-Origin", "*")        
        response.setContentType("application/json")
        response.contentString = json
      }
    }

    Future(response)
  }
}