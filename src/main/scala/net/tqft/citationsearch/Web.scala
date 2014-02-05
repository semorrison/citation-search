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

object Web {
  def main(args: Array[String]) {
    val port = Properties.envOrElse("PORT", "8080").toInt
    println("Starting on port:" + port)
    ServerBuilder()
      .codec(Http())
      .name("citationsearch")
      .bindTo(new InetSocketAddress(port))
      .build(new ResolverService)
    println("Started.")
  }
}

class ResolverService extends Service[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest): Future[HttpResponse] = {
    val response = Response()

    val parameters = new QueryStringDecoder(req.getUri()).getParameters
    import scala.collection.JavaConverters._
    val callback = Option(parameters.get("callback")).map(_.asScala.headOption).flatten
    val query = Option(parameters.get("q")).map(_.asScala.headOption).flatten.getOrElse("")
    val results = Search.query(query)

    response.setStatusCode(200)
    val json = results.map({ case (c, q) => f""" { title: "${c.title}", authors: "${c.authors}", cite: "${c.cite}", score: $q } """ }).mkString("{ results: [\n", ",\n", "]}")
    callback match {
      case Some(c) => {
        response.setContentType("application/javascript")
        response.contentString = c + "(" + json + ");"
      }
      case None => {
        response.setContentType("application/json")
        response.contentString = json
      }
    }

    Future(response)
  }
}