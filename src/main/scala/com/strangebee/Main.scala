package com.strangebee

import java.nio.file.Paths

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ Await, Future }

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ FileIO, Flow, Framing, Keep, Sink, Source }
import akka.stream.{ ActorMaterializer, IOResult }
import akka.util.ByteString

case class Document(id: Int, name: String, content: String)

object Main extends App {
  /**
    * Check if the document exists in the remote system
    *
    * @param id ID of the document
    * @return Future(true) if it exists, Future(false) otherwise
    */
  def documentExists(id: Int): Future[Boolean] = {
    // The real method uses external API call
    // For test, it replies with basic logic
    Future.successful(id % 2 == 0)
  }

  /**
    * Parse a string an create an instance of Document if the syntax is valid
    * The input format is id:name:info. Id must be numeric
    * If the parsing fails, return a failed future
    *
    * @param line string representation of a document
    * @return The parsed document
    */
  def parseDocument(line: String): Future[Document] = {
    /* Regular expression for the document format */
    val documentFormat = "(\\d+):(\\w+):(.*)".r
    line match {
      case documentFormat(id, name, data) => Future.successful(Document(id.toInt, name, data))
      case _ => Future.failed(new IllegalArgumentException(s"""Invalid syntax for $line. It should match: "id:name:info""""))
    }
  }

  /**
    * Create a document in the remote system
    *
    * @param document the document to create
    * @return the created document if the operation succeeds, a failed future otherwise
    */
  def createDocument(document: Document): Future[Document] = {
    // The real method uses external API call
    // For test, it always succeeds
    println(s"createDocument($document)")
    Future.successful(document)
  }

  /**
    * A stream sink that parse document, check if it already exists and create it doesn't
    *
    * @return a stream sink
    */
  def parseAndCreateIfNotExists: Sink[String, Future[Seq[Document]]] =
    Flow[String]
      .mapAsync(4) { line =>
        for {
          doc <- parseDocument(line)
          exists <- documentExists(doc.id)
          newOrExistingDoc <- if (exists) Future.successful(doc) else createDocument(doc)
        } yield newOrExistingDoc
      }
      .toMat(Sink.seq)(Keep.right)

  /**
    * Read file and return a stream of its lines
    *
    * @param filename the name of the file to read
    * @return stream source of file lines
    */
  def readLineFromFile(filename: String): Source[String, Future[IOResult]] =
    FileIO
      .fromPath(Paths.get(filename))
      .via(Framing.delimiter(ByteString("\n"), 1024, true))
      .map(_.utf8String)


  /*== Main =====================*/
  implicit val system: ActorSystem = ActorSystem("stream-eval")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  try {
    args.headOption match {
      case Some(filename) =>
        val processDone = readLineFromFile(filename).runWith(parseAndCreateIfNotExists)
        Await.result(processDone, 1.minutes).foreach { doc =>
          println(s" => $doc")
        }
      case None => println("You must specify the name of a file in the first parameter")
    }

  } finally {
    system.terminate()
  }
}
