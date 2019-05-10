package com.strangebee

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class MainSpec(_system: ActorSystem)
  extends TestKit(_system)
    with Matchers
    with WordSpecLike
    with BeforeAndAfterAll {

  def this() = this(ActorSystem("MainSpec"))

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def afterAll: Unit = {
    shutdown(system)
  }

  "parseDocument" should {
    "parse valid document" in {
      val expected = Document(42, "Arthur_Dent", "data-data-data")
      Await.result(Main.parseDocument("42:Arthur_Dent:data-data-data"), 1.minute) shouldBe expected
    }

    "fail to parse invalid document" in {
      a[IllegalArgumentException] should be thrownBy {
        Await.result(Main.parseDocument("--invalid--"), 1.minute)
      }
    }
  }

  "createDocument" should {
    "successfully create a document" in {
      val doc = Document(666, "Van Helsing", "beast")
      Await.result(Main.createDocument(doc), 1.minute) shouldEqual doc
    }
  }

  "documentExists" should {
    "return true if document exists" in {
      Await.result(Main.documentExists(2), 1.minute) shouldBe true
    }

    "return false if document doesn't exist" in {
      Await.result(Main.documentExists(1), 1.minute) shouldBe false
      Await.result(Main.documentExists(3), 1.minute) shouldBe false
    }
  }

  "readLineFromFile" should {
    "read file lines" in {
      val source = Main.readLineFromFile("src/test/resources/file1.txt")
      val probe = TestProbe()(system)
      source.runWith(Sink.seq).pipeTo(probe.ref)
      probe.expectMsg(3.seconds, Seq(
        "1:a:data-a",
        "2:b:data-b",
        "3:c:data-c",
        "4:d:data-d"))
    }
  }

  "parseAndCreateIfNotExists" should {
    "parse and create a document if it does not exist" in {
      val source = Source(List(
        "1:a:data-a",
        "2:b:data-b",
        "3:c:data-c",
        "4:d:data-d"))
      val f = source.runWith(Main.parseAndCreateIfNotExists)
      val result = Await.result(f, 3.seconds)
      result shouldEqual Vector(
        Document(1, "a", "data-a"),
        Document(3, "c", "data-c")
      )
    }
    "fail parsing invalid document and create valid one if it does not exist" in {
      val source = Source(List(
        "1:a:data-a",
        "2:b:data-b",
        "3-c:data-c",
        "4:d:data-d"))
      val probe = TestProbe()(system)
      source.runWith(Main.parseAndCreateIfNotExists).pipeTo(probe.ref)
      probe.expectMsg(3.seconds, Vector(
        Document(1, "a", "data-a")
      ))
    }
  }
}
