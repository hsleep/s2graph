//package controllers
//
//import com.kakao.s2graph.core.Graph
//import com.kakao.s2graph.core.rest.RestCaller
//import com.typesafe.config.ConfigFactory
//import play.api.libs.json._
//import play.api.test.FakeApplication
//import play.api.test.Helpers._
//
//import scala.concurrent.Await
//import scala.concurrent.duration.Duration
//
//class VertexSpec extends SpecCommon {
//  //  init()
//
//  "vetex tc" should {
//    "tc1" in {
//
//      running(FakeApplication()) {
//        val config = ConfigFactory.load()
//        val graph = new Graph(config)
//        val rest = new RestCaller(graph)
//
//        val ids = (7 until 20).map(tcNum => tcNum * 1000 + 0)
//
//        val (serviceName, columnName) = (testServiceName, testColumnName)
//
//        val data = vertexInsertsPayload(serviceName, columnName, ids)
//        val payload = Json.parse(Json.toJson(data).toString)
//        println(payload)
//
//        val jsResult = contentAsString(VertexController.tryMutates(payload, "insert",
//          Option(serviceName), Option(columnName), withWait = true))
//
//        val query = vertexQueryJson(serviceName, columnName, ids)
//        val res = rest.uriMatch("/graphs/getVertices", query)
//
//
//        val ret = Await.result(res, Duration("30 seconds"))
//
//        println(">>>", ret)
//        val fetched = ret.as[Seq[JsValue]]
//        for {
//          (d, f) <- data.zip(fetched)
//        } yield {
//          (d \ "id") must beEqualTo((f \ "id"))
//          ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
//        }
//      }
//      true
//    }
//  }
//}
