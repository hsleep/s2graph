package test.controllers


import controllers.{QueryController, VertexController}
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

import scala.concurrent.Await

class VertexSpec extends SpecCommon {
  init()

  "vetex tc" should {
    "tc1" in {

      running(FakeApplication()) {
        val ids = (0 until 3).toList
        val (serviceName, columnName) = (testServiceName, testColumnName)

        val data = vertexInsertsPayload(serviceName, columnName, ids)
        val payload = Json.parse(Json.toJson(data).toString)
        println(payload)

        val jsResult = contentAsString(VertexController.tryMutates(payload, "insert",
          Option(serviceName), Option(columnName), withWait = true))
        Thread.sleep(asyncFlushInterval)

        val query = vertexQueryJson(serviceName, columnName, ids)
        val ret = contentAsJson(QueryController.getVerticesInner(query))
        println(">>>", ret)
        val fetched = ret.as[Seq[JsValue]]
        for {
          (d, f) <- data.zip(fetched)
        } yield {
          (d \ "id") must beEqualTo((f \ "id"))
          ((d \ "props") \ "age") must beEqualTo(((f \ "props") \ "age"))
        }
      }
      true
    }
  }
}
