package com.kakao.s2graph.core.storage.hbase

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.mysqls.Model
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.types.{VertexId, HBaseType}
import com.kakao.s2graph.core._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.kakao.s2graph.core.utils.Configuration._
import play.api.libs.json.Json

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 30..
  */
class HBaseStorageTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  import ExecutionContext.Implicits.global

  val config = ConfigFactory.load().withFallback(Graph.DefaultConfig)
  Model(config)

  val vertexCache = CacheBuilder.newBuilder().maximumSize(config.getInt("cache.max.size")).build[java.lang.Integer, Option[Vertex]]()
  val asyncStorage = new AsynchbaseStorage(config, vertexCache)
  val hbaseStorage = new HBaseStorage(config)
  val requestParser = new RequestParser(config)

  val service = "test"
  val labelName = "test_storage"

  override def beforeAll: Unit ={
    if (Management.findService(service).isEmpty) {
      Management.createService(service, config.getOrElse("hbase.zookeeper.quorum", "localhost"), service, 1, None, "gz")
    }
    if (Management.findLabel(labelName).isEmpty) {
      val prop1 = Management.JsonModel.Prop("prop1", "", "string")
      val prop2 = Management.JsonModel.Prop("prop1", "0", "long")
      Management.createLabel(labelName, service, "userid", "string", service, "userid", "string", true, service, Nil, Seq(prop1, prop2), "weak", None, None, HBaseType.DEFAULT_VERSION, false, "gz")
    }
  }

  override def afterAll: Unit = {
    Management.deleteLabel(labelName)
    Management.deleteService(service)
  }

  "HBaseStorage" should "fetch correctly" in {
    val edge = Management.toEdge(100, "insert", "0", "1", labelName, "out", """{"prop1": "name_0", "prop2": 30}""")
    asyncStorage.mutateEdges(Seq(edge), true)

    val js = Json.obj (
      "srcVertices" -> Json.arr (
        Json.obj (
          "serviceName" -> service,
          "columnName" -> "userid",
          "ids" -> Json.arr (
            "0"
          )
        )
      ),
      "steps" -> Json.arr (
        Json.obj (
          "step" -> Json.arr (
            Json.obj (
              "label" -> labelName
            )
          )
        )
      )
    )
    val q = requestParser.toQuery(js, true)
    val queryReq = QueryRequest(q, 0, q.vertices.head, q.steps.head.queryParams.head)
    val result = Await.result(hbaseStorage.fetches(Seq((queryReq, 1.0)), Map.empty[VertexId, Seq[EdgeWithScore]]), 10 seconds)

    result.head.queryResult.edgeWithScoreLs.head.edge should equal(edge)
  }
}
