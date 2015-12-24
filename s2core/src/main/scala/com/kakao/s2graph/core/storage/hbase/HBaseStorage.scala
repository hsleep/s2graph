package com.kakao.s2graph.core.storage.hbase

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{StorageDeserializable, StorageSerializable, Storage}
import com.kakao.s2graph.core.types.VertexId
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by hsleep on 2015. 12. 23..
  */
class HBaseStorage(config: Config)(implicit ex: ExecutionContext) extends Storage {
  val cacheOpt: Option[Cache[Integer, Seq[QueryResult]]] = None

  val vertexCacheOpt: Option[Cache[Integer, Option[Vertex]]] = None

  // Serializer/Deserializer
  override def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): StorageSerializable[SnapshotEdge] =
    new SnapshotEdgeSerializable(snapshotEdge)

  val snapshotEdgeDeserializer: StorageDeserializable[SnapshotEdge] = new SnapshotEdgeDeserializable

  override def indexEdgeSerializer(indexEdge: IndexEdge): StorageSerializable[IndexEdge] =
    new IndexEdgeSerializable(indexEdge)

  val indexEdgeDeserializer: StorageDeserializable[IndexEdge] = new IndexEdgeDeserializable

  override def vertexSerializer(vertex: Vertex): StorageSerializable[Vertex] =
    new VertexSerializable(vertex)

  val vertexDeserializer: StorageDeserializable[Vertex] = new VertexDeserializable

  private def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam): Future[QueryRequestWithResult] = {
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam)
    fetch(queryRequest, 1.0, isInnerCall = true, parentEdges = Nil)
  }

  override def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = Future.sequence {
    for {
      (srcVertex, tgtVertex, queryParam) <- params
    } yield getEdge(srcVertex, tgtVertex, queryParam)
  }

  override def flush(): Unit = ???

  private def writeAsyncSimple(zkQuorum: String, )
  // Interface
  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = ???

  override def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = ???

  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = throw new UnsupportedOperationException

  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = throw new UnsupportedOperationException

  private def fetch(queryRequest: QueryRequest, prevStepScore: Double, isInnerCall: Boolean, parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = ???

  // Interface
  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])
                      (implicit ec: ExecutionContext): Future[Seq[QueryRequestWithResult]] = Future.sequence {
    for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
      parentEdges <- prevStepEdges.get(queryRequest.vertex.id)
    } yield fetch(queryRequest, prevStepScore, isInnerCall = true, parentEdges)
  }

  override def deleteAllFetchedEdgesAsyncOld(queryRequestWithResult: QueryRequestWithResult, requestTs: Long, retryNum: Int): Future[Boolean] = throw new UnsupportedOperationException
}
