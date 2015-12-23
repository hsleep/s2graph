package com.kakao.s2graph.core.storage.hbase

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{StorageDeserializable, StorageSerializable, Storage}
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

  override def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = Future.sequence {
    for {
      (srcVertex, tgtVertex, queryParam) <- params
    } yield getEdge(srcVertex, tgtVertex, queryParam)
  }

  override def flush(): Unit = ???

  // Interface
  private def getEdge(src: Vertex, tgt: Vertex, queryParam: QueryParam): Future[QueryRequestWithResult] = ???

  override def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = ???

  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = ???

  override def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] = ???

  override def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = ???

  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = ???

  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = ???
}
