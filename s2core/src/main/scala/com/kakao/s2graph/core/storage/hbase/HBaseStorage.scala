package com.kakao.s2graph.core.storage.hbase

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{SKeyValue, StorageDeserializable, StorageSerializable, Storage}
import com.kakao.s2graph.core.types.VertexId
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.{Increment, Put, Delete, Mutation}
import org.apache.hadoop.hbase.util.Bytes

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

  private def writeAsyncSimple(zkQuorum: String, elementRpcs: Seq[Mutation], withWait: Boolean): Future[Boolean] = ???

  // Interface
  private def put(kvs: Seq[SKeyValue]): Seq[Mutation] = kvs.map { kv =>
    new Put(kv.row, kv.timestamp).addColumn(kv.cf, kv.qualifier, kv.timestamp, kv.value)
  }

  private def increment(kvs: Seq[SKeyValue]): Seq[Mutation] = kvs.map { kv =>
    new Increment(kv.row).addColumn(kv.cf, kv.qualifier, Bytes.toLong(kv.value))
  }

  private def delete(kvs: Seq[SKeyValue]): Seq[Mutation] = kvs.map { kv =>
    if (kv.qualifier == null) new Delete(kv.row, kv.timestamp).addFamily(kv.cf)
    else new Delete(kv.row, kv.timestamp).addColumn(kv.cf, kv.qualifier)
  }

  private def buildPutAsync(indexedEdge: IndexEdge): Seq[Mutation] =
    put(indexEdgeSerializer(indexedEdge).toKeyValues)

  private def buildDeleteAsync(indexedEdge: IndexEdge): Seq[Mutation] =
    delete(indexEdgeSerializer(indexedEdge).toKeyValues)

  private def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[Mutation] =
    put(snapshotEdgeSerializer(snapshotEdge).toKeyValues)

  private def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[Mutation] =
    delete(snapshotEdgeSerializer(snapshotEdge).toKeyValues)

  private def buildIncrementAsync(indexedEdge: IndexEdge, amount: Long = 1L): Seq[Mutation] =
    indexEdgeSerializer(indexedEdge).toKeyValues.headOption match {
      case None => Nil
      case Some(kv) =>
        val copiedKV = kv.copy(qualifier = Array.empty[Byte], value = Bytes.toBytes(amount))
        increment(Seq(copiedKV))
    }

  private def buildPutAsync(vertex: Vertex): Seq[Mutation] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    put(kvs)
  }


  def buildDeleteAsync(vertex: Vertex): Seq[Mutation] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head
    delete(Seq(kv.copy(qualifier = null)))
  }

  def buildDeleteBelongsToId(vertex: Vertex): Seq[Mutation] = {
    val kvs = vertexSerializer(vertex).toKeyValues
    val kv = kvs.head

    val newKVs = vertex.belongLabelIds.map { id =>
      kv.copy(qualifier = Bytes.toBytes(Vertex.toPropKey(id)))
    }
    delete(newKVs)
  }

  def buildVertexPutAsync(edge: Edge): Seq[Mutation] =
    if (edge.op == GraphUtil.operations("delete"))
      buildDeleteBelongsToId(edge.srcForVertex) ++ buildDeleteBelongsToId(edge.tgtForVertex)
    else
      buildPutAsync(edge.srcForVertex) ++ buildPutAsync(edge.tgtForVertex)

  private def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[Mutation] = {
    val deleteMutations = edgeMutate.edgesToDelete.flatMap(edge => buildDeleteAsync(edge))
    val insertMutations = edgeMutate.edgesToInsert.flatMap(edge => buildPutAsync(edge))

    deleteMutations ++ insertMutations
  }

  private def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[Mutation] =
    edgeMutate.newSnapshotEdge.map(e => buildPutAsync(e)).getOrElse(Nil)

  private def increments(edgeMutate: EdgeMutate): Seq[Mutation] = {
    (edgeMutate.edgesToDelete.isEmpty, edgeMutate.edgesToInsert.isEmpty) match {
      case (true, true) =>

        /** when there is no need to update. shouldUpdate == false */
        Nil
      case (true, false) =>

        /** no edges to delete but there is new edges to insert so increase degree by 1 */
        edgeMutate.edgesToInsert.flatMap { e => buildIncrementAsync(e) }
      case (false, true) =>

        /** no edges to insert but there is old edges to delete so decrease degree by 1 */
        edgeMutate.edgesToDelete.flatMap { e => buildIncrementAsync(e, -1L) }
      case (false, false) =>

        /** update on existing edges so no change on degree */
        Nil
    }
  }

  private def mutateEdgesInner(edges: Seq[Edge],
                               checkConsistency: Boolean,
                               withWait: Boolean)(f: (Option[Edge], Seq[Edge]) => (Edge, EdgeMutate)): Future[Boolean] = ???

  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = {
    val strongConsistency = edge.label.consistencyLevel == "strong"
    val edgeFuture =
      if (edge.op == GraphUtil.operations("delete") && !strongConsistency) {
        val zkQuorum = edge.label.hbaseZkAddr
        val (_, edgeUpdate) = Edge.buildDeleteBulk(None, edge)
        val mutations =
          indexedEdgeMutations(edgeUpdate) ++
            snapshotEdgeMutations(edgeUpdate) ++
            increments(edgeUpdate)
        writeAsyncSimple(zkQuorum, mutations, withWait)
      } else {
        mutateEdgesInner(Seq(edge), strongConsistency, withWait)(Edge.buildOperation)
      }

    val vertexFuture = writeAsyncSimple(edge.label.hbaseZkAddr, buildVertexPutAsync(edge), withWait)
    Future.sequence(Seq(edgeFuture, vertexFuture)).map { rets => rets.forall(identity) }
  }

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
