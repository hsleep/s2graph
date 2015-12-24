package com.kakao.s2graph.core.storage

import com.google.common.cache.Cache
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.types.{LabelWithDirection, VertexId}
import com.kakao.s2graph.core.utils.logger
import scala.collection.{Map, Seq}
import scala.concurrent.{Future, ExecutionContext}
import scala.util.Try

trait QueryBuilder[R, T] {

  def buildRequest(queryRequest: QueryRequest): R

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): T

  def fetch(queryRequest: QueryRequest,
            prevStepScore: Double,
            isInnerCall: Boolean,
            parentEdges: Seq[EdgeWithScore]): T

  def toCacheKeyBytes(request: R): Array[Byte]

  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]])
             (implicit ec: ExecutionContext): Future[Seq[QueryRequestWithResult]]

}
