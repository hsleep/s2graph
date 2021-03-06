package com.daumkakao.s2graph.core.types2.v1

import com.daumkakao.s2graph.core.types2._

/**
 * Created by shon on 6/10/15.
 */

object EdgeQualifierInverted extends HBaseDeserializable {
  import HBaseType._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION1): (EdgeQualifierInverted, Int) = {
    val (tgtVertexId, numOfBytesUsed) = TargetVertexId.fromBytes(bytes, offset, len, version)
    (EdgeQualifierInverted(tgtVertexId), numOfBytesUsed)
  }
}
case class EdgeQualifierInverted(tgtVertexId: VertexId) extends EdgeQualifierInvertedLike {

  def bytes: Array[Byte] = {
    VertexId.toTargetVertexId(tgtVertexId).bytes
  }
}