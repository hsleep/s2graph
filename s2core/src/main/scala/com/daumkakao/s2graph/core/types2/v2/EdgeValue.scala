package com.daumkakao.s2graph.core.types2.v2

import com.daumkakao.s2graph.core.types2._

/**
 * Created by shon on 6/10/15.
 */
object EdgeValue extends HBaseDeserializable {
  import HBaseType._
  import HBaseDeserializable._
  def fromBytes(bytes: Array[Byte],
                offset: Int,
                len: Int,
                version: String = VERSION2): (EdgeValue, Int) = {
    val (props, endAt) = bytesToKeyValues(bytes, offset, 0, version)
    (EdgeValue(props), endAt - offset)
  }
}
case class EdgeValue(props: Seq[(Byte, InnerValLike)]) extends EdgeValueLike {
  import HBaseSerializable._
  def bytes: Array[Byte] = propsToKeyValues(props)
}
