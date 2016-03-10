package s2.counter.core

import java.text.SimpleDateFormat

import org.scalatest.{FlatSpec, Matchers}
import s2.counter.core.TimedQualifier.IntervalUnit

/**
  * Created by hsleep(honeysleep@gmail.com) on 2016. 3. 10..
  */
class TimedQualifierSpec extends FlatSpec with Matchers {
  val DateFormat = new SimpleDateFormat("yyyyMMddHHmm")

  "TimedQualifier" should "get qualifiers" in {
    val ts = 1457587599802l
    val tqs = TimedQualifier.getQualifiers(Seq(IntervalUnit.MINUTELY), ts)

    tqs.head.dateTime should equal(DateFormat.format(ts).toLong)
  }

  it should "get qualifiers by 10 minutely interval" in {
    val tqs = TimedQualifier.getQualifiersExtend(Seq("10m"), 1457586780000l)  // 2016.03.10 14:13

    tqs.head.dateTime should equal(201603101410l)
  }

  it should "get qualifiers by 6 minutely interval" in {
    val tqs = TimedQualifier.getQualifiersExtend(Seq("6m"), 1457586780000l)  // 2016.03.10 14:13

    tqs.head.dateTime should equal(201603101412l)
  }
}
