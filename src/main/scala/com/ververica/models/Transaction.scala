package com.ververica.models

import io.circe.generic.auto.*
import io.circe.parser.*
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.time.Instant
import scala.language.implicitConversions

case class Transaction(var t_time: Instant, var t_id: Long, var t_customer_id: Long, var t_amount: Long):
  // We need a public constructor without arguments, see
  // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/serialization/types_serialization/
  def this() =
    this(null, 0L, 0L, 0L)

  override def toString: String =
    s"Transaction(time:$t_time, id:$t_id, customer_id:$t_customer_id, amount:$t_amount)"

class TransactionDeserializer extends DeserializationSchema[Transaction]:
  override def isEndOfStream(customer: Transaction): Boolean = false

  override def getProducedType: TypeInformation[Transaction] =
    TypeInformation.of(classOf[Transaction])

  override def deserialize(bytes: Array[Byte]): Transaction =
    val decoded = decode[Transaction](new String(bytes))
    decoded.getOrElse(new Transaction())
