package com.ververica.models

import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.time.LocalDate
import scala.language.implicitConversions

case class Customer(var c_id: Long, var c_name: String, var c_birthday: LocalDate):
  def this() =
    this(0L, "", null)

  override def toString: String = s"Customer(id:$c_id, name:$c_name, birthday:$c_birthday)"

class CustomerDeserializer extends DeserializationSchema[Customer]:
  override def isEndOfStream(customer: Customer): Boolean = false

  override def getProducedType: TypeInformation[Customer] =
    TypeInformation.of(classOf[Customer])

  override def deserialize(bytes: Array[Byte]): Customer =
    val decoded = decode[Customer](new String(bytes))
    decoded.getOrElse(new Customer())
