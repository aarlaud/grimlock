// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.framework.encoding

import commbank.grimlock.framework.metadata.Type

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Try

import shapeless.{ :+:, CNil, Coproduct, Poly1 }
import shapeless.ops.coproduct.Inject

/** Trait for encoding/decoding (basic) data types. */
trait Codec[D] { self =>
  /** The value (boxed data) type. */
  type V <: Value[D]

  def box(value: D): V // TODO: Is this needed?

  /**
   * Compare two value objects.
   *
   * @param x The first value to compare.
   * @param y The second value to compare.
   *
   * @return The returned value is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
   */
  def compare(x: D, y: D): Int

  /**
   * Decode a basic data type.
   *
   * @param str String to decode.
   *
   * @return `Some[D]` if the decode was successful, `None` otherwise.
   */
  def decode(str: String): Option[D]

  /**
   * Converts a value to a consise (terse) string.
   *
   * @param value The value to convert to string.
   *
   * @return Short string representation of the value.
   */
  def encode(value: D): String

  /** Return a consise (terse) string representation of a codec. */
  def toShortString: String

  /** Return an optional date type class for this data type. */
  def date: Option[D => Date] = None

  /** Return an optional intergal type class for this data type. */
  def integral: Option[Integral[D]] = None

  /** Return an optional numeric type class for this data type. */
  def numeric: Option[Numeric[D]] = None

  /** Return an optional Ordering for this data type. */
  def ordering: Option[Ordering[D]] = None
}

/** Companion object to the `Codec` trait. */
object Codec {
  /** Default coproduct for codecs. */
  type DefaultCodecs = BooleanCodec.type :+:
    DateCodec :+:
    DoubleCodec.type :+:
    LongCodec.type :+:
    StringCodec.type :+:
    TypeCodec.type :+:
    CNil

  /**
   * Parse a codec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[C]` in case of success, `None` otherwise.
   */
  def fromShortString[
    C <: Coproduct
  ](
    str: String
  )(implicit
    ev1: Inject[C, BooleanCodec.type],
    ev2: Inject[C, DateCodec],
    ev3: Inject[C, DoubleCodec.type],
    ev4: Inject[C, LongCodec.type],
    ev5: Inject[C, StringCodec.type],
    ev6: Inject[C, TypeCodec.type]
  ): Option[C] = str match {
    case BooleanCodec.Pattern() => BooleanCodec.fromShortString(str).map(Coproduct(_))
    case DateCodec.Pattern(_) => DateCodec.fromShortString(str).map(Coproduct(_))
    case DoubleCodec.Pattern() => DoubleCodec.fromShortString(str).map(Coproduct(_))
    case LongCodec.Pattern() => LongCodec.fromShortString(str).map(Coproduct(_))
    case StringCodec.Pattern() => StringCodec.fromShortString(str).map(Coproduct(_))
    case TypeCodec.Pattern() => TypeCodec.fromShortString(str).map(Coproduct(_))
    case _ => None
  }
}

/** Codec for dealing with `java.util.Date`. */
case class DateCodec(format: String = "yyyy-MM-dd") extends Codec[Date] { self =>
  type V = DateValue

  def box(value: Date): DateValue = DateValue(value, this)

  def compare(x: Date, y: Date): Int = x.getTime().compare(y.getTime())

  def decode(value: String): Option[Date] = Try(df.parse(value)).toOption

  def encode(value: Date): String = df.format(value)

  def toShortString = s"date(${format})"

  override def date: Option[Date => Date] = Option(identity)
  override def ordering: Option[Ordering[Date]] = Option(
    new Ordering[Date] { def compare(x: Date, y: Date): Int = self.compare(x, y) }
  )

  private def df: SimpleDateFormat = new SimpleDateFormat(format)
}

/** Companion object to DateCodec. */
object DateCodec {
  /** Pattern for parsing `DateCodec` from string. */
  val Pattern = """date\((.*)\)""".r

  /**
   * Parse a DateCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[DateCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[DateCodec] = str match {
    case Pattern(format) => Option(DateCodec(format))
    case _ => None
  }
}

/** Codec for dealing with `String`. */
case object StringCodec extends Codec[String] {
  type V = StringValue

  /** Pattern for parsing `StringCodec` from string. */
  val Pattern = "string".r

  def box(value: String): StringValue = StringValue(value, this)

  def compare(x: String, y: String): Int = x.compare(y)

  def decode(str: String): Option[String] = Option(str)

  def encode(value: String): String = value

  /**
   * Parse a StringCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[StringCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[StringCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = "string"

  override def ordering: Option[Ordering[String]] = Option(Ordering.String)
}

/** Codec for dealing with `Double`. */
case object DoubleCodec extends Codec[Double] {
  type V = DoubleValue

  /** Pattern for parsing `DoubleCodec` from string. */
  val Pattern = "double".r

  def box(value: Double): DoubleValue = DoubleValue(value, this)

  def compare(x: Double, y: Double): Int = x.compare(y)

  def decode(str: String): Option[Double] = Try(str.toDouble).toOption

  def encode(value: Double): String = value.toString

  /**
   * Parse a DoubleCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[DoubleCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[DoubleCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = "double"

  override def numeric: Option[Numeric[Double]] = Option(Numeric.DoubleIsFractional)
  override def ordering: Option[Ordering[Double]] = Option(Ordering.Double)
}

/** Codec for dealing with `Long`. */
case object LongCodec extends Codec[Long] {
  type V = LongValue

  /** Pattern for parsing `LongCodec` from string. */
  val Pattern = "long|int|short".r

  def box(value: Long): LongValue = LongValue(value, this)

  def compare(x: Long, y: Long): Int = x.compare(y)

  def decode(str: String): Option[Long] = Try(new BigDecimal(str.trim).longValueExact).toOption

  def encode(value: Long): String = value.toString

  /**
   * Parse a LongCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[LongCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[LongCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = "long"

  override def integral: Option[Integral[Long]] = Option(Numeric.LongIsIntegral)
  override def numeric: Option[Numeric[Long]] = Option(Numeric.LongIsIntegral)
  override def ordering: Option[Ordering[Long]] = Option(Ordering.Long)
}

/** Codec for dealing with `Boolean`. */
case object BooleanCodec extends Codec[Boolean] {
  type V = BooleanValue

  /** Pattern for parsing `BooleanCodec` from string. */
  val Pattern = "boolean".r

  def box(value: Boolean): BooleanValue = BooleanValue(value, this)

  def compare(x: Boolean, y: Boolean): Int = x.compare(y)

  def decode(str: String): Option[Boolean] = Try(str.toBoolean).toOption

  def encode(value: Boolean): String = value.toString

  /**
   * Parse a BooleanCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[BooleanCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[BooleanCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = "boolean"

  override def ordering: Option[Ordering[Boolean]] = Option(Ordering.Boolean)
}

/** Codec for dealing with `Type`. */
case object TypeCodec extends Codec[Type] {
  type V = TypeValue

  /** Pattern for parsing `TypeCodec` from string. */
  val Pattern = "type".r

  def box(value: Type): TypeValue = TypeValue(value, this)

  def compare(x: Type, y: Type): Int = x.toString.compare(y.toString)

  def decode(str: String): Option[Type] = Type.fromShortString(str)

  def encode(value: Type): String = value.toShortString

  /**
   * Parse a TypeCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[TypeCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[TypeCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = "type"
}

// TODO: can CompareTuple, DecodeString and EncodeString be defined in terms of Codec[D] only?

object CompareTuple extends Poly1 {
  //implicit def goD[D] = at[((D, D), Codec[D])] { case ((x, y), c) => c.compare(x, y) }

  implicit def goDouble = at[((Double, Double), Codec[Double])] { case ((x, y), c) => c.compare(x, y) }
  implicit def goFloat = at[((Float, Float), Codec[Double])] { case ((x, y), c) => c.compare(x, y) }
  implicit def goInt = at[((Int, Int), Codec[Long])] { case ((x, y), c) => c.compare(x, y) }
  implicit def goLong = at[((Long, Long), Codec[Long])] { case ((x, y), c) => c.compare(x, y) }
  implicit def goShort = at[((Short, Short), Codec[Long])] { case ((x, y), c) => c.compare(x, y) }
  implicit def goString = at[((String, String), Codec[String])] { case ((x, y), c) => c.compare(x, y) }

  implicit def goDoubleCodec = at[((Double, Double), DoubleCodec.type)] { case ((x, y), c) => c.compare(x, y) }
  implicit def goFloatCodec = at[((Float, Float), DoubleCodec.type)] { case ((x, y), c) => c.compare(x, y) }
  implicit def goIntCodec = at[((Int, Int), LongCodec.type)] { case ((x, y), c) => c.compare(x, y) }
  implicit def goLongCodec = at[((Long, Long), LongCodec.type)] { case ((x, y), c) => c.compare(x, y) }
  implicit def goShortCodec = at[((Short, Short), LongCodec.type)] { case ((x, y), c) => c.compare(x, y) }
  implicit def goStringCodec = at[((String, String), StringCodec.type)] { case ((x, y), c) => c.compare(x, y) }
}

object DecodeString extends Poly1 {
  implicit def go[D] = at[(Codec[D], String)] { case (c, s) => c.decode(s) }

  implicit def goDouble = at[(DoubleCodec.type, String)] { case (c, s) => c.decode(s) }
  implicit def goLong = at[(LongCodec.type, String)] { case (c, s) => c.decode(s) }
  implicit def goString = at[(StringCodec.type, String)] { case (c, s) => c.decode(s) }
}

object EncodeString extends Poly1 {
  //implicit def goD[D] = at[(Codec[D], D)] { case (c, d) => c.encode(d) }

  implicit def goDouble = at[(Codec[Double], Double)] { case (c, d) => c.encode(d) }
  implicit def goFloat = at[(Codec[Double], Float)] { case (c, d) => c.encode(d) }
  implicit def goInt = at[(Codec[Long], Int)] { case (c, d) => c.encode(d) }
  implicit def goLong = at[(Codec[Long], Long)] { case (c, d) => c.encode(d) }
  implicit def goShort = at[(Codec[Long], Short)] { case (c, d) => c.encode(d) }
  implicit def goString = at[(Codec[String], String)] { case (c, d) => c.encode(d) }

  implicit def goDoubleCodec = at[(DoubleCodec.type, Double)] { case (c, d) => c.encode(d) }
  implicit def goFloatCodec = at[(DoubleCodec.type, Float)] { case (c, d) => c.encode(d) }
  implicit def goIntCodec = at[(LongCodec.type, Int)] { case (c, d) => c.encode(d) }
  implicit def goLongCodec = at[(LongCodec.type, Long)] { case (c, d) => c.encode(d) }
  implicit def goShortCodec = at[(LongCodec.type, Short)] { case (c, d) => c.encode(d) }
  implicit def goStringCodec = at[(StringCodec.type, String)] { case (c, d) => c.encode(d) }
}

