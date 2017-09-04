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

import java.util.Date

import scala.util.matching.Regex

/** Trait for variable values. */
trait Value[D] {
  /** The encapsulated value. */
  val value: D

  /** The codec used to encode/decode this value. */
  val codec: Codec[D]

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ[T](that: Value[T]): Boolean = evaluate(that, Equal)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq[T](that: Value[T]): Boolean = !(this equ that)

  /**
   * Check for for match with `that` regular expression.
   *
   * @param that Regular expression to match against.
   *
   * @note This always applies `toShortString` method before matching.
   */
  def like(that: Regex): Boolean = that.pattern.matcher(this.toShortString).matches

  // Note: These next 4 methods implement comparison in a non-standard way when comparing two objects that can't be
  //       compared. In such cases the result is always false. This is the desired behaviour for the 'which' method
  //       (i.e. a filter).

  /**
   * Check if `this` is less than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def lss[T](that: Value[T]): Boolean = evaluate(that, Less)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def leq[T](that: Value[T]): Boolean = evaluate(that, LessEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def gtr[T](that: Value[T]): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def geq[T](that: Value[T]): Boolean = evaluate(that, GreaterEqual)

  /** Return value as `java.util.Date`. */
  def asDate: Option[Date] = None

  /** Return value as `String`. */
  def asString: Option[String] = None

  /** Return value as `Double`. */
  def asDouble: Option[Double] = None

  /** Return value as `Long`. */
  def asLong: Option[Long] = None

  /** Return value as `Boolean`. */
  def asBoolean: Option[Boolean] = None

  /** Return value as `Type`. */
  def asType: Option[Type] = None

  // TODO: can toShortString and as* be removed?

  /** Return a consise (terse) string representation of a value. */
  def toShortString: String = codec.encode(value)

  protected def compare[T](that: Value[T]): Option[Int]

  private def evaluate[T](that: Value[T], op: CompareResult): Boolean = compare(that) match {
    case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
    case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
    case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
    case _ => false
  }
}

/** Compantion object to the `Value` trait. */
object Value {
  // TODO: can concatenate and fromShortString be removed?

  /**
   * Concatenates the string representation of two values.
   *
   * @param separator Separator to use between the strings.
   *
   * @return A function that concatenates values as a string.
   */
  def concatenate(separator: String): (Value[_], Value[_]) => StringValue = (left: Value[_], right: Value[_]) =>
    left.toShortString + separator + right.toShortString

  /**
   * Parse a value from string.
   *
   * @param codec The codec to decode with.
   * @param str   The string to parse.
   *
   * @return A `Some[Value]` if successful, `None` otherwise.
   */
  def fromShortString[D](codec: Codec[D], str: String): Option[codec.V] = codec.decode(str).map(codec.box(_))

  /** Converts a `String` to a `Value`. */
  implicit def stringToValue(t: String): StringValue = StringValue(t)

  /** Converts a `Double` to a `Value`. */
  implicit def doubleToValue(t: Double): DoubleValue = DoubleValue(t)

  /** Converts a `Long` to a `Value`. */
  implicit def longToValue(t: Long): LongValue = LongValue(t)

  /** Converts a `Int` to a `Value`. */
  implicit def intToValue(t: Int): LongValue = LongValue(t)

  /** Converts a `Boolean` to a `Value`. */
  implicit def booleanToValue(t: Boolean): BooleanValue = BooleanValue(t)

  /** Converts a `Type` to a `Value`. */
  implicit def typeToValue[T <: Type](t: T): TypeValue = TypeValue(t)
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codec: Codec[Date] = DateCodec()) extends Value[Date] {
  override def asDate: Option[Date] = Option(value)

  protected def compare[T](that: Value[T]): Option[Int] = that.asDate.map(d => codec.compare(value, d))
}

/** Companion object to `DateValue` case class. */
object DateValue {
  /** Convenience constructure that returns a `DateValue` from a date and format string. */
  def apply(value: Date, format: String): DateValue = DateValue(value, DateCodec(format))
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StringValue(value: String, codec: Codec[String] = StringCodec) extends Value[String] {
  override def asString: Option[String] = Option(value)

  protected def compare[T](that: Value[T]): Option[Int] = that.asString.map(s => codec.compare(value, s))
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DoubleValue(value: Double, codec: Codec[Double] = DoubleCodec) extends Value[Double] {
  override def asDouble: Option[Double] = Option(value)

  protected def compare[T](that: Value[T]): Option[Int] = that.asDouble.map(d => codec.compare(value, d))
}

/**
 * Value for when the data is of type `Long`.
 *
 * @param value A `Long`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class LongValue(value: Long, codec: Codec[Long] = LongCodec) extends Value[Long] {
  override def asDouble: Option[Double] = Option(value)
  override def asLong: Option[Long] = Option(value)

  protected def compare[T](that: Value[T]): Option[Int] = that match {
    case LongValue(l, _) => Option(codec.compare(value, l))
    case DoubleValue(d, c) => Option(c.compare(value, d))
    case _ => None
  }
}

/**
 * Value for when the data is of type `Boolean`.
 *
 * @param value A `Boolean`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class BooleanValue(value: Boolean, codec: Codec[Boolean] = BooleanCodec) extends Value[Boolean] {
  override def asDouble: Option[Double] = Option(if (value) 1 else 0)
  override def asLong: Option[Long] = Option(if (value) 1 else 0)
  override def asBoolean: Option[Boolean] = Option(value)

  protected def compare[T](that: Value[T]): Option[Int] = that.asBoolean.map(b => codec.compare(value, b))
}

/**
 * Value for when the data is of type `Type`.
 *
 * @param value A `Type`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class TypeValue(value: Type, codec: Codec[Type] = TypeCodec) extends Value[Type] {
  override def asType: Option[Type] = Option(value)

  protected def compare[T](that: Value[T]): Option[Int] = that.asType.map(t => codec.compare(value, t))
}

/** Hetrogeneous comparison results. */
sealed private trait CompareResult
private case object GreaterEqual extends CompareResult
private case object Greater extends CompareResult
private case object Equal extends CompareResult
private case object Less extends CompareResult
private case object LessEqual extends CompareResult

