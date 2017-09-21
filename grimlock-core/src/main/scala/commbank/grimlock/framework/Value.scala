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

/** Trait for representing strutured data. */
trait Structured

/** Trait for variable values. */
trait Value[T] {
  /** The codec used to encode/decode this value. */
  val codec: Codec[T]

  /** The encapsulated value. */
  val value: T

  /** Return value as `Boolean`. */
  def asBoolean: Option[Boolean] = None

  /** Return value as `java.util.Date`. */
  def asDate: Option[Date] = None

  /** Return value as `Double`. */
  def asDouble: Option[Double] = None

  /** Return value as `Long`. */
  def asLong: Option[Long] = None

  /** Return value as `String`. */
  def asString: Option[String] = None

  /** Return value as `Type`. */
  def asType: Option[Type] = None

  /**
   * Compare this value with another.
   *
   * @param that The value to compare against.
   *
   * @return The returned value is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
   */
  def compare(that: T): Int = codec.compare(value, that)

  /**
   * Compare this value with another.
   *
   * @param that The `Value` to compare against.
   *
   * @return If that can be compared to this, then an `Option` where the value is < 0 iff value < that,
   *         0 iff value = that, > 0 iff value > that. A `None` in all other case.
   */
  def compare[V <: Value[_]](that: V): Option[Int]

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ[V <% Value[_]](that: V): Boolean = evaluate(that, Equal)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def geq[V <% Value[_]](that: V): Boolean = evaluate(that, GreaterEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def gtr[V <% Value[_]](that: V): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def leq[V <% Value[_]](that: V): Boolean = evaluate(that, LessEqual)

  /**
   * Check for for match with `that` regular expression.
   *
   * @param that Regular expression to match against.
   *
   * @note This always applies `toShortString` method before matching.
   */
  def like(that: Regex): Boolean = that.pattern.matcher(this.toShortString).matches

  /**
   * Check if `this` is less than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def lss[V <% Value[_]](that: V): Boolean = evaluate(that, Less)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq[V <% Value[_]](that: V): Boolean = !(this equ that)

  /** Return a consise (terse) string representation of a value. */
  def toShortString(): String = codec.encode(value)

  private def evaluate(that: Value[_], op: CompareResult): Boolean = compare(that) match {
    case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
    case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
    case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
    case _ => false
  }
}

/** Compantion object to the `Value` trait. */
object Value {
  /** Converts a `Boolean` to a `Value`. */
  implicit def booleanToValue(t: Boolean): BooleanValue = BooleanValue(t)

  /** Converts a `Double` to a `Value`. */
  implicit def doubleToValue(t: Double): DoubleValue = DoubleValue(t)

  /** Converts a `Int` to a `Value`. */
  implicit def intToValue(t: Int): LongValue = LongValue(t)

  /** Converts a `Long` to a `Value`. */
  implicit def longToValue(t: Long): LongValue = LongValue(t)

  /** Converts a `String` to a `Value`. */
  implicit def stringToValue(t: String): StringValue = StringValue(t)

  /** Converts a `Type` to a `Value`. */
  implicit def typeToValue[T <: Type](t: T): TypeValue = TypeValue(t)

  /**
   * Concatenates the string representation of two values.
   *
   * @param separator Separator to use between the strings.
   *
   * @return A function that concatenates values as a string.
   */
  def concatenate[X <: Value[_], Y <: Value[_]](separator: String): (X, Y) => String = (x, y) =>
    x.toShortString + separator + y.toShortString

  /**
   * Parse a value from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to decode with.
   *
   * @return A `Option[codec.V]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[codec.V] = codec.decode(str).map(codec.box(_))

  /**
   * Define an ordering between 2 values.
   *
   * @param ascending Indicator if ordering should be ascending or descending.
   */
  def ordering[V <: Value[_]](ascending: Boolean = true): Ordering[V] = new Ordering[V] {
    def compare(x: V, y: V): Int = (if (ascending) 1 else -1) * x
      .compare(y)
      .getOrElse(throw new Exception("Different types should not be possible"))
  }
}

/**
 * Value for when the data is of type `Boolean`.
 *
 * @param value A `Boolean`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class BooleanValue(value: Boolean, codec: Codec[Boolean] = BooleanCodec) extends Value[Boolean] {
  override def asBoolean: Option[Boolean] = Option(value)
  override def asDouble: Option[Double] = Option(if (value) 1 else 0)
  override def asLong: Option[Long] = Option(if (value) 1 else 0)

  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case BooleanValue(b, _) => Option(compare(b))
    case _ => None
  }
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codec: Codec[Date] = DateCodec()) extends Value[Date] {
  override def asDate: Option[Date] = Option(value)
  
  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case DateValue(d, _) => Option(compare(d))
    case _ => None
  }
}

/** Companion object to `DateValue` case class. */
object DateValue {
  /** Convenience constructure that returns a `DateValue` from a date and format string. */
  def apply(value: Date, format: String): DateValue = DateValue(value, DateCodec(format))
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DoubleValue(value: Double, codec: Codec[Double] = DoubleCodec) extends Value[Double] {
  override def asDouble: Option[Double] = Option(value)

  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case DoubleValue(d, _) => Option(compare(d))
    case _ => None
  }
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

  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case LongValue(l, _) => Option(compare(l))
    case DoubleValue(d, c) => Option(c.compare(value, d))
    case _ => None
  }
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StringValue(value: String, codec: Codec[String] = StringCodec) extends Value[String] {
  override def asString: Option[String] = Option(value)

  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case StringValue(s, _) => Option(compare(s))
    case _ => None
  }
}

/**
 * Value for when the data is of type `Type`.
 *
 * @param value A `Type`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class TypeValue(value: Type, codec: Codec[Type] = TypeCodec) extends Value[Type] {
  override def asType: Option[Type] = Option(value)

  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case TypeValue(t, _) => Option(compare(t))
    case _ => None
  }
}

/** Hetrogeneous comparison results. */
sealed private trait CompareResult
private case object GreaterEqual extends CompareResult
private case object Greater extends CompareResult
private case object Equal extends CompareResult
private case object Less extends CompareResult
private case object LessEqual extends CompareResult

