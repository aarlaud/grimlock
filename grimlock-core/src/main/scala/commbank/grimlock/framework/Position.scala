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

package commbank.grimlock.framework.position

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.encoding.{ Codec, CompareTuple, DecodeString, EncodeString }
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner

import play.api.libs.json.{ JsArray, JsError, Json, JsResult, JsString, JsSuccess, JsValue, Reads }

import scala.util.matching.Regex

import shapeless.{ ::, =:!=, HList, HNil, IsDistinctConstraint, LUBConstraint, Poly1, Nat }
import shapeless.ops.hlist.{
  At,
  ConstMapper,
  FlatMapper,
  IsHCons,
  Mapper,
  Prepend,
  ReplaceAt,
  Split,
  ToTraversable,
  Zip,
  ZipWithIndex
}
import shapeless.ops.nat.ToInt

/**
 * Case class for the position of data in the matrix.
 *
 * @param coordinates List of coordinates of the position.
 */
case class Position[P <: HList](coordinates: P) {
  /**
   * Append a coordinate to the position.
   *
   * @param value The coordinate to append.
   *
   * @return A new position with the coordinate `value` appended.
   */
  def append[
    V,
    Out <: HList
  ](
    value: V
  )(implicit
    ev: Prepend.Aux[P, V :: HNil, Out]
  ): Position[Out] = Position(coordinates :+ value)

  /**
   * Return the coordinate at dimension (index) `dim`.
   *
   * @param dimimension Dimension of the coordinate to get.
   */
  def apply[D <: Nat, V](dimension: D)(implicit ev: At.Aux[P, D, V]): V = coordinates.at[D]

  /**
   * Insert a coordinate into the position.
   *
   * @param dimimension The dimension to insert at.
   * @param value       The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` inserted.
   */
  def insert[
    D <: Nat,
    V,
    Pre <: HList,
    Suf <: HList,
    Out <: HList
  ](
    dimension: D,
    value: V
  )(implicit
    ev1: Split.Aux[P, D, Pre, Suf],
    ev2: Prepend.Aux[Pre, V :: Suf, Out]
  ): Position[Out] = {
    val (prefix, suffix) = coordinates.split[D]

    Position(prefix ::: value :: suffix)
  }

  /**
   * Melt dimension `dimension` into `into`.
   *
   * @param dimension The dimension to remove.
   * @param into      The dimension into which to melt.
   * @param merge     The function to use for merging coordinates
   *
   * @return A new position with dimension `dimension` melted into `into`.
   */
  def melt[
    D <: Nat,
    I <: Nat,
    X,
    Y,
    Z,
    Rep <: HList,
    Pre <: HList,
    Suf <: HList,
    Tai <: HList,
    Out <: HList
  ](
    dimension: D,
    into: I,
    merge: (X, Y) => Z
  )(implicit
    ev1: At.Aux[P, I, X],
    ev2: At.Aux[P, D, Y],
    ev3: ReplaceAt.Aux[P, I, Z, (X, Rep)],
    ev4: Split.Aux[Rep, D, Pre, Suf],
    ev5: IsHCons.Aux[Suf, _, Tai],
    ev6: Prepend.Aux[Pre, Tai, Out],
    ev7: IsDistinctConstraint[D :: I :: HNil] // shapeless.=:!= doesn't serialise
  ): Position[Out] = {
    val (prefix, suffix) = coordinates.updatedAt[I](merge(coordinates.at[I], coordinates.at[D])).split[D]

    Position(prefix ++ suffix.tail)
  }

  /**
   * Prepend a coordinate to the position.
   *
   * @param value The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def prepend[V](value: V): Position[V :: P] = Position(value :: coordinates)

  /**
   * Remove the coordinate at dimension `dimension`.
   *
   * @param dimension The dimension to remove.
   *
   * @return A new position with dimension `dimension` removed.
   */
  def remove[
    D <: Nat,
    Pre <: HList,
    Suf <: HList,
    Tai <: HList,
    Out <: HList
  ](
    dimension: D
  )(implicit
    ev1: Split.Aux[P, D, Pre, Suf],
    ev2: IsHCons.Aux[Suf, _, Tai],
    ev3: Prepend.Aux[Pre, Tai, Out]
  ): Position[Out] = {
    val (prefix, suffix) = coordinates.split[D]

    Position(prefix ++ suffix.tail)
  }

  /**
   * Converts the position to a JSON string.
   *
   * @param codecs The codecs used to encode the coordinates.
   * @param pretty Indicator if the resulting JSON string to be indented.
   *
   * @return The JSON string representation.
   */
  def toJSON[
    L <: HList,
    Z <: HList,
    M <: HList
  ](
    codecs: L,
    pretty: Boolean = false
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, String]
  ): String = {
    val json = JsArray(codecs.zip(coordinates).map(EncodeString).toList.map(s => JsString(s)))

    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }

  /** Return this position as an option. */
  def toOption(): Option[this.type] = Option(this)

  /**
   * Converts the position to a consise (terse) string.
   *
   * @param codecs    The codecs used to encode the coordinates.
   * @param separator The separator to use between the coordinates.
   *
   * @return Short string representation.
   */
  def toShortString[
    L <: HList,
    Z <: HList,
    M <: HList
  ](
    codecs: L,
    separator: String
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, Any]
  ): String = codecs.zip(coordinates).map(EncodeString).mkString("", separator, "")

  /**
   * Update the coordinate at `dimimension` with `value`.
   *
   * @param dimension The dimension to set.
   * @param value     The coordinate to set.
   *
   * @return A position of the same size as `this` but with `value` set at index `dim`.
   */
  def update[
    D <: Nat,
    V,
    Old,
    Out <: HList
  ](
    dimension: D,
    value: V
  )(implicit
    ev: ReplaceAt.Aux[P, D, V, (Old, Out)]
  ): Position[Out] = Position(coordinates.updatedAt[D](value))

  /**
   * Compare this object with another position.
   *
   * @param codecs The codecs used to compare coordinates.
   * @param that   Position to compare against.
   *
   * @return x < 0 iff this < that, x = 0 iff this = that, x > 0 iff this > that.
   *
   * @note If the comparison is between two positions with different dimensions, then a comparison on the number of
   *       dimensions is performed.
   */
  def compare[
    L <: HList,
    Z1 <: HList,
    Z2 <: HList,
    M <: HList
  ](
    codecs: L,
    that: Position[P]
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[P :: P :: HNil, Z1],
    ev3: Zip.Aux[Z1 :: L :: HNil, Z2],
    ev4: Mapper.Aux[CompareTuple.type, Z2, M],
    ev5: ToTraversable.Aux[M, List, Int]
  ): Int = coordinates
    .zip(that.coordinates)
    .zip(codecs)
    .map(CompareTuple)
    .toList
    .collectFirst { case cmp if (cmp != 0) => cmp }
    .getOrElse(0)
}

/** Companion object with constructor, implicits, etc. */
object Position {
  /** Constructor for 0 dimensional position. */
  def apply(): Position[HNil] = Position(HNil)

  /** Constructor for 1 dimensional position. */
  def apply[V1](first: V1)(implicit ev: V1 =:!= HList): Position[V1 :: HNil] = Position(first :: HNil)

  /** Constructor for 2 dimensional position. */
  def apply[V1, V2](first: V1, second: V2): Position[V1 :: V2 :: HNil] = Position(first :: second :: HNil)

  /** Constructor for 3 dimensional position. */
  def apply[
    V1,
    V2,
    V3
  ](
    first: V1,
    second: V2,
    third: V3
  ): Position[V1 :: V2 :: V3 :: HNil] = Position(first :: second :: third :: HNil)

  /** Constructor for 4 dimensional position. */
  def apply[
    V1,
    V2,
    V3,
    V4
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4
  ): Position[V1 :: V2 :: V3 :: V4 :: HNil] = Position(first :: second :: third :: fourth :: HNil)

  /** Constructor for 5 dimensional position. */
  def apply[
    V1,
    V2,
    V3,
    V4,
    V5
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = Position(first :: second :: third :: fourth :: fifth :: HNil)

  /** Constructor for 6 dimensional position. */
  def apply[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: HNil
  )

  /** Constructor for 7 dimensional position. */
  def apply[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6,
    seventh: V7
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: seventh :: HNil
  )

  /** Constructor for 8 dimensional position. */
  def apply[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6,
    seventh: V7,
    eighth: V8
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: seventh :: eighth :: HNil
  )

  /** Constructor for 9 dimensional position. */
  def apply[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8,
    V9
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6,
    seventh: V7,
    eighth: V8,
    nineth: V9
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: seventh :: eighth :: nineth :: HNil
  )

  /**
   * Define an ordering between 2 positions.
   *
   * @param codecs    The codecs used to compare coordinates.
   * @param ascending Indicator if comparison results should be inverted.
   *
   * @return An ordering of `Position[P]`.
   */
  def ordering[ // TODO: Is this still needed?
    L <: HList,
    P <: HList,
    Z1 <: HList,
    Z2 <: HList,
    M <: HList
  ](
    codecs: L,
    ascending: Boolean = true
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[P :: P :: HNil, Z1],
    ev3: Zip.Aux[Z1 :: L :: HNil, Z2],
    ev4: Mapper.Aux[CompareTuple.type, Z2, M],
    ev5: ToTraversable.Aux[M, List, Int]
  ): Ordering[Position[P]] = new Ordering[Position[P]] {
    def compare(x: Position[P], y: Position[P]): Int = x.compare(codecs, y) * (if (ascending) 1 else -1)
  }

  /**
   * Return function that returns a string representation of a position.
   *
   * @param codecs    The codecs used to encode the coordinates.
   * @param verbose   Indicator if verbose string is required or not.
   * @param separator The separator to use between various fields (only used if verbose is `false`).
   */
  def toString[ // TODO: Is this still practical?
    P <: HList,
    L <: HList,
    Z <: HList,
    M <: HList
  ](
    codecs: L,
    verbose: Boolean = false,
    separator: String = "|"
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, Any],
    ev5: ToTraversable.Aux[P, List, Any]
  ): (Position[P]) => TraversableOnce[String] = (p: Position[P]) =>
    List(if (verbose) p.toString else p.toShortString(codecs, separator))

  /**
   * Return function that returns a JSON representation of a position.
   *
   * @param codecs The codecs used to encode the coordinates.
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON[ // TODO: Is this still practical?
    P <: HList,
    L <: HList,
    Z <: HList,
    M <: HList
  ](
    codecs: L,
    pretty: Boolean = false
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, String]
  ): (Position[P]) => TraversableOnce[String] = (p: Position[P]) => List(p.toJSON(codecs, pretty))

  /**
   * Return a `Reads` for parsing a JSON position.
   *
   * @param codecs The codecs used to parse the JSON position data.
   */
  def reads[ // TODO: Merge into a `fromJSON`?
    L <: HList,
    Tup <: HList,
    ZWI <: HList,
    Ind <: HList,
    Z <: HList,
    Dec <: HList,
    Out <: HList
  ](
    codecs: L
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: ToTraversable.Aux[L, List, Codec[_]],
    ev3: ConstMapper.Aux[List[String], L, Tup],
    ev4: ZipWithIndex.Aux[Tup, ZWI],
    ev5: Mapper.Aux[GetAtIndex.type, ZWI, Ind],
    ev6: Zip.Aux[L :: Ind :: HNil, Z],
    ev7: Mapper.Aux[DecodeString.type, Z, Dec],
    ev8: ToTraversable.Aux[Dec, List, Option[_]],
    ev9: FlatMapper.Aux[RemoveOption.type, Dec, Out]
  ): Reads[Position[Out]] = new Reads[Position[Out]] {
    def reads(json: JsValue): JsResult[Position[Out]] = {
      val fields = json.as[JsArray].value.toList.map(_.as[String])

      if (codecs.runtimeLength == fields.length) {
        val parsed = codecs
          .zip(codecs.mapConst(fields).zipWithIndex.map(GetAtIndex))
          .map(DecodeString)

        if (!parsed.toList.exists(_.isEmpty))
          JsSuccess(Position(parsed.flatMap(RemoveOption)))
        else
          JsError("Unable to parse coordinates")
      } else
        JsError("Incorrect number of coordinates")
    }
  }

  /** Converts a `V` to a `Position[V :: HNil]` */
  implicit def valueToPosition[V](v: V): Position[V :: HNil] = Position(v)

  /** Converts a `V` to a `List[Position[V :: HNil */
  implicit def valueToListPosition[V](v: V): List[Position[V :: HNil]] = List(Position(v))

  /** Converts a `Position[P]` to a `List[Position[P]]` */
  implicit def positionToListPosition[P <: HList](p: Position[P]): List[Position[P]] = List(p)
}

object GetAtIndex extends Poly1 {
  implicit def go[T <: Nat : ToInt] = at[(List[String], T)] { case (list, nat) => list(Nat.toInt[T]) }
}

object RemoveOption extends Poly1 {
  implicit def go[T] = at[Option[T]](_.get :: HNil)
}

/** Trait that represents the positions of a matrix. */
trait Positions[P <: HList, C <: Context[C]] extends Persist[Position[P], C] {
  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Position[slice.S]]` of the distinct position(s).
   */
  def names[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: Positions.NamesTuner[C#U, T]
  ): C#U[Position[slice.S]]

  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `Position[N]` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[Position[P]]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[Position[P]], // = Position.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[Position[P]]

  /**
   * Slice the positions using a regular expression applied to a dimension.
   *
   * @param keep  Indicator if the matched positions should be kept or removed.
   * @param dim   Dimension to slice on.
   * @param regex The regular expression to match on.
   *
   * @return A `C#U[Position[P]]` with only the positions of interest.
   *
   * @note The matching is done by converting the coordinate to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice[
    D <: Nat,
    V
  ](
    keep: Boolean,
    dim: D,
    regex: Regex
  )(implicit
    ev: At.Aux[P, D, V]
  ): C#U[Position[P]] = slice(keep, p => regex.pattern.matcher(p(dim).toString).matches) // toShortString?

  /**
   * Slice the positions using a regular expression.
   *
   * @param keep  Indicator if the matched positions should be kept or removed.
   * @param regex The regular expression to match on.
   *
   * @return A `C#U[Position[P]]` with only the positions of interest.
   *
   * @note The matching is done by converting each coordinate to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice(keep: Boolean, regex: Regex): C#U[Position[P]] = slice(
    keep,
    p => p.coordinates.map(c => regex.pattern.matcher(c.toShortString).matches).reduce(_ && _)
  )

  /**
   * Slice the positions using one or more positions.
   *
   * @param keep      Indicator if the matched positions should be kept or removed.
   * @param positions The positions to slice on.
   *
   * @return A `C#U[Position[P]]` with only the positions of interest.
   */
  def slice(
    keep: Boolean,
    positions: List[Position[P]]
  ): C#U[Position[P]] = slice(keep, p => positions.contains(p))

  protected def slice(keep: Boolean, f: Position[P] => Boolean): C#U[Position[P]]
}

/** Companion object to `Positions` with types, implicits, etc. */
object Positions {
  /** Trait for tuners permitted on a call to `names`. */
  trait NamesTuner[U[_], T <: Tuner] extends java.io.Serializable
}

