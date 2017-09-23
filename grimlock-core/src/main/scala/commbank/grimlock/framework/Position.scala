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
import commbank.grimlock.framework.encoding.{
  BooleanCodec,
  Codec,
  DateCodec,
  DoubleCodec,
  LongCodec,
  StringCodec,
  TypeCodec,
  Value
}
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.utility.JSON

import java.util.regex.Pattern

import play.api.libs.json.{ JsArray, JsError, JsResult, JsString, JsSuccess, JsValue, Reads, Writes }

import scala.util.matching.Regex

import shapeless.{
  ::,
  =:!=,
  HList,
  HNil,
  IsDistinctConstraint,
  LUBConstraint,
  Poly1,
  Nat
}
import shapeless.ops.hlist.{
  At,
  ConstMapper,
  FlatMapper,
  IsHCons,
  Length,
  Mapper,
  Prepend,
  ReplaceAt,
  Split,
  ToTraversable,
  Zip,
  ZipWithIndex
}
import shapeless.ops.nat.{ GT, GTEq, ToInt }


/**
 * Case class for dealing with positions.
 *
 * @param coordinates List of coordinates of the position.
 */
case class Position[P <: HList](coordinates: P)(implicit ev: Position.ValueConstraints[P]) {
  /**
   * Append a coordinate to the position.
   *
   * @param value The coordinate to append.
   *
   * @return A new position with the coordinate `value` appended.
   */
  def append[
    T <% V,
    V <: Value[_],
    Q <: HList
  ](
    value: T
  )(implicit
    ev: Position.AppendConstraints[P, V, Q]
  ): Position[Q] = {
    import ev._

    Position(coordinates :+ implicitly[V](value))
  }

  /**
   * Return the coordinate at dimension (index) `dim`.
   *
   * @param dimension Dimension of the coordinate to get.
   */
  def apply[D <: Nat, V <: Value[_]](dimension: D)(implicit ev: Position.IndexConstraints[P, D, V]): V = {
    import ev._

    coordinates.at[D]
  }

  /** Converts this position to a list of `Value`. */
  def asList(implicit ev: Position.ListConstraints[P]): List[Value[_]] = {
    import ev._

    coordinates.toList
  }

  /**
   * Compare this object with another position.
   *
   * @param that Position to compare against.
   *
   * @return x < 0 iff this < that, x = 0 iff this = that, x > 0 iff this > that.
   */
  def compare(that: Position[P])(implicit ev: Position.ListConstraints[P]): Int = asList
    .zip(that.asList)
    .map { case (x, y) => x.compare(y).getOrElse(throw new Exception("Different types should not be possible")) }
    .collectFirst { case cmp if (cmp != 0) => cmp }
    .getOrElse(0)

  /**
   * Insert a coordinate into the position.
   *
   * @param dimension The dimension to insert at.
   * @param value     The coordinate to insert.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def insert[
    D <: Nat,
    T <% V,
    V <: Value[_],
    Q <: HList
  ](
    dimension: D,
    value: T
  )(implicit
    ev: Position.InsertConstraints[P, D, V, Q]
  ): Position[Q] = {
    import ev._

    val (prefix, suffix) = coordinates.split[D]

    Position(prefix ::: implicitly[V](value) :: suffix)
  }

  /**
   * Melt dimension `dim` into `into`.
   *
   * @param dimension The dimension to remove.
   * @param into      The dimension into which to melt.
   * @param merge     The function to use for merging coordinates
   *
   * @return A new position with `dimension` removed. The coordinate at `into` will be the output of `merge`.
   */
  def melt[
    D <: Nat,
    I <: Nat,
    T <% V,
    V <: Value[_],
    X <: Value[_],
    Y <: Value[_],
    Q <: HList
  ](
    dim: D,
    into: I,
    merge: (X, Y) => T
  )(implicit
    ev: Position.MeltConstraints[P, D, I, V, X, Y, Q]
  ): Position[Q] = {
    import ev._

    val value = implicitly[V](merge(coordinates.at[I], coordinates.at[D]))
    val (prefix, suffix) = coordinates.updatedAt[I](value).split[D]

    Position(prefix ++ suffix.tail)
  }

  /**
   * Prepend a coordinate to the position.
   *
   * @param value The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def prepend[
    T <% V,
    V <: Value[_]
  ](
    value: T
  )(implicit
    ev: Position.ValueConstraints[V :: P]
  ): Position[V :: P] = Position(implicitly[V](value) :: coordinates)

  /**
   * Remove the coordinate at dimension `dimension`.
   *
   * @param dimension The dimension to remove.
   *
   * @return A new position with `dimension` removed.
   */
  def remove[D <: Nat, Q <: HList](dimension: D)(implicit ev: Position.RemoveConstraints[P, D, Q]): Position[Q] = {
    import ev._

    val (prefix, suffix) = coordinates.split[D]

    Position(prefix ++ suffix.tail)
  }

  /**
   * Converts the position to a JSON string.
   *
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON(
    pretty: Boolean = false
  )(implicit
    ev: Position.ListConstraints[P]
  ): String = JSON.to(this, Position.writes, pretty)

  /** Return this position as an option. */
  def toOption: Option[Position[P]] = Option(this)

  /**
   * Converts the position to a consise (terse) string.
   *
   * @param separator The separator to use between the coordinates.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String)(implicit ev: Position.ListConstraints[P]): String = asList
    .map(_.toShortString)
    .mkString(separator)

  /**
   * Update the coordinate at `dim` with `value`.
   *
   * @param dim   The dimension to set.
   * @param value The coordinate to set.
   *
   * @return A position of the same size as `this` but with `value` set at index `dim`.
   */
  def update[
    D <: Nat,
    T <% V,
    V <: Value[_],
    Q <: HList
  ](
    dimension: D,
    value: T
  )(implicit
    ev: Position.UpdateConstraints[P, D, V, Q]
  ): Position[Q] = {
    import ev._

    Position(coordinates.updatedAt[D](implicitly[V](value)))
  }
}

/** Companion object with constructor, implicits, etc. */
object Position {
  /** Type that captures all constraints for appending coordinates to a position. */
  trait AppendConstraints[P <: HList, V <: Value[_], Q <: HList] extends java.io.Serializable {
    implicit val prependV: Prepend.Aux[P, V :: HNil, Q]
    implicit val allValue: LUBConstraint[Q, Value[_]]
  }

  /** Type that captures that `Q` should have the same number of coordinates as `P`. */
  trait EqualConstraints[Q <: HList, P <: HList] extends java.io.Serializable {
  }

  /** Type that captures that `Q` should have the same or more coordinates than `P`. */
  trait GreaterEqualConstraints[Q <: HList, P <: HList] extends java.io.Serializable {
  }

  /** Type that captures that `Q` should have more coordinates than `P`. */
  trait GreaterThanConstraints[Q <: HList, P <: HList] extends java.io.Serializable {
  }

  /** Type that captures all constraints for indexing a position. */
  trait IndexConstraints[P <: HList, D <: Nat, V <: Value[_]] extends java.io.Serializable {
    implicit val atIndex: At.Aux[P, D, V]
  }

  /** Type that captures all constraints for inserting coordinates into a position. */
  trait InsertConstraints[P <: HList, D <: Nat, V <: Value[_], Q <: HList] extends java.io.Serializable {
    type PRE <: HList
    type SUF <: HList

    implicit val splitP: Split.Aux[P, D, PRE, SUF]
    implicit val createQ: Prepend.Aux[PRE, V :: SUF, Q]
    implicit val allValue: LUBConstraint[Q, Value[_]]
  }

  /** Type that captures all constraints for converting coordinates into a list. */
  trait ListConstraints[P <: HList] extends java.io.Serializable {
    implicit val asValueList: ToTraversable.Aux[P, List, Value[_]]
  }

  /** Type that captures all constraints for melting coordinates of a position. */
  trait MeltConstraints[
    P <: HList,
    D <: Nat,
    I <: Nat,
    V <: Value[_],
    X <: Value[_],
    Y <: Value[_],
    Q <: HList
  ] extends java.io.Serializable {
    type REP <: HList
    type PRE <: HList
    type SUF <: HList
    type TAI <: HList

    implicit val atI: At.Aux[P, I, X]
    implicit val atD: At.Aux[P, D, Y]
    implicit val replaceI: ReplaceAt.Aux[P, I, V, (X, REP)]
    implicit val splitP: Split.Aux[REP, D, PRE, SUF]
    implicit val hasTail: IsHCons.Aux[SUF, _, TAI]
    implicit val createQ: Prepend.Aux[PRE, TAI, Q]
    implicit val isDifferent: IsDistinctConstraint[D :: I :: HNil] // shapeless.=:!= doesn't serialise
    implicit val allValue: LUBConstraint[Q, Value[_]]
  }

  /** Type that captures that the position needs at least one coordinate. */
  trait NonEmptyConstraints[P <: HList] extends java.io.Serializable {
  }

  /** Type that captures all constraints for removing coordinates from a position. */
  trait RemoveConstraints[P <: HList, D <: Nat, Q <: HList] extends java.io.Serializable {
    type PRE <: HList
    type SUF <: HList
    type TAI <: HList

    implicit val splitP: Split.Aux[P, D, PRE, SUF]
    implicit val hasTail: IsHCons.Aux[SUF, _, TAI]
    implicit val createQ: Prepend.Aux[PRE, TAI, Q]
    implicit val allValue: LUBConstraint[Q, Value[_]]
  }

  /** Type that captures all constraints for parsing positions from string. */
  trait TextParseConstraints[L <: HList, Q <: HList] extends java.io.Serializable {
    type TUP <: HList
    type ZWI <: HList
    type IND <: HList
    type ZIP <: HList
    type DEC <: HList

    implicit val allCodec: LUBConstraint[L, Codec[_]]
    implicit val mapWithListString: ConstMapper.Aux[List[String], L, TUP]
    implicit val zipListStringWithIndex: ZipWithIndex.Aux[TUP, ZWI]
    implicit val getStringAtIndex: Mapper.Aux[GetAtIndex.type, ZWI, IND]
    implicit val zipWithString: Zip.Aux[L :: IND :: HNil, ZIP]
    implicit val decodeString: Mapper.Aux[DecodeString.type, ZIP, DEC]
    implicit val toOptionList: ToTraversable.Aux[DEC, List, Option[_]]
    implicit val removeOption: FlatMapper.Aux[RemoveOption.type, DEC, Q]
    implicit val allValue: LUBConstraint[Q, Value[_]]
  }

  /** Type that captures all constraints for updating coordinates in a position. */
  trait UpdateConstraints[P <: HList, D <: Nat, V <: Value[_], Q <: HList] extends java.io.Serializable {
    type UPD <: HList

    implicit val replaceAs: ReplaceAt.Aux[P, D, V, (UPD, Q)]
    implicit val allValue: LUBConstraint[Q, Value[_]]
  }

  /** Type that captures all constraints for the type of the coordinates in a position. */
  trait ValueConstraints[P <: HList] extends java.io.Serializable {
    implicit val allValue: LUBConstraint[P, Value[_]]
  }

  /** Implicit with all constraints for appending coordinates to a position. */
  implicit def appendConstraints[
    P <: HList,
    V <: Value[_],
    Q <: HList
  ](implicit
    ev1: Prepend.Aux[P, V :: HNil, Q],
    ev2: LUBConstraint[Q, Value[_]]
  ): AppendConstraints[P, V, Q] = new AppendConstraints[P, V, Q] {
    implicit val prependV = ev1
    implicit val allValue = ev2
  }

  /** Implicit that ensures that `Q` has at same number of coordinates as `P`. */
  implicit def equalConstraints[
    P <: HList,
    Q <: HList,
    L <: Nat,
    M <: Nat
  ](implicit
    ev1: Length.Aux[P, L],
    ev2: Length.Aux[Q, M],
    ev3: L =:= M
  ): EqualConstraints[Q, P] = new EqualConstraints[Q, P] { }

  /** Implicit that ensures that `Q` has at least as many coordinates as `P`. */
  implicit def greaterEqualConstraints[
    P <: HList,
    Q <: HList,
    L <: Nat,
    M <: Nat
  ](implicit
    ev1: Length.Aux[P, L],
    ev2: Length.Aux[Q, M],
    ev3: GTEq[M, L]
  ): GreaterEqualConstraints[Q, P] = new GreaterEqualConstraints[Q, P] { }

  /** Implicit that ensures that `Q` has at more coordinates than `P`. */
  implicit def greaterThanConstraints[
    P <: HList,
    Q <: HList,
    L <: Nat,
    M <: Nat
  ](implicit
    ev1: Length.Aux[P, L],
    ev2: Length.Aux[Q, M],
    ev3: GT[M, L]
  ): GreaterThanConstraints[Q, P] = new GreaterThanConstraints[Q, P] { }

  /** Implicit with all constraints for indexing coordinates in a position. */
  implicit def indexConstraints[
    P <: HList,
    D <: Nat,
    V <: Value[_]
  ](implicit
    ev: At.Aux[P, D, V]
  ): IndexConstraints[P, D, V] = new IndexConstraints[P, D, V] {
    implicit val atIndex = ev
  }

  /** Implicit with all constraints for inserting coordinates in a position. */
  implicit def insertConstraints[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    Q <: HList,
    F <: HList,
    L <: HList
  ](implicit
    ev1: Split.Aux[P, D, F, L],
    ev2: Prepend.Aux[F, V :: L, Q],
    ev3: LUBConstraint[Q, Value[_]]
  ): InsertConstraints[P, D, V, Q] = new InsertConstraints[P, D, V, Q] {
    type PRE = F
    type SUF = L

    implicit val splitP = ev1
    implicit val createQ = ev2
    implicit val allValue = ev3
  }

  /** Implicit with all constraints for converting the coordinates of a position to a list. */
  implicit def listConstraints[
    P <: HList
  ](implicit
    ev: ToTraversable.Aux[P, List, Value[_]]
  ): ListConstraints[P] = new ListConstraints[P] {
    implicit val asValueList = ev
  }

  /** Converts a `Value` to a `List[Position[V :: HNil]]` */
  implicit def listValueToListPosition[T <% V, V <: Value[_]](l: List[T]): List[Position[V :: HNil]] = l
    .map(t => Position(implicitly[V](t)))

  /** Implicit with all constraints for melting coordinates of a position. */
  implicit def meltConstraints[
    P <: HList,
    D <: Nat,
    I <: Nat,
    T <% V,
    V <: Value[_],
    X <: Value[_],
    Y <: Value[_],
    Q <: HList,
    U <: HList,
    F <: HList,
    L <: HList,
    A <: HList
  ](implicit
    ev1: At.Aux[P, I, X],
    ev2: At.Aux[P, D, Y],
    ev3: ReplaceAt.Aux[P, I, V, (X, U)],
    ev4: Split.Aux[U, D, F, L],
    ev5: IsHCons.Aux[L, _, A],
    ev6: Prepend.Aux[F, A, Q],
    ev7: IsDistinctConstraint[D :: I :: HNil],
    ev8: LUBConstraint[Q, Value[_]]
  ): MeltConstraints[P, D, I, V, X, Y, Q] =  new MeltConstraints[P, D, I, V, X, Y, Q] {
    type REP = U
    type PRE = F
    type SUF = L
    type TAI = A

    implicit val atI = ev1
    implicit val atD = ev2
    implicit val replaceI = ev3
    implicit val splitP = ev4
    implicit val hasTail = ev5
    implicit val createQ = ev6
    implicit val isDifferent = ev7
    implicit val allValue = ev8
  }

  /** Implicit for ensuring the position has coordinates. */
  implicit def nonEmptyConstrains[
    P <: HList
  ](implicit
    ev: P =:!= HNil
  ): NonEmptyConstraints[P] = new NonEmptyConstraints[P] { }

  /** Converts a `Position[P]` to a `List[Position[P]]` */
  implicit def positionToListPosition[P <: HList](p: Position[P]): List[Position[P]] = List(p)

  /** Implicit with all constraints for removing coordinates of a position. */
  implicit def removeConstraints[
    P <: HList,
    D <: Nat,
    Q <: HList,
    F <: HList,
    L <: HList,
    T <: HList
  ](implicit
    ev1: Split.Aux[P, D, F, L],
    ev2: IsHCons.Aux[L, _, T],
    ev3: Prepend.Aux[F, T, Q],
    ev4: LUBConstraint[Q, Value[_]]
  ): RemoveConstraints[P, D, Q] = new RemoveConstraints[P, D, Q] {
    type PRE = F
    type SUF = L
    type TAI = T

    implicit val splitP = ev1
    implicit val hasTail = ev2
    implicit val createQ = ev3
    implicit val allValue = ev4
  }

  /** Implicit with all constraints for parsing a position from string. */
  implicit def textParseConstraints[
    L <: HList,
    Q <: HList,
    T <: HList,
    W <: HList,
    I <: HList,
    Z <: HList,
    D <: HList
  ](implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: ConstMapper.Aux[List[String], L, T],
    ev3: ZipWithIndex.Aux[T, W],
    ev4: Mapper.Aux[GetAtIndex.type, W, I],
    ev5: Zip.Aux[L :: I :: HNil, Z],
    ev6: Mapper.Aux[DecodeString.type, Z, D],
    ev7: ToTraversable.Aux[D, List, Option[_]],
    ev8: FlatMapper.Aux[RemoveOption.type, D, Q],
    ev9: LUBConstraint[Q, Value[_]]
  ): TextParseConstraints[L, Q] = new TextParseConstraints[L, Q] {
    type TUP = T
    type ZWI = W
    type IND = I
    type ZIP = Z
    type DEC = D

    implicit val allCodec = ev1
    implicit val mapWithListString = ev2
    implicit val zipListStringWithIndex = ev3
    implicit val getStringAtIndex = ev4
    implicit val zipWithString = ev5
    implicit val decodeString = ev6
    implicit val toOptionList = ev7
    implicit val removeOption = ev8
    implicit val allValue = ev9
  }

  /** Implicit with all constraints for updating coordinates of a position. */
  implicit def updateConstraints[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    Q <: HList,
    U <: HList
  ](implicit
    ev1: ReplaceAt.Aux[P, D, V, (U, Q)],
    ev2: LUBConstraint[Q, Value[_]]
  ): UpdateConstraints[P, D, V, Q] = new UpdateConstraints[P, D, V, Q] {
    type UPD = U

    implicit val replaceAs = ev1
    implicit val allValue = ev2
  }

  /** Implicit with all constraints for the type of the coordinates of a position. */
  implicit def valueConstraints[
    P <: HList
  ](implicit
    ev: LUBConstraint[P, Value[_]]
  ): ValueConstraints[P] = new ValueConstraints[P] {
    implicit val allValue = ev
  }

  /** Converts a `Value` to a `List[Position[V :: HNil]]` */
  implicit def valueToListPosition[
    T <% V,
    V <: Value[_]
  ](
    t: T
  ): List[Position[V :: HNil]] = List(Position(implicitly[V](t)))

  /** Converts a `Value` to a `Position[V :: HNil]` */
  implicit def valueToPosition[T <% V, V <: Value[_]](t: T): Position[V :: HNil] = Position(implicitly[V](t))

  /** Constructor for 0 dimensional position. */
  def apply(): Position[HNil] = Position(HNil)

  /** Constructor for 1 dimensional position. */
  def apply[ 
    T1 <% V1,
    V1 <: Value[_]
  ](
    first: T1
  ): Position[V1 :: HNil] = Position(implicitly[V1](first) :: HNil)

  /** Constructor for 2 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    first: T1,
    second: T2
  ): Position[V1 :: V2 :: HNil] = Position(implicitly[V1](first) :: implicitly[V2](second) :: HNil)

  /** Constructor for 3 dimensional position. */
  def apply[ 
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3
  ): Position[V1 :: V2 :: V3 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    HNil
  )

  /** Constructor for 4 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4
  ): Position[V1 :: V2 :: V3 :: V4 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    implicitly[V4](fourth) ::
    HNil
  )

  /** Constructor for 5 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    implicitly[V4](fourth) ::
    implicitly[V5](fifth) ::
    HNil
  )

  /** Constructor for 6 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    implicitly[V4](fourth) ::
    implicitly[V5](fifth) ::
    implicitly[V6](sixth) ::
    HNil
  )

  /** Constructor for 7 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    T7 <% V7,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6,
    seventh: T7
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    implicitly[V4](fourth) ::
    implicitly[V5](fifth) ::
    implicitly[V6](sixth) ::
    implicitly[V7](seventh) ::
    HNil
  )

  /** Constructor for 8 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    T7 <% V7,
    T8 <% V8,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6,
    seventh: T7,
    eighth: T8
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    implicitly[V4](fourth) ::
    implicitly[V5](fifth) ::
    implicitly[V6](sixth) ::
    implicitly[V7](seventh) ::
    implicitly[V8](eighth) ::
    HNil
  )

  /** Constructor for 9 dimensional position. */
  def apply[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    T7 <% V7,
    T8 <% V8,
    T9 <% V9,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_],
    V9 <: Value[_]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6,
    seventh: T7,
    eighth: T8,
    nineth: T9
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = Position(
    implicitly[V1](first) ::
    implicitly[V2](second) ::
    implicitly[V3](third) ::
    implicitly[V4](fourth) ::
    implicitly[V5](fifth) ::
    implicitly[V6](sixth) ::
    implicitly[V7](seventh) ::
    implicitly[V8](eighth) ::
    implicitly[V9](nineth) ::
    HNil
  )

  /**
   * Parse a position from components.
   *
   * @param coordinates The coordinate strings to parse.
   * @param codecs      The codecs to parse with.
   *
   * @return A `Some[Position[Q]]` if successful, `None` otherwise.
   */
  def fromComponents[
    L <: HList,
    Q <: HList
  ](
    coordinates: List[String],
    codecs: L
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Option[Position[Q]] = parse(coordinates, codecs).right.toOption

  /**
   * Parse a position from a JSON string.
   *
   * @param str    The JSON string to parse.
   * @param codecs The codecs to parse with.
   *
   * @return A `Some[Position[Q]]` if successful, `None` otherwise.
   */
  def fromJSON[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Option[Position[Q]] = JSON.from(str, reads(codecs)).right.toOption

  /**
   * Parse a position from a short string.
   *
   * @param str       The short string to parse.
   * @param codecs    The codecs to parse with.
   * @param separator The separator between coordinate fields.
   *
   * @return A `Some[Position[Q]]` if successful, `None` otherwise.
   */
  def fromShortString[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L,
    separator: String
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Option[Position[Q]] = parse(str, codecs, separator).right.toOption

  /**
   * Return position parser for JSON strings.
   *
   * @param codecs The codecs to parse with.
   *
   * @return A position parser.
   */
  def jsonParser[
    L <: HList,
    Q <: HList
  ](
    codecs: L
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Persist.TextParser[Position[Q]] = (str) => List(JSON.from(str, reads(codecs)))

  /**
   * Define an ordering between 2 positions.
   *
   * @param ascending Indicator if ordering should be ascending or descending.
   */
  def ordering[
    P <: HList
  ](
    ascending: Boolean = true
  )(implicit
    ev: ListConstraints[P]
  ): Ordering[Position[P]] = new Ordering[Position[P]] {
    def compare(x: Position[P], y: Position[P]): Int = x.compare(y) * (if (ascending) 1 else -1)
  }

  /**
   * Return a `Reads` for parsing a JSON position.
   *
   * @param codecs The codecs used to parse the JSON position data.
   */
  def reads[
    L <: HList,
    Q <: HList
  ](
    codecs: L
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Reads[Position[Q]] = new Reads[Position[Q]] {
    def reads(json: JsValue): JsResult[Position[Q]] = parse(json.as[JsArray].value.toList.map(_.as[String]), codecs)
      .fold(JsError(_), JsSuccess(_))
  }

  /**
   * Return position parser for short strings.
   *
   * @param codecs    The codecs to parse with.
   * @param separator The separator between coordinate fields.
   *
   * @return A position parser.
   */
  def shortStringParser[
    L <: HList,
    Q <: HList
  ](
    codecs: L,
    separator: String
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Persist.TextParser[Position[Q]] = (str) => List(parse(str, codecs, separator))

  /**
   * Return function that returns a JSON representation of a position.
   *
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON[
    P <: HList
  ](
    pretty: Boolean = false
  )(implicit
    ev: ListConstraints[P]
  ): Persist.TextWriter[Position[P]] = (pos) => List(pos.toJSON(pretty))

  /**
   * Return function that returns a string representation of a position.
   *
   * @param separator The separator to use between the coordinates.
   */
  def toShortString[
    P <: HList
  ](
    separator: String
  )(implicit
    ev: ListConstraints[P]
  ): Persist.TextWriter[Position[P]] = (pos) => List(pos.toShortString(separator))

  /** `Writes` for converting a position to JSON. */
  def writes[
    P <: HList
  ](implicit
    ev: ListConstraints[P]
  ): Writes[Position[P]] = new Writes[Position[P]] {
    def writes(pos: Position[P]): JsValue = JsArray(pos.asList.map(v => JsString(v.toShortString)))
  }

  private def parse[
    L <: HList,
    Q <: HList
  ](
    coordinates: List[String],
    codecs: L
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Either[String, Position[Q]] = {
    import ev._

    if (codecs.runtimeLength == coordinates.length) {
      val parsed = codecs
        .zip(codecs.mapConst(coordinates).zipWithIndex.map(GetAtIndex))
        .map(DecodeString)

      if (!parsed.toList.exists(_.isEmpty))
        Right(Position(parsed.flatMap(RemoveOption)))
      else
        Left("Unable to parse coordinates")
    } else
      Left("Incorrect number of coordinates")
  }

  private def parse[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L,
    separator: String
  )(implicit
    ev: TextParseConstraints[L, Q]
  ): Either[String, Position[Q]] = parse(str.split(Pattern.quote(separator)).toList, codecs)
}

/** Object with implicits needed to parse coordinates from string. */
object DecodeString extends Poly1 {
  /** Parse coordinate from string using a Codec. */
  implicit def go[T] = at[(Codec[T], String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse boolean coordinate from string without having to cast it to a Codec. */
  implicit def goBoolean = at[(BooleanCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse date coordinate from string without having to cast it to a Codec. */
  implicit def goDate = at[(DateCodec, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse double coordinate from string without having to cast it to a Codec. */
  implicit def goDouble = at[(DoubleCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse long coordinate from string without having to cast it to a Codec. */
  implicit def goLong = at[(LongCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse string coordinate from string without having to cast it to a Codec. */
  implicit def goString = at[(StringCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse type coordinate from string without having to cast it to a Codec. */
  implicit def goType = at[(TypeCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }
}

/** Object with implicits needed to match strings and coordinates; used form parsing coordinates from string. */
object GetAtIndex extends Poly1 {
  /** Return the string (from a list), at the codec's index. */
  implicit def go[D <: Nat : ToInt] = at[(List[String], D)] { case (list, nat) => list(Nat.toInt[D]) }
}

/** Object with implicits needed to flatten options; used form parsing coordinates from string. */
object RemoveOption extends Poly1 {
  /** Remove option; use only if there are no None in the HList. */
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
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Positions.NamesTuner[C#U, T],
    ev3: Position.ListConstraints[S]
  ): C#U[Position[S]]

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
    writer: Persist.TextWriter[Position[P]],
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
    V <: Value[_]
  ](
    keep: Boolean,
    dim: D,
    regex: Regex
  )(implicit
    ev: Position.IndexConstraints[P, D, V]
  ): C#U[Position[P]] = slice(keep, p => regex.pattern.matcher(p(dim).toShortString).matches)

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
  def slice(
    keep: Boolean,
    regex: Regex
  )(implicit
    ev: Position.ListConstraints[P]
  ): C#U[Position[P]] = slice(keep, _.asList.map(c => regex.pattern.matcher(c.toShortString).matches).reduce(_ && _))

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

