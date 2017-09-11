// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.framework

import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ Codec, EncodeString }
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.{ ::, HList, HNil, LUBConstraint, Nat }
import shapeless.ops.hlist.{ At, Mapper, Prepend, ReplaceAt, ToTraversable, Zip }

// TODO: Remove calls of Object.toString

object Locate {
  /** Extract position. */
  type FromPosition[P <: HList, Q <: HList] = (Position[P]) => Option[Position[Q]]

  /** Extract position for left and right positions. */
  type FromPairwisePositions[P <: HList, Q <: HList] = (Position[P], Position[P]) => Option[Position[Q]]

  /** Extract position from cell. */
  type FromCell[P <: HList, Q <: HList] = (Cell[P]) => Option[Position[Q]]

  /** Extract position from cell with user provided value. */
  type FromCellWithValue[P <: HList, Q <: HList, V] = (Cell[P], V) => Option[Position[Q]]

  /** Extract position from cell and an optional value. */
  type FromCellAndOptionalValue[P <: HList, Q <: HList, V] = (Cell[P], Option[V]) => Option[Position[Q]]

  /** Extract position for left and right cells. */
  type FromPairwiseCells[P <: HList, Q <: HList] = (Cell[P], Cell[P]) => Option[Position[Q]]

  /** Extract position for the selected cell and its remainder. */
  type FromSelectedAndRemainder[S <: HList, R <: HList, Q <: HList] = (Position[S], Position[R]) => Option[Position[Q]]

  /** Extract position for the selected cell and its current and prior remainder. */
  type FromSelectedAndPairwiseRemainder[
    S <: HList,
    R <: HList,
    Q <: HList
  ] = (Position[S], Position[R], Position[R]) => Option[Position[Q]]

  /** Extract position from selected position and a value. */
  type FromSelectedAndOutput[S <: HList, T, Q <: HList] = (Position[S], T) => Option[Position[Q]]

  /** Extract position from selected position and a content. */
  type FromSelectedAndContent[S <: HList, Q <: HList, D] = (Position[S], Content[D]) => Option[Position[Q]]

  /**
   * Rename a dimension.
   *
   * @param dim  The dimension to rename.
   * @param name The rename pattern. Use `%1$``s` for the coordinate.
   */
  def RenameDimension[
    P <: HList,
    D <: Nat,
    V,
    Old,
    Out <: HList
  ](
    dim: D,
    name: String
  )(implicit
    ev1: At.Aux[P, D, V],
    ev2: ReplaceAt.Aux[P, D, String, (Old, Out)]
  ): FromCell[P, Out] = (cell: Cell[P]) => cell
    .position
    .update(dim, name.format(cell.position(dim).toString))
    .toOption

  /**
   * Rename coordinate according a name pattern.
   *
   * @param dim  Dimension for which to update coordinate name.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def RenameDimensionWithContent[
    P <: HList,
    D <: Nat,
    V,
    Old,
    Out <: HList
  ](
    dim: D,
    name: String = "%1$s=%2$s"
  )(implicit
    ev1: At.Aux[P, D, V],
    ev2: ReplaceAt.Aux[P, D, String, (Old, Out)]
  ): FromCell[P, Out] = (cell: Cell[P]) => cell
    .position
    .update(dim, name.format(cell.position(dim).toString, cell.content.value.toString))
    .toOption

  /**
   * Append a coordinate to the position.
   *
   * @param name The coordinate to append to the outcome cell.
   */
  def AppendValue[
    P <: HList,
    V,
    Out <: HList
  ](
    name: V
  )(implicit
    ev: Prepend.Aux[P, V :: HNil, Out]
  ): FromCell[P, Out] = (cell: Cell[P]) => cell.position.append(name).toOption

  /**
   * Extract position use a name pattern.
   *
   * @param slice     Encapsulates the dimension(s) from which to construct the name.
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  left and right selected positions respectively.
   * @param all       Indicates if all positions should be returned (true), or only if left and right remainder
   *                  are equal.
   * @param separator Separator to use when converting left and right positions to string.
   *
   * @note If a position is returned then it's always right cell's remainder with an additional coordinate prepended.
   */
  def PrependPairwiseSelectedStringToRemainder[
    P <: HList,
    L <: HList,
    Z <: HList,
    M <: HList
  ](
    slice: Slice[P],
    codecs: L,
    pattern: String,
    all: Boolean = false,
    separator: String = "|"
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: slice.S :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, Any]
  ): FromPairwiseCells[P, String :: slice.R] = (left: Cell[P], right: Cell[P]) => {
    val reml = slice.remainder(left.position)
    val remr = slice.remainder(right.position)

    if (all || reml == remr)
      reml
        .prepend(
          pattern.format(
            slice.selected(left.position).toShortString(codecs, separator),
            slice.selected(right.position).toShortString(codecs, separator)
          )
        )
        .toOption
    else
      None
  }

  /**
   * Append with a dimension from the remainder.
   *
   * @param dim The dimension (out of `rem`) to append to the cell's position.
   */
  def AppendRemainderDimension[
    S <: HList,
    R <: HList,
    D <: Nat,
    V,
    Out <: HList
  ](
    dim: D
  )(implicit
    ev1: At.Aux[R, D, V],
    ev2: Prepend.Aux[S, V :: HNil, Out]
  ): FromSelectedAndRemainder[S, R, Out] = (sel: Position[S], rem: Position[R]) => sel.append(rem(dim)).toOption

  /**
   * Extract position using string of `rem`.
   *
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendRemainderString[
    S <: HList,
    R <: HList,
    L <: HList,
    Z <: HList,
    M <: HList,
    Out <: HList
  ](
    codecs: L,
    separator: String = "|"
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: R :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, Any],
    ev5: Prepend.Aux[S, String :: HNil, Out]
  ): FromSelectedAndRemainder[S, R, Out] = (sel: Position[S], rem: Position[R]) => sel
    .append(rem.toShortString(codecs, separator))
    .toOption

  /**
   * Extract position using string of current and previous `rem`.
   *
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  previous and current remainder positions respectively.
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendPairwiseString[
    S <: HList,
    R <: HList,
    Out <: HList
  ](
    pattern: String,
    separator: String = "|"
  )(implicit
    ev: Prepend.Aux[S, String :: HNil, Out]
  ): FromSelectedAndPairwiseRemainder[S, R, Out] = (sel: Position[S], curr: Position[R], prev: Position[R]) => sel
    .append(pattern.format(prev.toShortString(separator), curr.toShortString(separator)))
    .toOption

  /**
   * Expand position by appending a string coordinate from double.
   *
   * @param name The name pattern. Use `%1$``s` for the output pattern.
   */
  def AppendDoubleString[
    S <: HList,
    Out <: HList
  ](
    name: String = "%1$f%%"
  )(implicit
    ev: Prepend.Aux[S, String :: HNil, Out]
  ): FromSelectedAndOutput[S, Double, Out] = (sel: Position[S], value: Double) => sel
    .append(name.format(value))
    .toOption

  /** Append the content string to the position. */
  def AppendContentString[
    S <: HList,
    D,
    Out <: HList
  ](
  )(implicit
    ev: Prepend.Aux[S, String :: HNil, Out]
  ): FromSelectedAndContent[S, Out, D] = (sel: Position[S], con: Content[D]) => sel
    .append(con.value.toString)
    .toOption

  /**
   * Append a coordinate according a name pattern.
   *
   * @param dim  Dimension for which to update coordinate name.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def AppendDimensionAndContentString[
    S <: HList,
    D <: Nat,
    V,
    Out <: HList
  ](
    dim: D,
    name: String = "%1$s=%2$s"
  )(implicit
    ev1: At.Aux[S, D, V],
    ev2: Prepend.Aux[S, String :: HNil, Out]
  ): FromSelectedAndContent[S, Out, V] = (sel: Position[S], con: Content[V]) => sel
    .append(name.format(sel(dim).toString, con.value.toString))
    .toOption
}

