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
import commbank.grimlock.framework.encoding.{ Value, StringValue }
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.{ ::, HList, Nat }

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
  type FromCellAndOptionalValue[P <: HList, Q <: HList] = (Cell[P], Option[Value[_]]) => Option[Position[Q]]

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
  type FromSelectedAndContent[S <: HList, Q <: HList] = (Position[S], Content) => Option[Position[Q]]

  /**
   * Rename a dimension.
   *
   * @param dim  The dimension to rename.
   * @param name The rename pattern. Use `%1$``s` for the coordinate.
   */
  def RenameDimension[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    Q <: HList
  ](
    dim: D,
    name: String
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],
    ev2: Position.UpdateConstraints[P, D, StringValue, Q]
  ): FromCell[P, Q] = (cell: Cell[P]) => cell
    .position
    .update(dim, StringValue(name.format(cell.position(dim).toShortString)))
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
    V <: Value[_],
    Q <: HList
  ](
    dim: D,
    name: String = "%1$s=%2$s"                                 // TODO: Use a function to allow other than String
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],                   // TODO: Can V be omitted?
    ev2: Position.UpdateConstraints[P, D, StringValue, Q]
  ): FromCell[P, Q] = (cell: Cell[P]) => cell
    .position
    .update(dim, StringValue(name.format(cell.position(dim).toShortString, cell.content.value.toShortString)))
    .toOption

  /**
   * Append a coordinate to the position.
   *
   * @param name The coordinate to append to the outcome cell.
   */
  def AppendValue[
    P <: HList,
    T <% V,
    V <: Value[_],
    Q <: HList
  ](
    name: T
  )(implicit
    ev: Position.AppendConstraints[P, V, Q]
  ): FromCell[P, Q] = (cell: Cell[P]) => cell.position.append(name).toOption

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
    S <: HList,
    R <: HList
  ](
    slice: Slice[P, S, R],
    pattern: String,
    all: Boolean = false,
    separator: String = "|"
  )(implicit
    ev1: Position.ValueConstraints[StringValue :: R],
    ev2: Position.ListConstraints[S]
  ): FromPairwiseCells[P, StringValue :: R] = (left: Cell[P], right: Cell[P]) => {
    val reml = slice.remainder(left.position)
    val remr = slice.remainder(right.position)

    if (all || reml == remr)
      reml
        .prepend(
          StringValue(
            pattern.format(
              slice.selected(left.position).toShortString(separator),
              slice.selected(right.position).toShortString(separator)
            )
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
    V <: Value[_],
    Q <: HList
  ](
    dim: D
  )(implicit
    ev1: Position.IndexConstraints[R, D, V],
    ev2: Position.AppendConstraints[S, V, Q]
  ): FromSelectedAndRemainder[S, R, Q] = (sel: Position[S], rem: Position[R]) => sel.append(rem(dim)).toOption

  /**
   * Extract position using string of `rem`.
   *
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendRemainderString[
    S <: HList,
    R <: HList,
    Q <: HList
  ](
    separator: String = "|"
  )(implicit
    ev1: Position.ListConstraints[R],
    ev2: Position.AppendConstraints[S, StringValue, Q]
  ): FromSelectedAndRemainder[S, R, Q] = (sel: Position[S], rem: Position[R]) => sel
    .append(StringValue(rem.toShortString(separator)))
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
    Q <: HList
  ](
    pattern: String,
    separator: String = "|"
  )(implicit
    ev1: Position.ListConstraints[R],
    ev2: Position.AppendConstraints[S, StringValue, Q]
  ): FromSelectedAndPairwiseRemainder[S, R, Q] = (sel: Position[S], curr: Position[R], prev: Position[R]) => sel
    .append(StringValue(pattern.format(prev.toShortString(separator), curr.toShortString(separator))))
    .toOption

  /**
   * Expand position by appending a string coordinate from double.
   *
   * @param name The name pattern. Use `%1$``s` for the output pattern.
   */
  def AppendDoubleString[
    S <: HList,
    Q <: HList
  ](
    name: String = "%1$f%%"
  )(implicit
    ev: Position.AppendConstraints[S, StringValue, Q]
  ): FromSelectedAndOutput[S, Double, Q] = (sel: Position[S], value: Double) => sel
    .append(StringValue(name.format(value)))
    .toOption

  /** Append the content string to the position. */
  def AppendContentString[
    S <: HList,
    Q <: HList
  ](implicit
    ev: Position.AppendConstraints[S, StringValue, Q]
  ): FromSelectedAndContent[S, Q] = (sel: Position[S], con: Content) => sel
    .append(StringValue(con.value.toShortString))
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
    V <: Value[_],
    Q <: HList
  ](
    dim: D,
    name: String = "%1$s=%2$s"
  )(implicit
    ev1: Position.IndexConstraints[S, D, V],
    ev2: Position.AppendConstraints[S, StringValue, Q]
  ): FromSelectedAndContent[S, Q] = (sel: Position[S], con: Content) => sel
    .append(StringValue(name.format(sel(dim).toShortString, con.value.toShortString)))
    .toOption
}

