// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.library.window

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.utility._
import au.com.cba.omnia.grimlock.framework.window._

/** Base trait for computing a moving average. */
trait MovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends Window[S, R, S#M] {

  /** The dimension in `rem` from which to get the coordinate to append to `sel`. */
  val dim: Dimension

  protected def getCollection(sel: S, coord: Value, value: Double): Collection[Cell[S#M]] = {
    Collection(Cell[S#M](sel.append(coord), Content(ContinuousSchema[Codex.DoubleCodex](), value)))
  }

  protected def getDouble(con: Content): Double = con.value.asDouble.getOrElse(Double.NaN)

  protected def getCurrent(rem: R, con: Content): (Value, Double) = (rem(dim), getDouble(con))
}

/**
 * Trait for computing moving average in batch mode; that is, keep the last N values and compute the moving average
 * from it.
 */
trait BatchMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends MovingAverage[S, R] {
  type T = List[(Value, Double)]

  /** Size of the window. */
  val window: Int

  /** Indicates if averages should be output when a full window isn't available yet. */
  val all: Boolean

  protected val idx: Int

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    val curr = getCurrent(rem, cell.content)

    (List(curr), if (all) { getCollection(cell.position, curr._1, curr._2) } else { Collection() })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val lst = updateList(rem, cell.content, t)
    val out = (all || lst.size == window) match {
      case true => getCollection(cell.position, lst(math.min(idx, lst.size - 1))._1, compute(lst))
      case false => Collection[Cell[S#M]]()
    }

    (lst, out)
  }

  private def updateList(rem: R, con: Content, lst: List[(Value, Double)]): List[(Value, Double)] = {
    (if (lst.size == window) { lst.tail } else { lst }) :+ getCurrent(rem, con)
  }

  protected def compute(lst: List[(Value, Double)]): Double
}

/**
 * Compute simple moving average over last `window` values.
 *
 * @param window Size of the window.
 * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param all    Indicates if averages should be output when a full window isn't available yet.
 */
case class SimpleMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  window: Int, dim: Dimension = First, all: Boolean = false) extends BatchMovingAverage[S, R] {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute centered moving average over last `2 * width + 1` values.
 *
 * @param width Number of values before and after a given value to use when computing the moving average.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class CenteredMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  width: Int, dim: Dimension = First) extends BatchMovingAverage[S, R] {
  val window = 2 * width + 1
  val all = false
  protected val idx = width

  protected def compute(lst: List[(Value, Double)]): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute weighted moving average over last `window` values.
 *
 * @param window Size of the window.
 * @param dim    The dimension in `rem` from which to get the coordinate to append to `sel`.
 * @param all    Indicates if averages should be output when a full window isn't available yet.
 */
case class WeightedMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  window: Int, dim: Dimension = First, all: Boolean = false) extends BatchMovingAverage[S, R] {
  protected val idx = window - 1

  protected def compute(lst: List[(Value, Double)]): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/** Trait for computing moving average in online mode. */
trait OnlineMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition]
  extends MovingAverage[S, R] {
  type T = (Double, Long)

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    val curr = getCurrent(rem, cell.content)

    ((curr._2, 1), getCollection(cell.position, curr._1, curr._2))
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val curr = compute(getDouble(cell.content), t)

    ((curr, t._2 + 1), getCollection(cell.position, rem(dim), curr))
  }

  protected def compute(curr: Double, t: T): Double
}

/**
 * Compute cumulatve moving average.
 *
 * @param dim  The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class CumulativeMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  dim: Dimension = First) extends OnlineMovingAverage[S, R] {
  protected def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/**
 * Compute exponential moving average.
 *
 * @param alpha Degree of weighting coefficient.
 * @param dim   The dimension in `rem` from which to get the coordinate to append to `sel`.
 */
case class ExponentialMovingAverage[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  alpha: Double, dim: Dimension = First) extends OnlineMovingAverage[S, R] {
  protected def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/**
 * Compute cumulative sum.
 *
 * @param separator The separator to use for the appended coordinate.
 *
 * @note Non-numeric values are silently ignored.
 */
// TODO: Test this
case class CumulativeSum[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  separator: String = "|") extends Window[S, R, S#M] {
  type T = Option[Double]

  val schema = ContinuousSchema[Codex.DoubleCodex]()

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    val value = cell.content.value.asDouble

    (value, value match {
      case Some(d) => Collection(cell.position.append(rem.toShortString(separator)), Content(schema, d))
      case None => Collection()
    })
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val coord = rem.toShortString(separator)

    (t, cell.content.value.asDouble) match {
      case (None, None) => (None, Collection())
      case (None, Some(d)) => (Some(d), Collection(cell.position.append(coord), Content(schema, d)))
      case (Some(p), None) => (Some(p), Collection())
      case (Some(p), Some(d)) => (Some(p + d), Collection(cell.position.append(coord), Content(schema, p + d)))
    }
  }
}

/**
 * Compute sliding binary operator on sequential numeric cells.
 *
 * @param binop     The binary operator to apply to two sequential numeric cells.
 * @param name      The name of the appended (result) coordinate. Use `%[12]$``s` for the string representations of
 *                  left (first) and right (second) coordinates.
 * @param separator The separator to use for the appended coordinate.
 *
 * @note Non-numeric values are silently ignored.
 */
// TODO: Test this
case class BinOp[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition](
  binop: (Double, Double) => Double, name: String = "f(%1$s, %2$s)",
  separator: String = "|") extends Window[S, R, S#M] {
  type T = (Option[Double], String)

  def initialise(cell: Cell[S], rem: R): (T, Collection[Cell[S#M]]) = {
    ((cell.content.value.asDouble, rem.toShortString(separator)), Collection())
  }

  def present(cell: Cell[S], rem: R, t: T): (T, Collection[Cell[S#M]]) = {
    val coord = rem.toShortString(separator)
    val schema = ContinuousSchema[Codex.DoubleCodex]()

    (t, cell.content.value.asDouble) match {
      case ((None, _), None) =>
        ((None, coord), Collection())
      case ((None, _), Some(d)) =>
        ((Some(d), coord), Collection())
      case ((Some(p), _), None) =>
        ((None, coord), Collection())
      case ((Some(p), c), Some(d)) =>
        ((Some(d), coord), Collection(cell.position.append(name.format(c, coord)), Content(schema, binop(p, d))))
    }
  }
}

case class Quantile[S <: Position with ExpandablePosition, R <: Position with ExpandablePosition, W](
  probs: List[Double], count: Extract[S, W, Long], min: Extract[S, W, Double], max: Extract[S, W, Double],
  quantise: Quantile.Quantiser, name: String = "%1$f%%") extends WindowWithValue[S, R, S#M] {
  type T = (Option[Double], Long, List[(Long, Double, String)])
  type V = W

  def initialiseWithValue(cell: Cell[S], rem: R, ext: V): (T, Collection[Cell[S#M]]) = {
    val n = count.extract(cell, ext).get
    val s = probs
      .zipWithIndex
      .map {
        case (p, i) =>
          val (j, g) = quantise(p, n)

          (j, g, name.format(i + 1))
      }

    val col = Collection(List(
      min.extract(cell, ext).map { case v => Cell[S#M](cell.position.append(name.format(0)),
        Content(ContinuousSchema[Codex.DoubleCodex](), v)) }.toList,
      max.extract(cell, ext).map { case v => Cell[S#M](cell.position.append(name.format(1)),
        Content(ContinuousSchema[Codex.DoubleCodex](), v)) }.toList).flatten)

    ((cell.content.value.asDouble, 0, s), col)
  }

  def presentWithValue(cell: Cell[S], rem: R, ext: V, t: T): (T, Collection[Cell[S#M]]) = {
    val curr = cell.content.value.asDouble
    val idx = t._3.map(_._1).indexOf(t._2)
    val col = (idx != -1) match {
      case true =>
        val g = t._3(idx)._2
        val n = t._3(idx)._3
        val b = curr.flatMap {
          case c => t._1.map {
            case p => Cell[S#M](cell.position.append(n),
              Content(ContinuousSchema[Codex.DoubleCodex](), (1 - g) * p + g * c))
          }
        }

        Collection[Cell[S#M]](b.toList)
      case false => Collection[Cell[S#M]]()
    }

    ((curr, t._2 + 1, t._3), col)
  }
}

object Quantile {
  type Quantiser = (Double, Long) => (Long, Double)

  val Type1: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, 0)

    (j, if (g == 0) 0 else 1)
  }
  val Type2: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, 0)

    (j, if (g == 0) 0.5 else 1)
  }
  val Type3: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, -0.5)

    (j, if (g == 0 && j % 2 == 0) 0 else 1)
  }
  val Type4: Quantiser = (p: Double, n: Long) => { TypeX(p, n, 0) }
  val Type5: Quantiser = (p: Double, n: Long) => { TypeX(p, n, 0.5) }
  val Type6: Quantiser = (p: Double, n: Long) => { TypeX(p, n, p) }
  val Type7: Quantiser = (p: Double, n: Long) => { TypeX(p, n, 1 - p) }
  val Type8: Quantiser = (p: Double, n: Long) => { TypeX(p, n, (p + 1) / 3) }
  val Type9: Quantiser = (p: Double, n: Long) => { TypeX(p, n, (p / 4) + 3 / 8) }

  private val TypeX = (p: Double, n: Long, m: Double) => {
    val npm = n * p + m
    val j = math.floor(npm).toLong

    (j, npm - j)
  }
}

