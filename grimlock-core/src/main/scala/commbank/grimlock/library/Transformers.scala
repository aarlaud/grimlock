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

package commbank.grimlock.library.transform

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.extract.Extract
import commbank.grimlock.framework.metadata.{
  CategoricalType,
  ContinuousSchema,
  DiscreteSchema,
  NominalSchema,
  NumericType,
  OrdinalSchema,
  Type
}
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.transform.{ Transformer, TransformerWithValue }

import shapeless.{ ::, HList, HNil }

private[transform] object Transform {
  def check[P <: HList](cell: Cell[P], t: Type): Boolean = cell.content.schema.classification.isOfType(t)

  def presentDouble[
    P <: HList
  ](
    cell: Cell[P],
    f: (Double) => Double
  ): TraversableOnce[Cell[P]] = (check(cell, NumericType), cell.content.value.asDouble) match {
    case (true, Some(d)) => List(Cell(cell.position, Content(ContinuousSchema[Double](), f(d))))
    case _ => List()
  }

  def presentDoubleWithValue[
    P <: HList,
    W
  ](
    cell: Cell[P],
    ext: W,
    value: Extract[P, W, Double],
    transform: (Double, Double) => Double,
    inverse: Boolean = false
  ): TraversableOnce[Cell[P]] =
    (check(cell, NumericType), cell.content.value.asDouble, value.extract(cell, ext)) match {
      case (true, Some(l), Some(r)) => List(
        Cell(cell.position, Content(ContinuousSchema[Double](), if (inverse) transform(r, l) else transform(l, r)))
      )
      case _ => List()
    }

  def presentDoubleWithTwoValues[
    P <: HList,
    W
  ](
    cell: Cell[P],
    ext: W,
    first: Extract[P, W, Double],
    second: Extract[P, W, Double],
    transform: (Double, Double, Double) => Double
  ): TraversableOnce[Cell[P]] =
    (check(cell, NumericType), cell.content.value.asDouble, first.extract(cell, ext), second.extract(cell, ext)) match {
      case (true, Some(v), Some(f), Some(s)) =>
        List(Cell(cell.position, Content(ContinuousSchema[Double](), transform(v, f, s))))
      case _ => List()
    }
}

/** Create indicator variables. */
case class Indicator[P <: HList]() extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = List(Cell(cell.position, Content(DiscreteSchema[Long](), 1)))
}

/**
 * Binarise categorical variables.
 *
 * @param pos Function that returns the updated position.
 *
 * @note Binarisation is only applied to categorical variables.
 */
case class Binarise[P <: HList](pos: Locate.FromCell[P, P]) extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] =
    if (Transform.check(cell, CategoricalType)) pos(cell).map(Cell(_, Content(DiscreteSchema[Long](), 1))) else List()
}

/**
 * Normalise numeric variables.
 *
 * @param const Object that will extract, for `cell`, its corresponding normalisation constant.
 *
 * @note Normalisation scales a variable in the range [-1, 1]. It is only applied to numerical variables.
 */
case class Normalise[P <: HList, W](const: Extract[P, W, Double]) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    const,
    (v, n) => v / n
  )
}

/**
 * Standardise numeric variables.
 *
 * @param mean      Object that will extract, for `cell`, its corresponding mean value.
 * @param sd        Object that will extract, for `cell`, its corresponding standard deviation.
 * @param threshold Minimum standard deviation threshold. Values less than this result in standardised value of zero.
 * @param n         Number of times division by standard deviation.
 *
 * @note Standardisation results in a variable with zero mean and variance of one. It is only applied to numerical
 *       variables.
 */
case class Standardise[
  P <: HList,
  W
](
  mean: Extract[P, W, Double],
  sd: Extract[P, W, Double],
  threshold: Double = 1e-4,
  n: Int = 1
) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithTwoValues(
    cell,
    ext,
    mean,
    sd,
    (v, m, s) => if (math.abs(s) < threshold) 0.0 else (v - m) / (n * s)
  )
}

/**
 * Clamp numeric variables.
 *
 * @param lower Object that will extract, for `cell`, its corresponding lower clamping constant.
 * @param upper Object that will extract, for `cell`, its corresponding upper clamping constant.
 *
 * @note Clamping results in a variable not smaller (or greater) than the clamping constants. It is only applied to
 *       numerical variables.
 */
case class Clamp[
  P <: HList,
  W
](
  lower: Extract[P, W, Double],
  upper: Extract[P, W, Double]
) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithTwoValues(
    cell,
    ext,
    lower,
    upper,
    (v, l, u) => if (v < l) l else if (v > u) u else v
  )
}

/**
 * Compute the inverse document frequency.
 *
 * @param freq Object that will extract, for `cell`, its corresponding document frequency.
 * @param idf  Idf function to use.
 *
 * @note Idf is only applied to numerical variables.
 */
case class Idf[
  P <: HList,
  W
](
  freq: Extract[P, W, Double],
  idf: (Double, Double) => Double = (df, n) => math.log(n / (1 + df))
) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    freq,
    (df, n) => idf(df, n)
  )
}

/**
 * Create boolean term frequencies; all term frequencies are binarised.
 *
 * @note Boolean tf is only applied to numerical variables.
 */
case class BooleanTf[P <: HList]() extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = Transform.presentDouble(cell, (v) => 1)
}

/**
 * Create logarithmic term frequencies.
 *
 * @param log  Log function to use.
 *
 * @note Logarithmic tf is only applied to numerical variables.
 */
case class LogarithmicTf[P <: HList](log: (Double) => Double = math.log) extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = Transform.presentDouble(cell, (tf) => 1 + log(tf))
}

/**
 * Create augmented term frequencies.
 *
 * @param max Object that will extract, for `cell`, its corresponding maximum count.
 *
 * @note Augmented tf is only applied to numerical variables.
 */
case class AugmentedTf[P <: HList, W](max: Extract[P, W, Double]) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    max,
    (tf, m) => 0.5 + (0.5 * tf) / m
  )
}

/**
 * Create tf-idf values.
 *
 * @param idf Object that will extract, for `cell`, its corresponding inverse document frequency.
 *
 * @note Tf-idf is only applied to numerical variables.
 */
case class TfIdf[P <: HList, W](idf: Extract[P, W, Double]) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    idf,
    (t, i) => t * i
  )
}

/**
 * Add a value.
 *
 * @param value Object that will extract, for `cell`, its corresponding value to add.
 *
 * @note Add is only applied to numerical variables.
 */
case class Add[P <: HList, W](value: Extract[P, W, Double]) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    value,
    (l, r) => l + r
  )
}

/**
 * Subtract a value.
 *
 * @param value   Object that will extract, for `cell`, its corresponding value to subtract.
 * @param inverse Indicator specifying order of subtract.
 *
 * @note Subtract is only applied to numerical variables.
 */
case class Subtract[
  P <: HList,
  W
](
  value: Extract[P, W, Double],
  inverse: Boolean = false
) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    value,
    (l, r) => l - r,
    inverse
  )
}

/**
 * Multiply a value.
 *
 * @param value Object that will extract, for `cell`, its corresponding value to multiply by.
 *
 * @note Multiply is only applied to numerical variables.
 */
case class Multiply[P <: HList, W](value: Extract[P, W, Double]) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    value,
    (l, r) => l * r
  )
}

/**
 * Divide a value.
 *
 * @param value   Object that will extract, for `cell`, its corresponding value to divide by.
 * @param inverse Indicator specifying order of division.
 *
 * @note Fraction is only applied to numerical variables.
 */
case class Fraction[
  P <: HList,
  W
](
  value: Extract[P, W, Double],
  inverse: Boolean = false
) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] = Transform.presentDoubleWithValue(
    cell,
    ext,
    value,
    (l, r) => l / r,
    inverse
  )
}

/**
 * Raise value to a power.
 *
 * @param power The power to raise to.
 *
 * @note Power is only applied to numerical variables.
 */
case class Power[P <: HList](power: Double) extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = Transform.presentDouble(cell, (d) => math.pow(d, power))
}

/**
 * Take square root of a value.
 *
 * @note SquareRoot is only applied to numerical variables.
 */
case class SquareRoot[P <: HList]() extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = Transform.presentDouble(cell, (d) => math.sqrt(d))
}

/**
 * Convert a numeric value to categorical.
 *
 * @param extractor Object that will extract, for `cell`, its corresponding bins.
 *
 * @note Cut is only applied to numerical variables.
 */
case class Cut[P <: HList, W](bins: Extract[P, W, List[Double]]) extends TransformerWithValue[P, P] {
  type V = W

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[P]] =
    (Transform.check(cell, NumericType), cell.content.value.asDouble, bins.extract(cell, ext)) match {
      case (true, Some(v), Some(b)) =>
        val bstr = b.sliding(2).map("(" + _.mkString(",") + "]").toList

        // TODO: Add correct Ordering to bstr
        List(Cell(cell.position, Content(OrdinalSchema[String](bstr.toSet), bstr(b.lastIndexWhere(_ < v)))))
      case _ => List()
    }
}

/** Trait that defined various rules for cutting continuous data. */
trait CutRules[E[_]] {

  /** Type of statistics data from which the number of bins is computed. */
  type Stats[K <: HList, V <: HList] = Map[Position[K], Map[Position[V], Content]]

  /**
   * Define range of `k` approximately equal size bins.
   *
   * @param ext A `E` containing the feature statistics.
   * @param min Key (into `ext`) that identifies the minimum value.
   * @param max Key (into `ext`) that identifies the maximum value.
   * @param k   The number of bins.
   *
   * @return A `E` holding the break values.
   */
  def fixed[
    K <: HList,
    V <: HList
  ](
    ext: E[Stats[K, V]],
    min: Position[V],
    max: Position[V],
    k: Long
  ): E[Map[Position[K], List[Double]]]

  protected def fixedFromStats[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    min: Position[V],
    max: Position[V],
    k: Long
  ): Map[Position[K], List[Double]] = cut(stats, min, max, (_: Map[Position[V], Content]) => Option(k))

  /**
   * Define range of bins based on the square-root choice.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   *
   * @return A `E` holding the break values.
   */
  def squareRootChoice[
    K <: HList,
    V <: HList
  ](
    ext: E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): E[Map[Position[K], List[Double]]]

  protected def squareRootChoiceFromStats[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): Map[Position[K], List[Double]] = cut(
    stats,
    min,
    max,
    (m: Map[Position[V], Content]) => extract(m, count).map(n => math.round(math.sqrt(n)))
  )

  /**
   * Define range of bins based on Sturges' formula.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   *
   * @return A `E` holding the break values.
   */
  def sturgesFormula[
    K <: HList,
    V <: HList
  ](
    ext: E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): E[Map[Position[K], List[Double]]]

  protected def sturgesFormulaFromStats[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): Map[Position[K], List[Double]] = cut(
    stats,
    min,
    max,
    (m: Map[Position[V], Content]) => extract(m, count).map(n => math.ceil(log2(n) + 1).toLong)
  )

  /**
   * Define range of bins based on the Rice rule.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   *
   * @return A `E` holding the break values.
   */
  def riceRule[
    K <: HList,
    V <: HList
  ](
    ext: E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): E[Map[Position[K], List[Double]]]

  protected def riceRuleFromStats[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): Map[Position[K], List[Double]] = cut(
    stats,
    min,
    max,
    (m: Map[Position[V], Content]) => extract(m, count).map(n => math.ceil(2 * math.pow(n, 1.0 / 3.0)).toLong)
  )

  /**
   * Define range of bins based on Doane's formula.
   *
   * @param ext      A `E` containing the feature statistics.
   * @param count    Key (into `ext`) that identifies the number of features.
   * @param min      Key (into `ext`) that identifies the minimum value.
   * @param max      Key (into `ext`) that identifies the maximum value.
   * @param skewness Key (into `ext`) that identifies the skewwness.
   *
   * @return A `E` holding the break values.
   */
  def doanesFormula[
    K <: HList,
    V <: HList
  ](
    ext: E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V],
    skewness: Position[V]
  ): E[Map[Position[K], List[Double]]]

  protected def doanesFormulaFromStats[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    count: Position[V],
    min: Position[V],
    max: Position[V],
    skewness: Position[V]
  ): Map[Position[K], List[Double]] = cut(
    stats,
    min,
    max,
    (m: Map[Position[V], Content]) => (extract(m, count), extract(m, skewness)) match {
      case (Some(n), Some(s)) =>
        Option(math.round(1 + log2(n) + log2(1 + math.abs(s) / math.sqrt((6 * (n - 2)) / ((n + 1) * (n + 3))))))
      case _ => None
    }
  )

  /**
   * Define range of bins based on Scott's normal reference rule.
   *
   * @param ext   A `E` containing the feature statistics.
   * @param count Key (into `ext`) that identifies the number of features.
   * @param min   Key (into `ext`) that identifies the minimum value.
   * @param max   Key (into `ext`) that identifies the maximum value.
   * @param sd    Key (into `ext`) that identifies the standard deviation.
   *
   * @return A `E` holding the break values.
   */
  def scottsNormalReferenceRule[
    K <: HList,
    V <: HList
  ](
    ext: E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V],
    sd: Position[V]
  ): E[Map[Position[K], List[Double]]]

  protected def scottsNormalReferenceRuleFromStats[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    count: Position[V],
    min: Position[V],
    max: Position[V],
    sd: Position[V]
  ): Map[Position[K], List[Double]] = cut(
    stats,
    min,
    max,
    (m: Map[Position[V], Content]) => (extract(m, count), extract(m, min), extract(m, max), extract(m, sd)) match {
      case (Some(n), Some(l), Some(u), Some(s)) =>
        Option(math.ceil((u - l) / (3.5 * s / math.pow(n, 1.0 / 3.0))).toLong)
      case _ => None
    }
  )

  /**
   * Return a `E` holding the user defined break values.
   *
   * @param range A map (holding for each key) the bins range of that feature.
   */
  def breaks[T <% K, K <: Value[_]](range: Map[T, List[Double]]): E[Map[Position[K :: HNil], List[Double]]]

  protected def breaksFromMap[
    T <% K,
    K <: Value[_]
  ](
    range: Map[T, List[Double]]
  ): Map[Position[K :: HNil], List[Double]] = range.map { case (p, l) => (Position(implicitly[K](p)), l) }

  // TODO: Add 'right' and 'labels' options (analogous to R's)
  private def cut[
    K <: HList,
    V <: HList
  ](
    stats: Stats[K, V],
    min: Position[V],
    max: Position[V],
    bins: (Map[Position[V], Content]) => Option[Long]
  ): Map[Position[K], List[Double]] = stats
    .flatMap { case (pos, map) =>
      (extract(map, min), extract(map, max), bins(map)) match {
        case (Some(l), Some(u), Some(k)) =>
          val delta = math.abs(u - l)
          val range = (l to u by (delta / k)).tail.toList

          Option((pos, (l - 0.001 * delta) :: range))
        case _ => None
      }
    }

  private def extract[V <: HList](ext: Map[Position[V], Content], key: Position[V]): Option[Double] = ext
    .get(key)
    .flatMap(_.value.asDouble)

  private def log2(x: Double): Double = math.log(x) / math.log(2)
}

/**
 * Check if a cell matches a predicate.
 *
 * @param comparer Function that checks if the cells matched a predicate.
 *
 * @note The returned cells contain boolean content.
 */
case class Compare[P <: HList](comparer: (Cell[P]) => Boolean) extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = List(
    Cell(cell.position, Content(NominalSchema[Boolean](), comparer(cell)))
  )
}

