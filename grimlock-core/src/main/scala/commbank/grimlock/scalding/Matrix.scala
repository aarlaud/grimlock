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

package commbank.grimlock.scalding

import commbank.grimlock.framework.{
  Cell,
  Compactable,
  Locate,
  Matrix => FwMatrix,
  Matrix1D => FwMatrix1D,
  Matrix2D => FwMatrix2D,
  Matrix3D => FwMatrix3D,
  Matrix4D => FwMatrix4D,
  Matrix5D => FwMatrix5D,
  Matrix6D => FwMatrix6D,
  Matrix7D => FwMatrix7D,
  Matrix8D => FwMatrix8D,
  Matrix9D => FwMatrix9D,
  MultiDimensionMatrix => FwMultiDimensionMatrix,
  Persist => FwPersist,
  Stream
}
import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{
  Binary,
  Default,
  InMemory,
  Redistribute,
  Reducers,
  Ternary,
  Tuner
}
import commbank.grimlock.framework.metadata.{ DiscreteSchema, NominalSchema, Type }
import commbank.grimlock.framework.pairwise.{ Comparer, Operator, OperatorWithValue }
import commbank.grimlock.framework.partition.{ Partitioner, PartitionerWithValue }
import commbank.grimlock.framework.position.{ listSetAdditiveCollection, Position, Positions => FwPositions, Slice }
import commbank.grimlock.framework.sample.{ Sampler, SamplerWithValue }
import commbank.grimlock.framework.squash.{ Squasher, SquasherWithValue }
import commbank.grimlock.framework.transform.{ Transformer, TransformerWithValue }
import commbank.grimlock.framework.utility.Escape
import commbank.grimlock.framework.window.{ Window, WindowWithValue }

import commbank.grimlock.scalding.distance.PairwiseDistance
import commbank.grimlock.scalding.distribution.ApproximateDistribution
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.tuner.{ MapMapSideJoin, SetMapSideJoin }
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._
import commbank.grimlock.scalding.statistics.Statistics

import com.twitter.algebird.Semigroup
import com.twitter.scalding.typed.ValuePipe

import org.apache.hadoop.io.Writable

import scala.collection.immutable.ListSet

import shapeless.{ ::, =:!=, HNil, IsDistinctConstraint, Nat }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ GT, GTEq, LTEq, Pred, ToInt }
import shapeless.syntax.sized._

/** Case class matrix operations using a `TypedPipe[Cell[P]]`. */
case class Matrix[
  P <: HList
](
  data: Context.U[Cell[P]]
) extends FwMatrix[P, Context]
  with Persist[Cell[P]]
  with ApproximateDistribution[P]
  with Statistics[P] {
  def change[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = InMemory()
  )(
    positions: Context.U[Position[slice.S]],
    schema: Content.Parser[Content[_]],
    writer: FwPersist.TextWriter[Cell[P]]
  )(implicit
    ev: FwMatrix.ChangeTuner[Context.U, T]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val msj = Option(SetMapSideJoin[Position[slice.S], Cell[P]]())

    val result = data
      .map { case c => (slice.selected(c.position), c) }
      .tunedLeftJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .flatMap {
        case (_, (c, Some(_))) => schema(c.content.value.toShortString)
          .map { case con => List(Right(Cell(c.position, con))) }
          .getOrElse(writer(c).map { case e => Left(e) })
        case (_, (c, None)) => List(Right(c))
      }

    (result.collect { case Right(cell) => cell }, result.collect { case Left(error) => error })
  }

  def compact(): Context.E[Map[Position[P], Content[_]]] = {
    val semigroup = new Semigroup[Map[Position[P], Content]] {
      def plus(l: Map[Position[P], Content], r: Map[Position[P], Content]) = l ++ r
    }

    ValuePipe(Map[Position[P], Content]())
      .leftCross(data.map { case c => Map(c.position -> c.content) }.sum(semigroup))
      .map { case (e, m) => m.getOrElse(e) }
  }

  def compact[
    T <: Tuner,
    V[_ <: HList]
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: Compactable[P, V],
    ev3: FwMatrix.CompactTuner[Context.U, T]
  ): Context.E[Map[Position[slice.S], V[slice.R]]] = {
    val semigroup = new Semigroup[Map[Position[slice.S], V[slice.R]]] {
      def plus(l: Map[Position[slice.S], V[slice.R]], r: Map[Position[slice.S], V[slice.R]]) = l ++ r
    }

    ValuePipe(Map[Position[slice.S], V[slice.R]]())
      .leftCross(
        data
          .map { case c => (slice.selected(c.position), ev2.toMap(slice, c)) }
          .tunedReduce(tuner, (l, r) => ev2.combineMaps(l, r))
          .values
          .sum(semigroup)
      )
      .map { case (e, m) => m.getOrElse(e) }
  }

  def get[
    T <: Tuner
  ](
    positions: Context.U[Position[P]],
    tuner: T = InMemory()
  )(implicit
    ev: FwMatrix.GetTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    val msj = Option(SetMapSideJoin[Position[P], Cell[P]]())

    data
      .map { case c => (c.position, c) }
      .tunedJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .map { case (_, (c, _)) => c }
  }

  def materialise(context: Context): List[Cell[P]] = data
    .toIterableExecution
    .waitFor(context.config, context.mode)
    .getOrElse(Iterable.empty)
    .toList

  def names[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: FwPositions.NamesTuner[Context.U, T]
  ): Context.U[Position[slice.S]] = data
    .map { case c => slice.selected(c.position) }
    .tunedDistinct(tuner)

  def pairwise[
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    operators: Operator[P, Q]*
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: GT[Q, slice.R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: Operator[P, Q] = if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, data, tuner).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseWithValue[
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    value: Context.E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: GT[Q, slice.R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: OperatorWithValue[P, Q] { type V >: W } =
      if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, data, tuner)
      .flatMapWithValue(value) { case ((lc, rc), vo) =>
        vo.toList.flatMap { case v => operator.computeWithValue(lc, rc, v) }
      }
  }

  def pairwiseBetween[
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    that: Context.U[Cell[P]],
    operators: Operator[P, Q]*
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: GT[Q, slice.R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: Operator[P, Q] = if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, that, tuner).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseBetweenWithValue[
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    that: Context.U[Cell[P]],
    value: Context.E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: GT[Q, slice.R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: OperatorWithValue[P, Q] { type V >: W } =
      if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, that, tuner)
      .flatMapWithValue(value) { case ((lc, rc), vo) =>
        vo.toList.flatMap { case v => operator.computeWithValue(lc, rc, v) }
      }
  }

  def relocate[Q <: HList](locate: Locate.FromCell[P, Q])(implicit ev: GTEq[Q, P]): Context.U[Cell[Q]] = data
    .flatMap { case c => locate(c).map { case p => Cell(p, c.content) } }

  def relocateWithValue[
    Q <: HList,
    W
  ](
    value: Context.E[W],
    locate: Locate.FromCellWithValue[P, Q, W]
  )(implicit
    ev: GTEq[Q, P]
  ): Context.U[Cell[Q]] = data.flatMapWithValue(value) { case (c, vo) =>
    vo.flatMap { case v => locate(c, v).map { case p => Cell(p, c.content) } }
  }

  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[Cell[P]] = saveText(context, file, writer, tuner)

  def set[
    T <: Tuner
  ](
    values: Context.U[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SetTuner[Context.U, T]
  ): Context.U[Cell[P]] = data
    .map { case c => (c.position, c) }
    .tunedOuterJoin(tuner, values.map { case c => (c.position, c) })
    .flatMap { case (_, (co, cn)) => cn.orElse(co) }

  def shape[
    T <: Tuner
  ](
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.ShapeTuner[Context.U, T]
  ): Context.U[Cell[_1]] = data
    .flatMap { case c => c.position.coordinates.map { case c => c.toString }.zipWithIndex }
    .tunedDistinct(tuner)
    .map { case (s, i) => i }
    .tunedSize(tuner)
    .map { case (i, c) => Cell(Position(i + 1), Content(DiscreteSchema[Long](), c)) }

  def size[
    D <: Nat : ToInt,
    T <: Tuner
  ](
    dim: D,
    distinct: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: FwMatrix.SizeTuner[Context.U, T]
  ): Context.U[Cell[_1]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) coords else coords.tunedDistinct(tuner)(Value.ordering)

    dist
      .map { case c => 1L }
      .sum
      .map { case sum => Cell(Position(Nat.toInt[D]), Content(DiscreteSchema[Long](), sum)) }
  }

  def slice[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = InMemory()
  )(
    keep: Boolean,
    positions: Context.U[Position[slice.S]]
  )(implicit
    ev: FwMatrix.SliceTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    val msj = Option(SetMapSideJoin[Position[slice.S], Cell[P]]())

    data
      .map { case c => (slice.selected(c.position), c) }
      .tunedLeftJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
  }

  def slide[
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    ascending: Boolean,
    windows: Window[P, slice.S, slice.R, Q]*
  )(implicit
    ev1: slice.R =:!= HNil,
    ev2: GT[Q, slice.S],
    ev3: FwMatrix.SlideTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val window: Window[P, slice.S, slice.R, Q] = if (windows.size == 1) windows.head else windows.toList

    data
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))) }
      .tunedRedistribute(tuner) // TODO: Is this needed?
      .tuneReducers(tuner)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMap {
        case (p, Some((_, to))) => to.flatMap { case o => window.present(p, o) }
        case _ => List()
      }
  }

  def slideWithValue[
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    ascending: Boolean,
    value: Context.E[W],
    windows: WindowWithValue[P, slice.S, slice.R, Q] { type V >: W }*
  )(implicit
    ev1: slice.R =:!= HNil,
    ev2: GT[Q, slice.S],
    ev3: FwMatrix.SlideTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val window: WindowWithValue[P, slice.S, slice.R, Q] { type V >: W } =
      if (windows.size == 1) windows.head else windows.toList

    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.map { case v => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, v))) }
      }
      .tunedRedistribute(tuner) // TODO: Is this needed?
      .tuneReducers(tuner)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMapWithValue(value) {
        case ((p, Some((_, to))), Some(v)) => to.flatMap { case o => window.presentWithValue(p, o, v) }
        case _ => List()
      }
  }

  def split[I](partitioners: Partitioner[P, I]*): Context.U[(I, Cell[P])] = {
    val partitioner: Partitioner[P, I] = if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
  }

  def splitWithValue[
    I,
    W
  ](
    value: Context.E[W],
    partitioners: PartitionerWithValue[P, I] { type V >: W }*
  ): Context.U[(I, Cell[P])] = {
    val partitioner: PartitionerWithValue[P, I] { type V >: W } =
      if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMapWithValue(value) { case (c, vo) =>
      vo.toList.flatMap { case v => partitioner.assignWithValue(c, v).map { case q => (q, c) } }
    }
  }

  def stream[
    Q <: HList
  ](
    command: String,
    files: List[String],
    writer: FwPersist.TextWriter[Cell[P]],
    parser: Cell.TextParser[Q],
    reducers: Reducers,
    hash: (Position[P]) => Int
  ): (Context.U[Cell[Q]], Context.U[String]) = {
    val tuner = Default(reducers)

    val result = data
      .flatMap { case c => writer(c).map { case s => (hash(c.position) % reducers.reducers, s) } }
      .tuneReducers(tuner)
      .tunedStream(tuner, (key, itr) => Stream.delegate(command, files)(key, itr).flatMap { case s => parser(s) })

    (result.collect { case (_, Right(c)) => c }, result.collect { case (_, Left(e)) => e })
  }

  def streamByPosition[
    Q <: HList
  ](
    slice: Slice[P]
  )(
    command: String,
    files: List[String],
    writer: FwPersist.TextWriterByPosition[Cell[P]],
    parser: Cell.TextParser[Q],
    reducers: Reducers
  )(implicit
    ev: GTEq[Q, slice.S]
  ): (Context.U[Cell[Q]], Context.U[String]) = {
    val tuner = Default(reducers)
    val murmur = new scala.util.hashing.MurmurHash3.ArrayHashing[Value]()
    val (rows, _) = Util.pivot(data, slice, tuner)

    val result = rows
      .flatMap { case (key, list) => writer(list.map { case (_, v) => v })
        .map { case s => (murmur.hash(key.coordinates.toArray) % reducers.reducers, s) }
      }
      .tuneReducers(tuner)
      .tunedStream(tuner, (key, itr) => Stream.delegate(command, files)(key, itr).flatMap { case s => parser(s) })

    (result.collect { case (_, Right(c)) => c }, result.collect { case (_, Left(e)) => e })
  }

  def subset(samplers: Sampler[P]*): Context.U[Cell[P]] = {
    val sampler: Sampler[P] = if (samplers.size == 1) samplers.head else samplers.toList

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](value: Context.E[W], samplers: SamplerWithValue[P] { type V >: W }*): Context.U[Cell[P]] = {
    val sampler: SamplerWithValue[P] { type V >: W } = if (samplers.size == 1) samplers.head else samplers.toList

    data.filterWithValue(value) { case (c, vo) => vo.map { case v => sampler.selectWithValue(c, v) }.getOrElse(false) }
  }

  def summarise[
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    aggregators: Aggregator[P, slice.S, Q]*
  )(implicit
    ev1: GTEq[Q, slice.S],
    ev2: Aggregator.Validate[P, slice.S, Q],
    ev3: FwMatrix.SummariseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val aggregator = ev2.check(aggregators)

    data
      .flatMap { case c => aggregator.prepare(c).map { case t => (slice.selected(c.position), t) } }
      .tunedReduce(tuner, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMap { case (p, t) => aggregator.present(p, t) }
  }

  def summariseWithValue[
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    value: Context.E[W],
    aggregators: AggregatorWithValue[P, slice.S, Q] { type V >: W }*
  )(implicit
    ev1: GTEq[Q, slice.S],
    ev2: AggregatorWithValue.Validate[P, slice.S, Q, W],
    ev3: FwMatrix.SummariseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val aggregator = ev2.check(aggregators)

    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.flatMap { case v => aggregator.prepareWithValue(c, v).map { case t => (slice.selected(c.position), t) } }
      }
      .tunedReduce(tuner, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMapWithValue(value) { case ((p, t), vo) =>
        vo.toList.flatMap { case v => aggregator.presentWithValue(p, t, v) }
      }
  }

  def toSequence[
    K <: Writable,
    V <: Writable
  ](
    writer: FwPersist.SequenceWriter[Cell[P], K, V]
  ): Context.U[(K, V)] = data.flatMap { case c => writer(c) }

  def toText(writer: FwPersist.TextWriter[Cell[P]]): Context.U[String] = data.flatMap { case c => writer(c) }

  def toVector[V](melt: (P) => V): Context.U[Cell[V :: HNil]] = data
    .map { case Cell(p, c) => Cell(Position(melt(p.coordinates)), c) }

  def transform[Q <: HList](transformers: Transformer[P, Q]*)(implicit ev: GTEq[Q, P]): Context.U[Cell[Q]] = {
    val transformer: Transformer[P, Q] = if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMap { case c => transformer.present(c) }
  }

  def transformWithValue[
    Q <: HList,
    W
  ](
    value: Context.E[W],
    transformers: TransformerWithValue[P, Q] { type V >: W }*
  )(implicit
    ev: GTEq[Q, P]
  ): Context.U[Cell[Q]] = {
    val transformer: TransformerWithValue[P, Q] { type V >: W } =
      if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMapWithValue(value) { case (c, vo) => vo.toList.flatMap { case v => transformer.presentWithValue(c, v) } }
  }

  def types[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    specific: Boolean
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: FwMatrix.TypesTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = data
    .map { case Cell(p, c) => (slice.selected(p), c.schema.classification) }
    .tunedReduce(tuner, (lt, rt) => lt.getCommonType(rt))
    .map { case (p, t) => Cell(p, Content(NominalSchema[Type](), if (specific) t else t.getRootType)) }

  def unique[T <: Tuner](tuner: T = Default())(implicit ev: FwMatrix.UniqueTuner[Context.U, T]): Context.U[Content] = {
    val ordering = new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) }

    data
      .map { case c => c.content }
      .tunedDistinct(tuner)(ordering)
  }

  def uniqueByPosition[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= HNil,
    ev2: FwMatrix.UniqueTuner[Context.U, T]
  ): Context.U[(Position[slice.S], Content)] = {
    val ordering = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  def which(predicate: Cell.Predicate[P]): Context.U[Position[P]] = data
    .collect { case c if predicate(c) => c.position }

  def whichByPosition[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = InMemory()
  )(
    predicates: List[(Context.U[Position[slice.S]], Cell.Predicate[P])]
  )(implicit
    ev: FwMatrix.WhichTuner[Context.U, T]
  ): Context.U[Position[P]] = {
    val msj = Option(MapMapSideJoin[Position[slice.S], Cell[P], List[Cell.Predicate[P]]]())

    val pp = predicates
      .map { case (pos, pred) => pos.map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)
      .map { case (pos, pred) => (pos, List(pred)) }
      .tunedReduce(tuner, _ ++ _)

    data
      .map { case c => (slice.selected(c.position), c) }
      .tunedJoin(tuner, pp, msj)
      .collect { case (_, (c, lst)) if (lst.exists(pred => pred(c))) => c.position }
  }

  private def pairwiseTuples[
    T <: Tuner
  ](
    slice: Slice[P],
    comparer: Comparer,
    ldata: Context.U[Cell[P]],
    rdata: Context.U[Cell[P]],
    tuner: T
  ): Context.U[(Cell[P], Cell[P])] = {
    tuner match {
      case InMemory(_) =>
        ldata
          .tunedCross[Cell[P]](
            tuner,
            (lc, rc) => comparer.keep(slice.selected(lc.position), slice.selected(rc.position)),
            rdata
          )
      case _ =>
        def msj[V] = Option(MapMapSideJoin[Position[slice.S], Cell[P], V]())

        val (ct, lt, rt) = tuner match {
          case Ternary(a, s, r) => (a, s, r)
          case _ => (Default(), Default(), Default())
        }

        ldata
          .map { case c => (slice.selected(c.position), c) }
          .tunedJoin(
            lt,
            ldata
              .map { case Cell(p, _) => slice.selected(p) }
              .tunedDistinct(lt)
              .tunedCross[Position[slice.S]](
                ct,
                (lp, rp) => comparer.keep(lp, rp),
                rdata.map { case Cell(p, _) => slice.selected(p) }.tunedDistinct(rt)
              )
              .forceToDisk, // TODO: Should this be configurable?
            msj
          )
          .map { case (_, (lc, rp)) => (rp, lc) }
          .tunedJoin(rt, rdata.map { case c => (slice.selected(c.position), c) }, msj)
          .map { case (_, (lc, rc)) => (lc, rc) }
    }
  }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: HNil]]`. */
case class Matrix1D[V1](data: Context.U[Cell[V1 :: HNil]]) extends FwMatrix1D[V1, Context] with MatrixXD[V1 :: HNil] {
  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (c.position, c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: HNil]] = coordinates(_1, tuner)
    .map { case c1 => Position(c1) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: HNil]]`. */
case class Matrix2D(data: Context.U[Cell[V1 :: V2 :: HNil]]) extends FwMatrix2D[V1, V2, Context] with MatrixXD[V1 :: V2 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2
  )(implicit
    ev1: LTEq[D1, _2],
    ev2: LTEq[D2, _2],
    ev3: D1 =:!= D2
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2]).zipWithIndex.sortBy { case (d, _) => d }.map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(2).get), c) }
  }

  def saveAsCSV[
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    separator: String,
    escapee: Escape,
    writeHeader: Boolean,
    header: String,
    writeRowId: Boolean,
    rowId: String
  )(implicit
    ev: FwMatrix.SaveAsCSVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    val (pt, rt) = tuner match {
      case Binary(p, r) => (p, r)
      case r @ Redistribute(_) => (Default(), r)
      case p @ Default(_) => (p, Default())
      case _ => (Default(), Default())
    }

    val (pivoted, columns) = Util.pivot(data, slice, pt)

    if (writeHeader)
      columns
        .map { case lst =>
          (if (writeRowId) escapee.escape(rowId) + separator else "") + lst
            .map { case p => escapee.escape(p.coordinates.head.toShortString) }
            .mkString(separator)
        }
        .toTypedPipe
        .tunedSaveAsText(context, Redistribute(1), header.format(file))

    pivoted
      .map { case (p, lst) =>
        (if (writeRowId) escapee.escape(p.coordinates.head.toShortString) + separator else "") + lst
          .map { case (_, v) => v.map { case c => escapee.escape(c.content.value.toShortString) }.getOrElse("") }
          .mkString(separator)
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .tunedSaveAsText(context, rt, file)

    data
  }

  def saveAsVW[
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(slice, tuner)(context, file, None, None, tag, dictionary, separator)

  def saveAsVWWithLabels[
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    labels: Context.U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(slice, tuner)(context, file, Option(labels), None, tag, dictionary, separator)

  def saveAsVWWithImportance[
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    importance: Context.U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(slice, tuner)(context, file, None, Option(importance), tag, dictionary, separator)

  def saveAsVWWithLabelsAndImportance[
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    labels: Context.U[Cell[slice.S]],
    importance: Context.U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(
    slice,
    tuner
  )(
    context,
    file,
    Option(labels),
    Option(importance),
    tag,
    dictionary,
    separator
  )

  private def saveVW[
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil],
    tuner: T
  )(
    context: Context,
    file: String,
    labels: Option[Context.U[Cell[slice.S]]],
    importance: Option[Context.U[Cell[slice.S]]],
    tag: Boolean,
    dictionary: String,
    separator: String
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    val msj = Option(MapMapSideJoin[Position[slice.S], String, Cell[slice.S]]())

    val (pt, jt, rt) = tuner match {
      case Ternary(f, s, t) => (f, s, t)
      case Binary(t @ Default(_), r @ Redistribute(_)) => (t, t, r)
      case Binary(p, j) => (p, j, Default())
      case t @ Default(Reducers(_)) => (t, t, Default())
      case _ => (Default(), Default(), Default())
    }

    val (pivoted, columns) = Util.pivot(data, slice, pt)
    val dict = columns.map { case l => l.zipWithIndex.toMap }

    dict
      .flatMap { case m => m.map { case (p, i) => p.coordinates.head.toShortString + separator + i } }
      .tunedSaveAsText(context, Redistribute(1), dictionary.format(file))

    val features = pivoted
      .flatMapWithValue(dict) { case ((key, lst), dct) =>
        dct.map { case d =>
          (
            key,
            lst
              .flatMap { case (p, v) =>
                v.flatMap { case c => c.content.value.asDouble.map { case w => d(p) + ":" + w } }
              }
              .mkString((if (tag) key.coordinates.head.toShortString else "") + "| ", " ", "")
          )
        }
      }

    val weighted = importance match {
      case Some(imp) => features
        .tunedJoin(jt, imp.map { case c => (c.position, c) }, msj)
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => features
    }

    val examples = labels match {
      case Some(lab) => weighted
        .tunedJoin(jt, lab.map { case c => (c.position, c) }, msj)
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .map { case (c1, c2) => Position(c1, c2) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: HNil]]`. */
case class Matrix3D(data: Context.U[Cell[V1 :: V2 :: V3 :: HNil]]) extends FwMatrix3D[V1, V2, V3, Context] with MatrixXD[V1 :: V2 :: V3 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3
  )(implicit
    ev1: LTEq[D1, _3],
    ev2: LTEq[D2, _3],
    ev3: LTEq[D3, _3],
    ev4: IsDistinctConstraint[D1 :: D2 :: D3 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: HNil]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3])
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(3).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .map { case ((c1, c2), c3) => Position(c1, c2, c3) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]`. */
case class Matrix4D(data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]) extends FwMatrix4D[V1, V2, V3, V4, Context] with MatrixXD[V1 :: V2 :: V3 :: V4 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4
  )(implicit
    ev1: LTEq[D1, _4],
    ev2: LTEq[D2, _4],
    ev3: LTEq[D3, _4],
    ev4: LTEq[D4, _4],
    ev5: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4])
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(4).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) =>
        i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: V4 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .map { case (((c1, c2), c3), c4) => Position(c1, c2, c3, c4) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]`. */
case class Matrix5D(data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]) extends FwMatrix5D[V1, V2, V3, V4, V5, Context] with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5
  )(implicit
    ev1: LTEq[D1, _5],
    ev2: LTEq[D2, _5],
    ev3: LTEq[D3, _5],
    ev4: LTEq[D4, _5],
    ev5: LTEq[D5, _5],
    ev6: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4], Nat.toInt[D5])
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(5).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) =>
        i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .map { case ((((c1, c2), c3), c4), c5) => Position(c1, c2, c3, c4, c5) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]`. */
case class Matrix6D(data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]) extends FwMatrix6D[V1, V2, V3, V4, V5, V6, Context] with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6
  )(implicit
    ev1: LTEq[D1, _6],
    ev2: LTEq[D2, _6],
    ev3: LTEq[D3, _6],
    ev4: LTEq[D4, _6],
    ev5: LTEq[D5, _6],
    ev6: LTEq[D6, _6],
    ev7: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4], Nat.toInt[D5], Nat.toInt[D6])
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(6).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .map { case (((((c1, c2), c3), c4), c5), c6) => Position(c1, c2, c3, c4, c5, c6) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]`. */
case class Matrix7D(data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]) extends FwMatrix7D[V1, V2, V3, V4, V5, V6, V7, Context] with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt,
    D7 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7
  )(implicit
    ev1: LTEq[D1, _7],
    ev2: LTEq[D2, _7],
    ev3: LTEq[D3, _7],
    ev4: LTEq[D4, _7],
    ev5: LTEq[D5, _7],
    ev6: LTEq[D6, _7],
    ev7: LTEq[D7, _7],
    ev8: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: D7 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]] = {
    val l = List(
        Nat.toInt[D1],
        Nat.toInt[D2],
        Nat.toInt[D3],
        Nat.toInt[D4],
        Nat.toInt[D5],
        Nat.toInt[D6],
        Nat.toInt[D7]
      )
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(7).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_7)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(_7, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_7, tuner))
    .map { case ((((((c1, c2), c3), c4), c5), c6), c7) => Position(c1, c2, c3, c4, c5, c6, c7) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]`. */
case class Matrix8D(data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]) extends FwMatrix8D[V1, V2, V3, V4, V5, V6, V7, V8, Context] with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt,
    D7 <: Nat : ToInt,
    D8 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7,
    dim8: D8
  )(implicit
    ev1: LTEq[D1, _8],
    ev2: LTEq[D2, _8],
    ev3: LTEq[D3, _8],
    ev4: LTEq[D4, _8],
    ev5: LTEq[D5, _8],
    ev6: LTEq[D6, _8],
    ev7: LTEq[D7, _8],
    ev8: LTEq[D8, _8],
    ev9: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: D7 :: D8 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]] = {
    val l = List(
        Nat.toInt[D1],
        Nat.toInt[D2],
        Nat.toInt[D3],
        Nat.toInt[D4],
        Nat.toInt[D5],
        Nat.toInt[D6],
        Nat.toInt[D7],
        Nat.toInt[D8]
      )
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(8).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_7)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(_7, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (Position(c.position(_8)), (c, i, j, k, l, m, n, o)) }
      .tunedJoin(jt, saveDictionary(_8, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        p + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_7, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_8, tuner))
    .map { case (((((((c1, c2), c3), c4), c5), c6), c7), c8) => Position(c1, c2, c3, c4, c5, c6, c7, c8) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]`. */
case class Matrix9D(data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]) extends FwMatrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9, Context] with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] {
  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt,
    D7 <: Nat : ToInt,
    D8 <: Nat : ToInt,
    D9 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7,
    dim8: D8,
    dim9: D9
  )(implicit
    ev1: LTEq[D1, _9],
    ev2: LTEq[D2, _9],
    ev3: LTEq[D3, _9],
    ev4: LTEq[D4, _9],
    ev5: LTEq[D5, _9],
    ev6: LTEq[D6, _9],
    ev7: LTEq[D7, _9],
    ev8: LTEq[D8, _9],
    ev9: LTEq[D9, _9],
    ev10: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: D7 :: D8 :: D9 :: HNil]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]] = {
    val l = List(
        Nat.toInt[D1],
        Nat.toInt[D2],
        Nat.toInt[D3],
        Nat.toInt[D4],
        Nat.toInt[D5],
        Nat.toInt[D6],
        Nat.toInt[D7],
        Nat.toInt[D8],
        Nat.toInt[D9]
      )
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(9).get), c) }
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_7)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(_7, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (Position(c.position(_8)), (c, i, j, k, l, m, n, o)) }
      .tunedJoin(jt, saveDictionary(_8, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (Position(c.position(_9)), (c, i, j, k, l, m, n, o, p)) }
      .tunedJoin(jt, saveDictionary(_9, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        p + separator +
        q + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_7, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_8, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_9, tuner))
    .map { case ((((((((c1, c2), c3), c4), c5), c6), c7), c8), c9) => Position(c1, c2, c3, c4, c5, c6, c7, c8, c9) }
}

/** Trait for XD specific implementations. */
trait MatrixXD[P <: HList] extends Persist[Cell[P]] {
  def domain[
    T <: Tuner
  ](
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.DomainTuner[Context.U, T]
  ): Context.U[Position[P]] = naturalDomain(tuner)

  def fillHeterogeneous[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    values: Context.U[Cell[slice.S]]
  )(implicit
    ev: FwMatrix.FillHeterogeneousTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    val msj = Option(MapMapSideJoin[Position[slice.S], Position[P], Content]())

    val (dt, vt, jt) = tuner match {
      case Ternary(f, s, t) => (f, s, t)
      case _ => (Default(), Default(), Default())
    }

    naturalDomain(dt)
      .map { case p => (slice.selected(p), p) }
      .tunedJoin(vt, values.map { case c => (c.position, c.content) }, msj)
      .map { case (_, (p, c)) => (p, Cell(p, c)) }
      .tunedLeftJoin(jt, data.map { case c => (c.position, c) })
      .map { case (_, (c, co)) => co.getOrElse(c) }
  }

  def fillHomogeneous[
    T <: Tuner
  ](
    value: Content,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.FillHomogeneousTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    val (dt, jt) = tuner match {
      case Binary(f, s) => (f, s)
      case _ => (Default(), Default())
    }

    naturalDomain(dt)
      .map { case p => (p, ()) }
      .tunedLeftJoin(jt, data.map { case c => (c.position, c) })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  protected def coordinates[
    D <: Nat : ToInt
  ](
    dim: D,
    tuner: Tuner
  )(implicit
    ev: LTEq[D, P]
  ): Context.U[Value] = data.map { case c => c.position(dim) }.tunedDistinct(tuner)(Value.ordering)

  protected def getSaveAsIVTuners(tuner: Tuner): (Tuner, Tuner) = tuner match {
    case Binary(j, r) => (j, r)
    case _ => (Default(), Default())
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[P]]

  protected def saveDictionary[
    D <: Nat : ToInt
  ](
    dim: D,
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: Tuner
  )(implicit
    ev: LTEq[D, P]
  ): Context.U[(Position[_1], Long)] = {
    val numbered = coordinates(dim, tuner)
      .groupAll
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, (c, i)) => (Position(c), i.toLong) }

    numbered
      .map { case (Position(c), i) => c.toShortString + separator + i }
      .tunedSaveAsText(context, Redistribute(1), dictionary.format(file, Nat.toInt[D]))

    numbered
  }
}

/** Case class for methods that change the number of dimensions using a `TypedPipe[Cell[P]]`. */
case class MultiDimensionMatrix[
  P <: HList
](
  data: Context.U[Cell[P]]
) extends FwMultiDimensionMatrix[P, Context]
  with PairwiseDistance[P] {
  def join[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    that: Context.U[Cell[P]]
  )(implicit
    ev: FwMatrix.JoinTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    def msj[V] = Option(SetMapSideJoin[Position[slice.S], V]())

    val (t1, t2) = tuner match {
      case Binary(f, s) => (f, s)
      case t => (t, t)
    }

    val keep = data
      .map { case c => slice.selected(c.position) }
      .tunedDistinct(t1)
      .map { case p => (p, ()) }
      .tunedJoin(
        t1,
        that
          .map { case c => slice.selected(c.position) }
          .tunedDistinct(t1)
          .map { case p => (p, ()) },
        msj
      )
      .map { case (p, _) => (p, ()) }
      .forceToDisk // TODO: Should this be configurable?

    (data ++ that)
      .map { case c => (slice.selected(c.position), c) }
      .tunedJoin(t2, keep, msj)
      .map { case (_, (c, _)) => c }
  }

  def melt[
    D <: Nat : ToInt,
    I <: Nat : ToInt,
    V,
    W,
    X,
    Q <: HList
  ](
    dim: D,
    into: I,
    merge: (V, W) => X
  )(implicit
    ev1: LTEq[D, P],
    ev2: LTEq[I, P],
    ev3: IsDistinctConstraint[D :: I :: HNil],
    ev4: Pred.Aux[P, Q]
  ): Context.U[Cell[Q]] = data.map { case Cell(p, c) => Cell(p.melt(dim, into, merge), c) }

  def reshape[
    D <: Nat : ToInt,
    V,
    Q <: HList,
    T <: Tuner
  ](
    dim: D,
    coordinate: V,
    locate: Locate.FromCellAndOptionalValue[P, Q],
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: GT[Q, P],
    ev3: Pred[P],
    ev4: FwMatrix.ReshapeTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    implicit val pred = Pred[P]

    val msj = Option(MapMapSideJoin[Position[ev3.Out], Cell[P], Value]())

    val keys = data
      .collect[(Position[ev3.Out], Value)] { case c if (c.position(dim) equ coordinate) =>
        (c.position.remove(dim), c.content.value)
      }

    data
      .collect { case c if (c.position(dim) neq coordinate) => (c.position.remove(dim), c) }
      .tunedLeftJoin(tuner, keys, msj)
      .flatMap { case (_, (c, v)) => locate(c, v).map { case p => Cell(p, c.content) } }
  }

  def squash[
    D <: Nat : ToInt,
    Q <: HList,
    T <: Tuner
  ](
    dim: D,
    squasher: Squasher[P],
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: Pred.Aux[P, Q],
    ev3: FwMatrix.SquashTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    data
      .flatMap { case c => squasher.prepare(c, dim).map { case t => (c.position.remove(dim), t) } }
      .tunedReduce(tuner, (lt, rt) => squasher.reduce(lt, rt))
      .flatMap { case (p, t) => squasher.present(t).map { case c => Cell(p, c) } }
  }

  def squashWithValue[
    D <: Nat : ToInt,
    Q <: HList,
    W,
    T <: Tuner
  ](
    dim: D,
    value: Context.E[W],
    squasher: SquasherWithValue[P] { type V >: W },
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: Pred.Aux[P, Q],
    ev3: FwMatrix.SquashTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.flatMap { case v => squasher.prepareWithValue(c, dim, v).map { case t => (c.position.remove(dim), t) } }
      }
      .tunedReduce(tuner, (lt, rt) => squasher.reduce(lt, rt))
      .flatMapWithValue(value) { case ((p, t), vo) =>
        vo.flatMap { case v => squasher.presentWithValue(t, v).map { case c => Cell(p, c) } }
      }
  }
}

private object Util {
  def pivot[
    P <: HList
  ](
    data: Context.U[Cell[P]],
    slice: Slice[P],
    tuner: Tuner
  ): (
    Context.U[(Position[slice.S], List[(Position[slice.R], Option[Cell[P]])])],
    Context.E[List[Position[slice.R]]]
  ) = {
    def setSemigroup = new Semigroup[Set[Position[slice.R]]] {
      def plus(l: Set[Position[slice.R]], r: Set[Position[slice.R]]) = l ++ r
    }
    def mapSemigroup = new Semigroup[Map[Position[slice.R], Cell[P]]] {
      def plus(l: Map[Position[slice.R], Cell[P]], r: Map[Position[slice.R], Cell[P]]) = l ++ r
    }

    val columns = data
      .map { case c => Set(slice.remainder(c.position)) }
      .sum(setSemigroup)
      .map { case s => s.toList.sorted }

    val pivoted = data
      .map { case c => (slice.selected(c.position), Map(slice.remainder(c.position) -> c)) }
      .tuneReducers(tuner)
      .sum(mapSemigroup)
      .flatMapWithValue(columns) { case ((key, map), opt) =>
        opt.map { case cols => (key, cols.map { case c => (c, map.get(c)) }) }
      }

    (pivoted, columns)
  }
}

