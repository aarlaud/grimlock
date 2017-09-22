// Copyright 2017 Commonwealth Bank of Australia
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

package commbank.grimlock.spark

import commbank.grimlock.framework.{ Cell, MatrixWithParseErrors }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distance.PairwiseDistance.{ CorrelationTuner, MutualInformationTuner }
import commbank.grimlock.framework.distribution.ApproximateDistribution.{
  CountMapQuantilesTuner,
  HistogramTuner,
  QuantilesTuner,
  TDigestQuantilesTuner,
  UniformQuantilesTuner
}
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{
  Binary,
  Default,
  InMemory,
  NoParameters,
  Redistribute,
  Reducers,
  Ternary
}
import commbank.grimlock.framework.Matrix.{
  ChangeTuner,
  CompactTuner,
  DomainTuner,
  FillHeterogeneousTuner,
  FillHomogeneousTuner,
  GetTuner,
  JoinTuner,
  PairwiseTuner,
  ReshapeTuner,
  SaveAsCSVTuner,
  SaveAsIVTuner,
  SaveAsVWTuner,
  ShapeTuner,
  SetTuner,
  SizeTuner,
  SliceTuner,
  SlideTuner,
  SquashTuner,
  SummariseTuner,
  TypesTuner,
  UniqueTuner,
  WhichTuner
}
import commbank.grimlock.framework.partition.Partitions.{ ForAllTuner, IdsTuner }
import commbank.grimlock.framework.Persist.SaveAsTextTuner
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.position.Positions.NamesTuner
import commbank.grimlock.framework.statistics.Statistics.{
  CountsTuner,
  DistinctCountsTuner,
  KurtosisTuner,
  MaximumAbsoluteTuner,
  MaximumTuner,
  MeanTuner,
  MinimumTuner,
  PredicateCountsTuner,
  StandardDeviationTuner,
  SkewnessTuner,
  SumsTuner
}

import commbank.grimlock.spark.content.{ Contents, IndexedContents }
import commbank.grimlock.spark.partition.Partitions
import commbank.grimlock.spark.position.Positions

import scala.reflect.ClassTag

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8 }
import shapeless.ops.hlist.Length
import shapeless.ops.nat.GT

package object environment {
  // *** Matrix/Position shared tuners

  /** Implicit for checking tuners on a call to `names`. */
  implicit def rddNamesTunerDn = new NamesTuner[Context.U, Default[NoParameters]] { }
  implicit def rddNamesTunerDr = new NamesTuner[Context.U, Default[Reducers]] { }

  // *** Persist tuners

  /** Implicit for checking tuners on a call to `saveAstext`. */
  implicit def rddSaveAsTextTunerDn = new SaveAsTextTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSaveAsTextTunerRr = new SaveAsTextTuner[Context.U, Redistribute] { }

  // *** Pairwise distance tuners

  /** Implicit for checking tuners on a call to `corrrelation`. */
  implicit def rddCorrelationTunerDnInDn = new CorrelationTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddCorrelationTunerDnDnDn = new CorrelationTuner[Context.U, Default[NoParameters]] { }
  implicit def rddCorrelationTunerDrInDn = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddCorrelationTunerDrInDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddCorrelationTunerDrIrDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]
  ] { }
  implicit def rddCorrelationTunerDrDnDn = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddCorrelationTunerDrDnDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddCorrelationTunerDrDrDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }

  /** Implicit for checking tuners on a call to `mutualInformation`. */
  implicit def rddMutualInformationTunerDnInDn = new MutualInformationTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddMutualInformationTunerDnDnDn = new MutualInformationTuner[Context.U, Default[NoParameters]] { }
  implicit def rddMutualInformationTunerDrInDn = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddMutualInformationTunerDrInDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddMutualInformationTunerDrIrDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]
  ] { }
  implicit def rddMutualInformationTunerDrDnDn = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddMutualInformationTunerDrDnDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddMutualInformationTunerDrDrDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }

  // *** Distribution tuners

  /** Implicit for checking tuners on a call to `histogram`. */
  implicit def rddHistogramTunerDn = new HistogramTuner[Context.U, Default[NoParameters]] { }
  implicit def rddHistogramTunerDr = new HistogramTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `quantiles`. */
  implicit def rddQuantilesTunerIn = new QuantilesTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddQuantilesTunerIr = new QuantilesTuner[Context.U, InMemory[Reducers]] { }
  implicit def rddQuantilesTunerDn = new QuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def rddQuantilesTunerDr = new QuantilesTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `countMapQuantiles`. */
  implicit def rddCountMapQuantilesTunerDn = new CountMapQuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def rddCountMapQuantilesTunerDr = new CountMapQuantilesTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `tDigestQuantiles`. */
  implicit def rddTDigestQuantilesTunerDn = new TDigestQuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def rddTDigestQuantilesTunerDr = new TDigestQuantilesTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `uniformQuantiles`. */
  implicit def rddUniformQuantilesTunerDn = new UniformQuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def rddUniformQuantilesTunerDr = new UniformQuantilesTuner[Context.U, Default[Reducers]] { }

  // *** Partition tuners

  /** Implicit for checking tuners on a call to `forAll`. */
  implicit def rddForAllTunerDn = new ForAllTuner[Context.U, Default[NoParameters]] { }
  implicit def rddForAllTunerDr = new ForAllTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `ids`. */
  implicit def rddIdsTunerDn = new IdsTuner[Context.U, Default[NoParameters]] { }
  implicit def rddIdsTunerDr = new IdsTuner[Context.U, Default[Reducers]] { }

  // *** Statistics tuners

  /** Implicit for checking tuners on a call to `counts`. */
  implicit def rddCountsTunerDn = new CountsTuner[Context.U, Default[NoParameters]] { }
  implicit def rddCountsTunerDr = new CountsTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `distinctCounts`. */
  implicit def rddDistinctCountsTunerDn = new DistinctCountsTuner[Context.U, Default[NoParameters]] { }
  implicit def rddDistinctCountsTunerDr = new DistinctCountsTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `predicateCounts`. */
  implicit def rddPredicateCountsTunerDn = new PredicateCountsTuner[Context.U, Default[NoParameters]] { }
  implicit def rddPredicateCountsTunerDr = new PredicateCountsTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `mean`. */
  implicit def rddMeanTunerDn = new MeanTuner[Context.U, Default[NoParameters]] { }
  implicit def rddMeanTunerDr = new MeanTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `standardDeviation`. */
  implicit def rddStandardDeviationTunerDn = new StandardDeviationTuner[Context.U, Default[NoParameters]] { }
  implicit def rddStandardDeviationTunerDr = new StandardDeviationTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `skewness`. */
  implicit def rddSkewnessTunerDn = new SkewnessTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSkewnessTunerDr = new SkewnessTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `kurtosis`. */
  implicit def rddKurtosisTunerDn = new KurtosisTuner[Context.U, Default[NoParameters]] { }
  implicit def rddKurtosisTunerDr = new KurtosisTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `minimum`. */
  implicit def rddMinimumTunerDn = new MinimumTuner[Context.U, Default[NoParameters]] { }
  implicit def rddMinimumTunerDr = new MinimumTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `maximum`. */
  implicit def rddMaximumTunerDn = new MaximumTuner[Context.U, Default[NoParameters]] { }
  implicit def rddMaximumTunerDr = new MaximumTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `maximumAbsolute`. */
  implicit def rddMaximumAbsoluteTunerDn = new MaximumAbsoluteTuner[Context.U, Default[NoParameters]] { }
  implicit def rddMaximumAbsoluteTunerDr = new MaximumAbsoluteTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `sums`. */
  implicit def rddSumsTunerDn = new SumsTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSumsTunerDr = new SumsTuner[Context.U, Default[Reducers]] { }

  // *** Matrix tuners

  /** Implicit for checking tuners on a call to `change`. */
  implicit def rddChangeTunerIn = new ChangeTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddChangeTunerDn = new ChangeTuner[Context.U, Default[NoParameters]] { }
  implicit def rddChangeTunerDr = new ChangeTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `compact`. */
  implicit def rddCompactTunerDn = new CompactTuner[Context.U, Default[NoParameters]] { }
  implicit def rddCompactTunerDr = new CompactTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `domain`. */
  implicit def rddDomainTunerDn = new DomainTuner[Context.U, Default[NoParameters]] { }
  implicit def rddDomainTunerDr = new DomainTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `fillHeterogeneous`. */
  implicit def rddFillHeterogeneousTunerDnDnDn = new FillHeterogeneousTuner[Context.U, Default[NoParameters]] { }
  implicit def rddFillHeterogeneousTunerInInDn = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddFillHeterogeneousTunerInDnDn = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddFillHeterogeneousTunerInInDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddFillHeterogeneousTunerInDnDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddFillHeterogeneousTunerInDrDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def rddFillHeterogeneousTunerDnDnDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[Default[NoParameters], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddFillHeterogeneousTunerDnDrDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[Default[NoParameters], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def rddFillHeterogeneousTunerDrDrDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }

  /** Implicit for checking tuners on a call to `fillHomogeneous`. */
  implicit def rddFillHomogeneousTunerDnDn = new FillHomogeneousTuner[Context.U, Default[NoParameters]] { }
  implicit def rddFillHomogeneousTunerInDn = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddFillHomogeneousTunerInDr = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddFillHomogeneousTunerIrDn = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[Reducers], Default[NoParameters]]
  ] { }
  implicit def rddFillHomogeneousTunerIrDr = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[Reducers], Default[Reducers]]
  ] { }
  implicit def rddFillHomogeneousTunerDnDr = new FillHomogeneousTuner[
    Context.U,
    Binary[Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddFillHomogeneousTunerDrDn = new FillHomogeneousTuner[
    Context.U,
    Binary[Default[Reducers], Default[NoParameters]]
  ] { }
  implicit def rddFillHomogeneousTunerDrDr = new FillHomogeneousTuner[
    Context.U,
    Binary[Default[Reducers], Default[Reducers]]
  ] { }

  /** Implicit for checking tuners on a call to `get`. */
  implicit def rddGetTunerIn = new GetTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddGetTunerDn = new GetTuner[Context.U, Default[NoParameters]] { }
  implicit def rddGetTunerDr = new GetTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `join`. */
  implicit def rddJoinTunerInIn = new JoinTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddJoinTunerDnDn = new JoinTuner[Context.U, Default[NoParameters]] { }
  implicit def rddJoinTunerInDn = new JoinTuner[Context.U, Binary[InMemory[NoParameters], Default[NoParameters]]] { }
  implicit def rddJoinTunerInDr = new JoinTuner[Context.U, Binary[InMemory[NoParameters], Default[Reducers]]] { }
  implicit def rddJoinTunerIrDr = new JoinTuner[Context.U, Binary[InMemory[Reducers], Default[Reducers]]] { }
  implicit def rddJoinTunerDnDr = new JoinTuner[Context.U, Binary[Default[NoParameters], Default[Reducers]]] { }
  implicit def rddJoinTunerDrDr = new JoinTuner[Context.U, Binary[Default[Reducers], Default[Reducers]]] { }

  /** Implicit for checking tuners on a call to `pairwise*`. */
  implicit def rddPairwiseTunerIn = new PairwiseTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddPairwiseTunerDnDnDn = new PairwiseTuner[Context.U, Default[NoParameters]] { }
  implicit def rddPairwiseTunerInDrDr = new PairwiseTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def rddPairwiseTunerDrDrDr = new PairwiseTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }

  /** Implicit for checking tuners on a call to `saveAsIV`. */
  implicit def rddSaveAsIVTunerDnDn = new SaveAsIVTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSaveAsIVTunerInDn = new SaveAsIVTuner[
    Context.U,
    Binary[InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddSaveAsIVTunerInRr = new SaveAsIVTuner[Context.U, Binary[InMemory[NoParameters], Redistribute]] { }
  implicit def rddSaveAsIVTunerDnRr = new SaveAsIVTuner[Context.U, Binary[Default[NoParameters], Redistribute]] { }
  implicit def rddSaveAsIVTunerDrDn = new SaveAsIVTuner[Context.U, Binary[Default[Reducers], Default[NoParameters]]] { }
  implicit def rddSaveAsIVTunerDrRr = new SaveAsIVTuner[Context.U, Binary[Default[Reducers], Redistribute]] { }

  /** Implicit for checking tuners on a call to `set`. */
  implicit def rddSetTunerDn = new SetTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSetTunerDr = new SetTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `shape`. */
  implicit def rddShapeTunerDn = new ShapeTuner[Context.U, Default[NoParameters]] { }
  implicit def rddShapeTunerDr = new ShapeTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `size`. */
  implicit def rddSizeTunerDn = new SizeTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSizeTunerDr = new SizeTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `slice`. */
  implicit def rddSliceTunerIn = new SliceTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddSliceTunerDn = new SliceTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSliceTunerDr = new SliceTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `slide`. */
  implicit def rddSlideTunerDn = new SlideTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSlideTunerDr = new SlideTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `summmarise`. */
  implicit def rddSummariseTunerDn = new SummariseTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSummariseTunerDr = new SummariseTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `types`. */
  implicit def rddTypesTunerDn = new TypesTuner[Context.U, Default[NoParameters]] { }
  implicit def rddTypesTunerDr = new TypesTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `unique`. */
  implicit def rddUniqueTunerDn = new UniqueTuner[Context.U, Default[NoParameters]] { }
  implicit def rddUniqueTunerDr = new UniqueTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `which`. */
  implicit def rddWhichTunerIn = new WhichTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddWhichTunerDn = new WhichTuner[Context.U, Default[NoParameters]] { }
  implicit def rddWhichTunerDr = new WhichTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `squash`. */
  implicit def rddSquashTunerDn = new SquashTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSquashTunerDr = new SquashTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `reshape`. */
  implicit def rddReshapeTunerIn = new ReshapeTuner[Context.U, InMemory[NoParameters]] { }
  implicit def rddReshapeTunerDn = new ReshapeTuner[Context.U, Default[NoParameters]] { }
  implicit def rddReshapeTunerDr = new ReshapeTuner[Context.U, Default[Reducers]] { }

  /** Implicit for checking tuners on a call to `saveAsCSV`. */
  implicit def rddSaveAsCSVTunerDnDn = new SaveAsCSVTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSaveAsCSVTunerDrDn = new SaveAsCSVTuner[Context.U, Default[Reducers]] { }
  implicit def rddSaveAsCSVTunerDnRr = new SaveAsCSVTuner[Context.U, Redistribute] { }
  implicit def rddSaveAsCSVTunerDrRr = new SaveAsCSVTuner[Context.U, Binary[Default[Reducers], Redistribute]] { }

  /** Implicit for checking tuners on a call to `saveAsVW`. */
  implicit def rddSaveAsVWTunerDnDnDn = new SaveAsVWTuner[Context.U, Default[NoParameters]] { }
  implicit def rddSaveAsVWTunerDrDrDn = new SaveAsVWTuner[Context.U, Default[Reducers]] { }
  implicit def rddSaveAsVWTunerDnDnRr = new SaveAsVWTuner[Context.U, Binary[Default[NoParameters], Redistribute]] { }
  implicit def rddSaveAsVWTunerDrDrRr = new SaveAsVWTuner[Context.U, Binary[Default[Reducers], Redistribute]] { }
  implicit def rddSaveAsVWTunerDnInDn = new SaveAsVWTuner[
    Context.U,
    Binary[Default[NoParameters], InMemory[NoParameters]]
  ] { }
  implicit def rddSaveAsVWTunerDnDrDn = new SaveAsVWTuner[
    Context.U,
    Binary[Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def rddSaveAsVWTunerDnInRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[NoParameters], InMemory[NoParameters], Redistribute]
  ] { }
  implicit def rddSaveAsVWTunerDnDrRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[NoParameters], Default[Reducers], Redistribute]
  ] { }
  implicit def rddSaveAsVWTunerDrInDn = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def rddSaveAsVWTunerDrInRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Redistribute]
  ] { }

  /** Converts a `Cell[P]` into a `RDD[Cell[P]]`. */
  implicit def cellToRDD[
    P <: HList
  ](
    c: Cell[P]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.cellToU(c)

  /** Converts a `List[Cell[P]]` into a `RDD[Cell[P]]`. */
  implicit def listCellToRDD[
    P <: HList
  ](
    l: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.listCellToU(l)

  /** Converts a `RDD[Content]` to a `Contents`. */
  implicit def rddToContents(
    data: Context.U[Content]
  )(implicit
    ctx: Context
  ): Contents = ctx.implicits.content.toContents(data)

  /** Converts a `RDD[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def rddToIndexed[
    P <: HList
  ](
    data: Context.U[(Position[P], Content)]
  )(implicit
    ctx: Context
  ): IndexedContents[P] = ctx.implicits.content.toIndexed(data)

  /** Conversion from `RDD[Cell[P]]` to a `Matrix`. */
  implicit def rddToMatrix[
    P <: HList
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = ctx.implicits.matrix.toMatrix(data)

  /** Conversion from `RDD[Cell[V1 :: HNil]]` to a `Matrix1D`. */
  implicit def rddToMatrix1D[
    V1 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = ctx.implicits.matrix.toMatrix1D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: HNil]]` to a `Matrix2D`. */
  implicit def rddToMatrix2D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = ctx.implicits.matrix.toMatrix2D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: HNil]]` to a `Matrix3D`. */
  implicit def rddToMatrix3D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = ctx.implicits.matrix.toMatrix3D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]` to a `Matrix4D`. */
  implicit def rddToMatrix4D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = ctx.implicits.matrix.toMatrix4D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]` to a `Matrix5D`. */
  implicit def rddToMatrix5D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = ctx.implicits.matrix.toMatrix5D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]` to a `Matrix6D`. */
  implicit def rddToMatrix6D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = ctx.implicits.matrix.toMatrix6D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]` to a `Matrix7D`. */
  implicit def rddToMatrix7D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = ctx.implicits.matrix.toMatrix7D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]` to a `Matrix8D`. */
  implicit def rddToMatrix8D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag,
    V8 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = ctx.implicits.matrix.toMatrix8D(data)

  /** Conversion from `RDD[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]` to a `Matrix9D`. */
  implicit def rddToMatrix9D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag,
    V8 <: Value[_] : ClassTag,
    V9 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _7, V8],
    ev9: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _8, V9]
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = ctx.implicits.matrix.toMatrix9D(data)

  /** Conversion from `RDD[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def rddToMultiDimensionMatrix[
    P <: HList,
    L <: Nat
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ctx: Context,
    ev1: Length.Aux[P, L],
    ev2: GT[L, _1]
  ): MultiDimensionMatrix[P] = ctx.implicits.matrix.toMultiDimensionMatrix(data)

  /** Conversion from `List[Cell[P]]` to a `Matrix`. */
  implicit def listToRDDMatrix[
    P <: HList
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = ctx.implicits.matrix.listToMatrix(data)

  /** Conversion from `List[Cell[V1 :: HNil]]` to a `Matrix1D`. */
  implicit def listToRDDMatrix1D[
    V1 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = ctx.implicits.matrix.listToMatrix1D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: HNil]]` to a `Matrix2D`. */
  implicit def listToRDDMatrix2D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = ctx.implicits.matrix.listToMatrix2D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: HNil]]` to a `Matrix3D`. */
  implicit def listToRDDMatrix3D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = ctx.implicits.matrix.listToMatrix3D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]` to a `Matrix4D`. */
  implicit def listToRDDMatrix4D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = ctx.implicits.matrix.listToMatrix4D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]` to a `Matrix5D`. */
  implicit def listToRDDMatrix5D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = ctx.implicits.matrix.listToMatrix5D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]` to a `Matrix6D`. */
  implicit def listToRDDMatrix6D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = ctx.implicits.matrix.listToMatrix6D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]` to a `Matrix7D`. */
  implicit def listToRDDMatrix7D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = ctx.implicits.matrix.listToMatrix7D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]` to a `Matrix8D`. */
  implicit def listToRDDMatrix8D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag,
    V8 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = ctx.implicits.matrix.listToMatrix8D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]` to a `Matrix9D`. */
  implicit def listToRDDMatrix9D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag,
    V8 <: Value[_] : ClassTag,
    V9 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _7, V8],
    ev9: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _8, V9]
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = ctx.implicits.matrix.listToMatrix9D(data)

  /** Conversion from `List[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def listToRDDMultiDimensionMatrix[
    P <: HList,
    L <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context,
    ev1: Length.Aux[P, L],
    ev2: GT[L, _1]
  ): MultiDimensionMatrix[P] = ctx.implicits.matrix.listToMultiDimensionMatrix(data)

  /** Conversion from `List[(T1, Content)]` to a `Matrix`. */
  implicit def tuple1ToRDDMatrix[
    T1 <% V1,
    V1 <: Value[_]
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: HNil] = ctx.implicits.matrix.tuple1ToMatrix(list)

  /** Conversion from `List[(T1, Content)]` to a `Matrix1D`. */
  implicit def tuple1ToRDDMatrix1D[
    T1 <% V1,
    V1 <: Value[_] : ClassTag
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = ctx.implicits.matrix.tuple1ToMatrix1D(list)

  /** Conversion from `List[(T1, T2, Content)]` to a `Matrix`. */
  implicit def tuple2ToRDDMatrix[
    T1 <% V1,
    T2 <% V2,
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: HNil] = ctx.implicits.matrix.tuple2ToMatrix(list)

  /** Conversion from `List[(T1, T2, Content)]` to a `Matrix2D`. */
  implicit def tuple2ToRDDMatrix2D[
    T1 <% V1,
    T2 <% V2,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = ctx.implicits.matrix.tuple2ToMatrix2D(list)

  /** Conversion from `List[(T1, T2, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple2ToRDDMultiDimensionMatrix[
    T1 <% V1,
    T2 <% V2,
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: HNil] = ctx.implicits.matrix.tuple2ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, Content)]` to a `Matrix`. */
  implicit def tuple3ToRDDMatrix[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: HNil] = ctx.implicits.matrix.tuple3ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, Content)]` to a `Matrix3D`. */
  implicit def tuple3ToRDDMatrix3D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = ctx.implicits.matrix.tuple3ToMatrix3D(list)

  /** Conversion from `List[(T1, T2, T3, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple3ToRDDMultiDimensionMatrix[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: HNil] = ctx.implicits.matrix.tuple3ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, Content)]` to a `Matrix`. */
  implicit def tuple4ToRDDMatrix[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: V4 :: HNil] = ctx.implicits.matrix.tuple4ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, Content)]` to a `Matrix4D`. */
  implicit def tuple4ToRDDMatrix4D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = ctx.implicits.matrix.tuple4ToMatrix4D(list)

  /** Conversion from `List[(T1, T2, T3, T4, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple4ToRDDMultiDimensionMatrix[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: HNil] = ctx.implicits.matrix.tuple4ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, Content)]` to a `Matrix`. */
  implicit def tuple5ToRDDMatrix[
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
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = ctx.implicits.matrix.tuple5ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, Content)]` to a `Matrix5D`. */
  implicit def tuple5ToRDDMatrix5D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = ctx.implicits.matrix.tuple5ToMatrix5D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple5ToRDDMultiDimensionMatrix[
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
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = ctx.implicits.matrix.tuple5ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, Content)]` to a `Matrix`. */
  implicit def tuple6ToRDDMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = ctx.implicits.matrix.tuple6ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, Content)]` to a `Matrix6D`. */
  implicit def tuple6ToRDDMatrix6D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, T4, T5, T6, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = ctx.implicits.matrix.tuple6ToMatrix6D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple6ToRDDMultiDimensionMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = ctx
    .implicits
    .matrix
    .tuple6ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, Content)]` to a `Matrix`. */
  implicit def tuple7ToRDDMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, T7, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = ctx.implicits.matrix.tuple7ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, Content)]` to a `Matrix7D`. */
  implicit def tuple7ToRDDMatrix7D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    T7 <% V7,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = ctx.implicits.matrix.tuple7ToMatrix7D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple7ToRDDMultiDimensionMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, T7, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = ctx
    .implicits
    .matrix
    .tuple7ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]` to a `Matrix`. */
  implicit def tuple8ToRDDMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = ctx.implicits.matrix.tuple8ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]` to a `Matrix8D`. */
  implicit def tuple8ToRDDMatrix8D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    T7 <% V7,
    T8 <% V8,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag,
    V8 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = ctx.implicits.matrix.tuple8ToMatrix8D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple8ToRDDMultiDimensionMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = ctx
    .implicits
    .matrix
    .tuple8ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]` to a `Matrix`. */
  implicit def tuple9ToRDDMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = ctx.implicits.matrix.tuple9ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]` to a `Matrix9D`. */
  implicit def tuple9ToRDDMatrix9D[
    T1 <% V1,
    T2 <% V2,
    T3 <% V3,
    T4 <% V4,
    T5 <% V5,
    T6 <% V6,
    T7 <% V7,
    T8 <% V8,
    T9 <% V9,
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag,
    V7 <: Value[_] : ClassTag,
    V8 <: Value[_] : ClassTag,
    V9 <: Value[_] : ClassTag
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]
  )(implicit
    ctx: Context
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = ctx.implicits.matrix.tuple9ToMatrix9D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple9ToRDDMultiDimensionMatrix[
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
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = ctx
    .implicits
    .matrix
    .tuple9ToMultiDimensionMatrix(list)

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToRDDParseErrors[
    P <: HList
  ](
    t: (Context.U[Cell[P]], Context.U[String])
  )(implicit
    ctx: Context
  ): MatrixWithParseErrors[P, Context.U] = ctx.implicits.matrix.tupleToParseErrors(t)

  /** Conversion from `RDD[(I, Cell[P])]` to a `Partitions`. */
  implicit def rddToPartitions[
    P <: HList,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  )(implicit
    ctx: Context
  ): Partitions[P, I] = ctx.implicits.partition.toPartitions(data)

  /** Converts a `T` to a `RDD[Position[V :: HNil]]`. */
  implicit def valueToRDD[
    T <% V,
    V <: Value[_]
  ](
    t: T
  )(implicit
    ctx: Context
  ): Context.U[Position[V :: HNil]] = ctx.implicits.position.valueToU(t)

  /** Converts a `List[T]` to a `RDD[Position[V :: HNil]]`. */
  implicit def listValueToRDD[
    T <% V,
    V <: Value[_]
  ](
    l: List[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[V :: HNil]] = ctx.implicits.position.listValueToU(l)

  /** Converts a `Position[P]` to a `RDD[Position[P]]`. */
  implicit def positionToRDD[
    P <: HList
  ](
    p: Position[P]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = ctx.implicits.position.positionToU(p)

  /** Converts a `List[Position[P]]` to a `RDD[Position[P]]`. */
  implicit def listPositionToRDD[
    P <: HList
  ](
    l: List[Position[P]]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = ctx.implicits.position.listPositionToU(l)

  /** Converts a `RDD[Position[P]]` to a `Positions`. */
  implicit def rddToPositions[
    P <: HList
  ](
    data: Context.U[Position[P]]
  )(implicit
    ctx: Context
  ): Positions[P] = ctx.implicits.position.toPositions(data)

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(RDD[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToRDDList[
    P <: HList,
    S <: HList,
    T <% Context.U[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.predicateToU(t)

  /** Converts a `List[(T, Cell.Predicate[P])]` to a `List[(RDD[Position[S]], Cell.Predicate[P])]`. */
  implicit def listPredicateToScaldingList[
    P <: HList,
    S <: HList,
    T <% Context.U[Position[S]]
  ](
    l: List[(T, Cell.Predicate[P])]
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.listPredicateToU(l)

  /** Converts a `RDD[String]` to a `SaveStringsAsText`. */
  implicit def savePipeStringsAsText(
    data: Context.U[String]
  )(implicit
    ctx: Context
  ): SaveStringsAsText = ctx.implicits.environment.saveStringsAsText(data)
}

