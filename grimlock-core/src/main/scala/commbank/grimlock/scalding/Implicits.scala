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

package commbank.grimlock.scalding.environment.implicits

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.implicits.{
  CellImplicits => FwCellImplicits,
  ContentImplicits => FwContentImplicits,
  EnvironmentImplicits => FwEnvironmentImplicits,
  Implicits => FwImplicits,
  MatrixImplicits => FwMatrixImplicits,
  NativeOperations => FwNativeOperations,
  PartitionImplicits => FwPartitionImplicits,
  PositionImplicits => FwPositionImplicits,
  ValueOperations => FwValueOperations
}
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.Position

import commbank.grimlock.scalding.{
  Matrix,
  Matrix1D,
  Matrix2D,
  Matrix3D,
  Matrix4D,
  Matrix5D,
  Matrix6D,
  Matrix7D,
  Matrix8D,
  Matrix9D,
  MultiDimensionMatrix,
  SaveStringsAsText
}
import commbank.grimlock.scalding.content.{ Contents, IndexedContents }
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.partition.Partitions
import commbank.grimlock.scalding.position.Positions

import com.twitter.scalding.typed.IterablePipe

import scala.reflect.ClassTag

import shapeless.{ ::, =:!=, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8 }
import shapeless.ops.hlist.Length
import shapeless.ops.nat.GT

/** Implements all implicits. */
case class Implicits() extends FwImplicits[Context] {
  val cell = CellImplicits()
  val content = ContentImplicits()
  val environment = EnvironmentImplicits()
  val matrix = MatrixImplicits()
  val partition = PartitionImplicits()
  val position = PositionImplicits()
}

/** Implements all cell implicits. */
case class CellImplicits() extends FwCellImplicits[Context] {
  implicit def cellToU[
    P <: HList
  ](
    c: Cell[P]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = IterablePipe(List(c))

  implicit def listCellToU[
    P <: HList
  ](
    l: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = IterablePipe(l)
}

/** Implements all content implicits. */
case class ContentImplicits() extends FwContentImplicits[Context] {
  implicit def toContents(data: Context.U[Content]): Contents = Contents(data)

  implicit def toIndexed[
    P <: HList
  ](
    data: Context.U[(Position[P], Content)]
  ): IndexedContents[P] = IndexedContents(data)
}

/** Implements all environment implicits for `context`. */
case class EnvironmentImplicits() extends FwEnvironmentImplicits[Context]  {
  implicit def saveStringsAsText(data: Context.U[String]): SaveStringsAsText = SaveStringsAsText(data)

  implicit def nativeFunctions[X](data: Context.U[X]): NativeOperations[X] = NativeOperations(data)

  implicit def valueFunctions[X](value: Context.E[X]): ValueOperations[X] = ValueOperations(value)

  implicit def eToU[
    X : ClassTag
  ](
    value: Context.E[X]
  )(implicit
    ctx: Context
  ): Context.U[X] = value.toTypedPipe
}

/** Implements all matrix implicits. */
case class MatrixImplicits() extends FwMatrixImplicits[Context] {
  implicit def toMatrix[P <: HList](data: Context.U[Cell[P]]): Matrix[P] = Matrix(data)

  implicit def toMatrix1D[
    V1 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = Matrix1D(data)

  implicit def toMatrix2D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = Matrix2D(data)

  implicit def toMatrix3D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = Matrix3D(data)

  implicit def toMatrix4D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = Matrix4D(data)

  implicit def toMatrix5D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = Matrix5D(data)

  implicit def toMatrix6D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag,
    V3 <: Value[_] : ClassTag,
    V4 <: Value[_] : ClassTag,
    V5 <: Value[_] : ClassTag,
    V6 <: Value[_] : ClassTag
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = Matrix6D(data)

  implicit def toMatrix7D[
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
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = Matrix7D(data)

  implicit def toMatrix8D[
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
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = Matrix8D(data)

  implicit def toMatrix9D[
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
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _1, V2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _2, V3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _3, V4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _4, V5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _5, V6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _6, V7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _7, V8],
    ev9: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _8, V9]
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = Matrix9D(data)

  implicit def toMultiDimensionMatrix[
    P <: HList,
    L <: Nat
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ev1: Length.Aux[P, L],
    ev2: GT[L, _1]
  ): MultiDimensionMatrix[P] = MultiDimensionMatrix(data)

  implicit def listToMatrix[
    P <: HList
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = Matrix(IterablePipe(data))

  implicit def listToMatrix1D[
    V1 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = Matrix1D(IterablePipe(data))

  implicit def listToMatrix2D[
    V1 <: Value[_] : ClassTag,
    V2 <: Value[_] : ClassTag
  ](
    data: List[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = Matrix2D(IterablePipe(data))

  implicit def listToMatrix3D[
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
  ): Matrix3D[V1, V2, V3] = Matrix3D(IterablePipe(data))

  implicit def listToMatrix4D[
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
  ): Matrix4D[V1, V2, V3, V4] = Matrix4D(IterablePipe(data))

  implicit def listToMatrix5D[
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
  ): Matrix5D[V1, V2, V3, V4, V5] = Matrix5D(IterablePipe(data))

  implicit def listToMatrix6D[
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
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = Matrix6D(IterablePipe(data))

  implicit def listToMatrix7D[
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
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = Matrix7D(IterablePipe(data))

  implicit def listToMatrix8D[
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
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = Matrix8D(IterablePipe(data))

  implicit def listToMatrix9D[
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
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = Matrix9D(IterablePipe(data))

  implicit def listToMultiDimensionMatrix[
    P <: HList,
    L <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context,
    ev1: Length.Aux[P, L],
    ev2: GT[L, _1]
  ): MultiDimensionMatrix[P] = MultiDimensionMatrix(IterablePipe(data))

  implicit def tuple1ToMatrix[
    T1 <% V1,
    V1 <: Value[_]
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: HNil] = Matrix(IterablePipe(list.map { case (v, c) => Cell(Position(v), c) }))

  implicit def tuple1ToMatrix1D[
    T1 <% V1,
    V1 <: Value[_] : ClassTag
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = Matrix1D(IterablePipe(list.map { case (v, c) => Cell(Position(v), c) }))

  implicit def tuple2ToMatrix[
    T1 <% V1,
    T2 <% V2,
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): Matrix[V1 :: V2 :: HNil] = Matrix(IterablePipe(list.map { case (v1, v2, c) => Cell(Position(v1, v2), c) }))

  implicit def tuple2ToMatrix2D[
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
  ): Matrix2D[V1, V2] = Matrix2D(IterablePipe(list.map { case (v1, v2, c) => Cell(Position(v1, v2), c) }))

  implicit def tuple2ToMultiDimensionMatrix[
    T1 <% V1,
    T2 <% V2,
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[V1 :: V2 :: HNil] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, c) => Cell(Position(v1, v2), c) })
  )

  implicit def tuple3ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: HNil] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, c) => Cell(Position(v1, v2, v3), c) })
  )

  implicit def tuple3ToMatrix3D[
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
  ): Matrix3D[V1, V2, V3] = Matrix3D(IterablePipe(list.map { case (v1, v2, v3, c) => Cell(Position(v1, v2, v3), c) }))

  implicit def tuple3ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: HNil] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, c) => Cell(Position(v1, v2, v3), c) })
  )

  implicit def tuple4ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: V4 :: HNil] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, c) => Cell(Position(v1, v2, v3, v4), c) })
  )

  implicit def tuple4ToMatrix4D[
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
  ): Matrix4D[V1, V2, V3, V4] = Matrix4D(
    IterablePipe(list.map { case (v1, v2, v3, v4, c) => Cell(Position(v1, v2, v3, v4), c) })
  )

  implicit def tuple4ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: HNil] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, c) => Cell(Position(v1, v2, v3, v4), c) })
  )

  implicit def tuple5ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, c) => Cell(Position(v1, v2, v3, v4, v5), c) })
  )

  implicit def tuple5ToMatrix5D[
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
  ): Matrix5D[V1, V2, V3, V4, V5] = Matrix5D(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, c) => Cell(Position(v1, v2, v3, v4, v5), c) })
  )

  implicit def tuple5ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, c) => Cell(Position(v1, v2, v3, v4, v5), c) })
  )

  implicit def tuple6ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, c) => Cell(Position(v1, v2, v3, v4, v5, v6), c) })
  )

  implicit def tuple6ToMatrix6D[
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
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = Matrix6D(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, c) => Cell(Position(v1, v2, v3, v4, v5, v6), c) })
  )

  implicit def tuple6ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, c) => Cell(Position(v1, v2, v3, v4, v5, v6), c) })
  )

  implicit def tuple7ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, v7, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7), c) })
  )

  implicit def tuple7ToMatrix7D[
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
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = Matrix7D(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, v7, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7), c) })
  )

  implicit def tuple7ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, v7, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7), c) })
  )

  implicit def tuple8ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = Matrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8), c) }
    )
  )

  implicit def tuple8ToMatrix8D[
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
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = Matrix8D(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8), c) }
    )
  )

  implicit def tuple8ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = MultiDimensionMatrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8), c) }
    )
  )

  implicit def tuple9ToMatrix[
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
  ): Matrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = Matrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, v9, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8, v9), c) }
    )
  )

  implicit def tuple9ToMatrix9D[
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
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = Matrix9D(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, v9, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8, v9), c) }
    )
  )

  implicit def tuple9ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = MultiDimensionMatrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, v9, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8, v9), c) }
    )
  )
}

/** Implements all partition implicits. */
case class PartitionImplicits() extends FwPartitionImplicits[Context] {
  implicit def toPartitions[
    P <: HList,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  ): Partitions[P, I] = Partitions(data)
}

/** Implements all position implicits. */
case class PositionImplicits() extends FwPositionImplicits[Context] {
  implicit def valueToU[
    T <% V,
    V <: Value[_]
  ](
    t: T
  )(implicit
    ctx: Context
  ): Context.U[Position[V :: HNil]] = IterablePipe(List(Position(t)))

  implicit def listValueToU[
    T <% V,
    V <: Value[_]
  ](
    l: List[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[V :: HNil]] = IterablePipe(l.map { case t => Position(t) })

  implicit def positionToU[
    P <: HList
  ](
    p: Position[P]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = IterablePipe(List(p))

  implicit def listPositionToU[
    P <: HList
  ](
    l: List[Position[P]]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = IterablePipe(l)

  implicit def toPositions[P <: HList](data: Context.U[Position[P]]): Positions[P] = Positions(data)
}

/** Implements all native operations. */
case class NativeOperations[X](data: Context.U[X]) extends FwNativeOperations[X, Context.U] {
  def ++(other: Context.U[X]): Context.U[X] = data ++ other

  def filter(f: (X) => Boolean): Context.U[X] = data.filter(f)

  def flatMap[Y : ClassTag](f: (X) => TraversableOnce[Y]): Context.U[Y] = data.flatMap(f)

  def map[Y : ClassTag](f: (X) => Y): Context.U[Y] = data.map(f)
}

/** Implements all value operations. */
case class ValueOperations[X](value: Context.E[X]) extends FwValueOperations[X, Context.E] {
  def cross[Y](that: Context.E[Y])(implicit ev: Y =:!= Nothing): Context.E[(X, Y)] = value
    .leftCross(that)
    .map {
      case (x, Some(y)) => (x, y)
      case (x, None) => throw new Exception("Empty ValuePipe not supported")
    }

  def map[Y](f: (X) => Y): Context.E[Y] = value.map(f)
}

