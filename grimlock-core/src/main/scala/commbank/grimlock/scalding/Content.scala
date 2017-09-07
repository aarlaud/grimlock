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

package commbank.grimlock.scalding.content

import commbank.grimlock.framework.{ Persist => FwPersist }
import commbank.grimlock.framework.content.{
  Contents => FwContents,
  IndexedContents => FwIndexedContents,
  Content
}
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.position.Position

import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.Persist

import shapeless.HList

/** Rich wrapper around a `TypedPipe[Content[_]]`. */
case class Contents(data: Context.U[Content[_]]) extends FwContents[Context] with Persist[Content[_]] {
  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[Content[_]],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[Content[_]] = saveText(context, file, writer, tuner)
}

/** Rich wrapper around a `TypedPipe[(Position[P], Content[_]]`. */
case class IndexedContents[
  P <: HList
](
  data: Context.U[(Position[P], Content[_])]
) extends FwIndexedContents[P, Context]
  with Persist[(Position[P], Content[_])] {
  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[(Position[P], Content[_])],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[(Position[P], Content[_])] = saveText(context, file, writer, tuner)
}

