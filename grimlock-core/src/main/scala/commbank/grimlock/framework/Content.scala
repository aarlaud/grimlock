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

package commbank.grimlock.framework.content

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.encoding.{ Codec, EncodeString, Value }
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.metadata.{ Schema, Type }
import commbank.grimlock.framework.position.Position

import java.util.Date
import java.util.regex.Pattern

import play.api.libs.json.{ JsError, JsObject, Json, JsResult, JsString, JsSuccess, JsValue, Reads }

import shapeless.{ ::, :+:, CNil, Coproduct, HList, HNil, Inl, Inr, LUBConstraint }
import shapeless.ops.coproduct.Inject
import shapeless.ops.hlist.{ Mapper, ToTraversable, Zip }

/**
 * Contents of a cell in a matrix.
 *
 * @param codec  Codec for encoding/decoding the value.
 * @param schema Schema (description) of the value.
 * @param value  The value of the variable.
 */
case class Content[D](codec: Codec[D], schema: Schema[D], value: D) {
  def asValue: Value[D] = codec.box(value)

  /**
   * Converts the content to a consise (terse) string.
   *
   * @param separator   The separator to use between the fields.
   * @param descriptive Indicator if codec and schema are required or not.
   *
   * @return Short string representation.
   */
  def toShortString(
    separator: String = "|",
    descriptive: Boolean = true
  ): String = (
    if (descriptive)
      codec.toShortString + separator + schema.toShortString(codec) + separator
    else
      ""
  ) + codec.encode(value)

  override def toString: String = "Content(" +
    codec.toString + "," +
    schema.toString + "," +
    codec.encode(value) +
  ")"

  /**
   * Converts the content to a JSON string.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON(pretty: Boolean = false, descriptive: Boolean = true): String = {
    val json = JsObject(
      (
        if (descriptive)
          Seq(
            "codec" -> JsString(codec.toShortString),
            "schema" -> JsString(schema.toShortString(codec))
          )
        else
          Seq.empty
      ) ++ Seq("value" -> JsString(codec.encode(value)))
    )

    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }
}

/** Companion object to `Content` trait. */
object Content {
  /** Default coproduct for contents. */
  type DefaultContents = Content[Boolean] :+:
    Content[Date] :+:
    Content[Double] :+:
    Content[Long] :+:
    Content[String] :+:
    Content[Type] :+:
    CNil

  /** Type for parsing a string to `Content`. */
  type Parser[T] = (String) => Option[T]

  /**
   * Return content parser from codec and schema.
   *
   * @param codec  The codec to decode content with.
   * @param schema The schema to validate content with.
   *
   * @return A content parser.
   */
  def parser[D](codec: Codec[D], schema: Schema[D]): Parser[Content[D]] = (str: String) => codec
    .decode(str)
    .flatMap { case value => if (schema.validate(codec, value)) Option(Content(codec, schema, value)) else None }

  /**
   * Return content parser from codec and schema strings.
   *
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   *
   * @return A content parser.
   */
  def parserFromComponents[
    C <: Coproduct
  ](
    codec: String,
    schema: String
  )(implicit
    ev1: Inject[C, Content[Boolean]],
    ev2: Inject[C, Content[Date]],
    ev3: Inject[C, Content[Double]],
    ev4: Inject[C, Content[Long]],
    ev5: Inject[C, Content[String]],
    ev6: Inject[C, Content[Type]]
  ): Option[Parser[C]] = {
    def toParser[D](codec: Codec[D], schema: Schema[D])(implicit ev: Inject[C, Content[D]]) = Option(
      (str: String) => codec.decode(str) match {
        case Some(value) if (schema.validate(codec, value)) => Option(Coproduct(Content(codec, schema, value)))
        case _ => None
      }
    )

    def parseSchemaToParser[D](codec: Codec[D], schema: String)(implicit ev: Inject[C, Content[D]]) = {
      Schema.fromShortString[Schema.DefaultSchemas[D], D](codec, schema) match {
        case Some(Inl(s)) => toParser(codec, s)
        case Some(Inr(Inl(s))) => toParser(codec, s)
        case Some(Inr(Inr(Inl(s)))) => toParser(codec, s)
        case Some(Inr(Inr(Inr(Inl(s))))) => toParser(codec, s)
        case Some(Inr(Inr(Inr(Inr(Inl(s)))))) => toParser(codec, s)
        case _ => None
      }
    }

    Codec.fromShortString[Codec.DefaultCodecs](codec) match {
      case Some(Inl(cdc)) => parseSchemaToParser(cdc, schema)
      case Some(Inr(Inl(cdc))) => parseSchemaToParser(cdc, schema)
      case Some(Inr(Inr(Inl(cdc)))) => parseSchemaToParser(cdc, schema)
      case Some(Inr(Inr(Inr(Inl(cdc))))) => parseSchemaToParser(cdc, schema)
      case Some(Inr(Inr(Inr(Inr(Inl(cdc)))))) => parseSchemaToParser(cdc, schema)
      case Some(Inr(Inr(Inr(Inr(Inr(Inl(cdc))))))) => parseSchemaToParser(cdc, schema)
      case _ => None
    }
  }


  /**
   * Parse a content from string components
   *
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   * @param value  The content string value to parse.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromComponents[
    C <: Coproduct
  ](
    codec: String,
    schema: String,
    value: String
  )(implicit
    ev1: Inject[C, Content[Boolean]],
    ev2: Inject[C, Content[Date]],
    ev3: Inject[C, Content[Double]],
    ev4: Inject[C, Content[Long]],
    ev5: Inject[C, Content[String]],
    ev6: Inject[C, Content[Type]]
  ): Option[C] = {
    def parseValue[D](codec: Codec[D], schema: Schema[D])(implicit ev: Inject[C, Content[D]]) = codec
      .decode(value)
      .map(v => Coproduct[C](Content(codec, schema, v)))

    def parseSchema[D](codec: Codec[D])(implicit ev: Inject[C, Content[D]]) = {
      Schema.fromShortString[Schema.DefaultSchemas[D], D](codec, schema) match {
        case Some(Inl(s)) => parseValue(codec, s)
        case Some(Inr(Inl(s))) => parseValue(codec, s)
        case Some(Inr(Inr(Inl(s)))) => parseValue(codec, s)
        case Some(Inr(Inr(Inr(Inl(s))))) => parseValue(codec, s)
        case Some(Inr(Inr(Inr(Inr(Inl(s)))))) => parseValue(codec, s)
        case _ => None
      }
    }

    Codec.fromShortString[Codec.DefaultCodecs](codec) match {
      case Some(Inl(cdc)) => parseSchema(cdc)
      case Some(Inr(Inl(cdc))) => parseSchema(cdc)
      case Some(Inr(Inr(Inl(cdc)))) => parseSchema(cdc)
      case Some(Inr(Inr(Inr(Inl(cdc))))) => parseSchema(cdc)
      case Some(Inr(Inr(Inr(Inr(Inl(cdc)))))) => parseSchema(cdc)
      case Some(Inr(Inr(Inr(Inr(Inr(Inl(cdc))))))) => parseSchema(cdc)
      case _ => None
    }
  }

  /**
   * Parse a content from string.
   *
   * @param str       The string to parse.
   * @param separator The separator between codec, schema and value.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromShortString[
    C <: Coproduct
  ](
    str: String,
    separator: String = "|"
  )(implicit
    ev1: Inject[C, Content[Boolean]],
    ev2: Inject[C, Content[Date]],
    ev3: Inject[C, Content[Double]],
    ev4: Inject[C, Content[Long]],
    ev5: Inject[C, Content[String]],
    ev6: Inject[C, Content[Type]]
  ): Option[C] = str.split(Pattern.quote(separator)) match {
    case Array(c, s, v) => fromComponents(c, s, v)
    case _ => None
  }

  /**
   * Return string representation of a content.
   *
   * @param verbose     Indicator if verbose string is required or not.
   * @param separator   The separator to use between various fields (only used if verbose is `false`).
   * @param descriptive Indicator if codec and schema are required or not (only used if verbose is `false`).
   */
  def toString[
    D
  ](
    verbose: Boolean = false,
    separator: String = "|",
    descriptive: Boolean = true
  ): (Content[D]) => TraversableOnce[String] = (t: Content[D]) =>
    List(if (verbose) t.toString else t.toShortString(separator, descriptive))

  /**
   * Return function that returns a JSON representation of a content.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON[
    D
  ](
    pretty: Boolean = false,
    descriptive: Boolean = true
  ): (Content[D]) => TraversableOnce[String] = (t: Content[D]) => List(t.toJSON(pretty, descriptive))

  /**
   * Return a `Reads` for parsing a JSON content.
   *
   * @param parser Optional parser; in case the JSON is not self describing.
   */
  def reads[
    C <: Coproduct
  ](
    parser: Option[Parser[C]]
  )(implicit
    ev1: Inject[C, Content[Boolean]],
    ev2: Inject[C, Content[Date]],
    ev3: Inject[C, Content[Double]],
    ev4: Inject[C, Content[Long]],
    ev5: Inject[C, Content[String]],
    ev6: Inject[C, Content[Type]]
  ): Reads[C] = new Reads[C] {
    def reads(json: JsValue): JsResult[C] = {
      val fields = json.as[JsObject].value

      if ((fields.size == 3 && parser.isEmpty) || (fields.size == 1 && parser.isDefined))
        (
          for {
            decoder <- parser.orElse(
              for {
                codec <- fields.get("codec").map(_.as[String])
                schema <- fields.get("schema").map(_.as[String])
                pfc <- Content.parserFromComponents(codec, schema)
              } yield pfc
            )
            value <- fields.get("value").map(_.as[String])
            content <- decoder(value)
          } yield JsSuccess(content)
        ).getOrElse(JsError("Unable to parse content"))
      else
        JsError("Incorrect number of fields")
    }
  }
}

/** Trait that represents the contents of a matrix. */
trait Contents[C <: Context[C]] extends Persist[Content[_], C] {
  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `Content` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[Content[_]]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[Content[_]], // = Content.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[Content[_]]
}

/** Trait that represents the output of uniqueByPosition. */
trait IndexedContents[P <: HList, C <: Context[C]] extends Persist[(Position[P], Content[_]), C] {
  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `IndexedContent` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[(Position[P], Content[_])]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[(Position[P], Content[_])], // = IndexedContents.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[(Position[P], Content[_])]
}

/** Object for `IndexedContents` functions. */
object IndexedContents {
  /**
   * Return string representation of an indexed content.
   *
   * @param verbose     Indicator if verbose string is required or not.
   * @param separator   The separator to use between various fields (only used if verbose is `false`).
   * @param descriptive Indicator if codec and schema are required or not (only used if verbose is `false`).
   */
  def toString[
    L <: HList,
    Z <: HList,
    M <: HList,
    P <: HList
  ](
    codecs: L,
    verbose: Boolean = false,
    separator: String = "|",
    descriptive: Boolean = true
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, Any]
  ): ((Position[P], Content[_])) => TraversableOnce[String] = (t: (Position[P], Content[_])) =>
    List(
      if (verbose)
        t.toString
      else
        t._1.toShortString(codecs, separator) + separator + t._2.toShortString(separator, descriptive)
    )

  /**
   * Return function that returns a JSON representations of an indexed content.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param separator   The separator to use between various both JSON strings.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   *
   * @note The index (Position) and content are separately encoded and then combined using the separator.
   */
  def toJSON[
    L <: HList,
    Z <: HList,
    M <: HList,
    P <: HList
  ](
    codecs: L,
    pretty: Boolean = false,
    separator: String = ",",
    descriptive: Boolean = false
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, String]
  ): ((Position[P], Content[_])) => TraversableOnce[String] = (t: (Position[P], Content[_])) =>
    List(t._1.toJSON(codecs, pretty) + separator + t._2.toJSON(pretty, descriptive))
}

