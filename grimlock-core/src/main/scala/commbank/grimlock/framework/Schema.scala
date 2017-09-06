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

package commbank.grimlock.framework.metadata

import commbank.grimlock.framework.encoding.Codec

import java.util.Date

import shapeless.{ :+:, CNil, Coproduct }
import shapeless.ops.coproduct.Inject

/** Trait for variable schemas. */
trait Schema[D] {
  /** The type of variable. */
  val classification: Type

  /**
   * Validates if a value confirms to this schema.
   *
   * @param codec The codec to use for comparing values.
   * @param value The value to validate.
   *
   * @return True is the value confirms to this schema, false otherwise.
   */
  def validate(codec: Codec[D], value: D): Boolean

  /**
   * Return a consise (terse) string representation of a schema.
   *
   * @param codec The codec used to encode this schema's data.
   */
  def toShortString(
    codec: Codec[D]
  ): String = classification.toShortString + round(paramString(true, d => codec.encode(d)))

  override def toString: String = getClass.getSimpleName + "(" + paramString(false, d => d.toString) + ")"

  protected def paramString(short: Boolean, f: (D) => String): String = ""

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
}

/** Companion object to the `Schema` trait. */
object Schema {
  /** Default coproduct for schemas. */
  type DefaultSchemas[D] = ContinuousSchema[D] :+:
    DateSchema[D] :+:
    DiscreteSchema[D] :+:
    NominalSchema[D] :+:
    OrdinalSchema[D] :+:
    CNil

  /**
   * Parse a schema from a string.
   *
   * @param codec Codec with which to decode the data.
   * @param str   String from which to parse the schema.
   *
   * @return A `Some[C]` in case of success, `None` otherwise.
   */
  def fromShortString[ 
    C <: Coproduct,
    D
  ](
    codec: Codec[D], 
    str: String
  )(implicit
    ev1: Inject[C, ContinuousSchema[D]],
    ev2: Inject[C, DateSchema[D]],
    ev3: Inject[C, DiscreteSchema[D]],
    ev5: Inject[C, NominalSchema[D]],
    ev6: Inject[C, OrdinalSchema[D]]
  ): Option[C] = str match {
    case ContinuousSchema.Pattern(_) => ContinuousSchema.fromShortString(codec, str).map(Coproduct(_))
    case DateSchema.Pattern(_) => DateSchema.fromShortString(codec, str).map(Coproduct(_))
    case DiscreteSchema.PatternStep(_, _) => DiscreteSchema.fromShortString(codec, str).map(Coproduct(_))
    case DiscreteSchema.Pattern(_) => DiscreteSchema.fromShortString(codec, str).map(Coproduct(_))
    case NominalSchema.Pattern(_) => NominalSchema.fromShortString(codec, str).map(Coproduct(_))
    case OrdinalSchema.Pattern(_) => OrdinalSchema.fromShortString(codec, str).map(Coproduct(_))
    case _ => None
  }
}

/** Trait for schemas for numerical variables. */
trait NumericalSchema[D] extends Schema[D] {
  /* Optional range of variable */
  val range: Option[(D, D)]

  protected def validateRange(value: D)(implicit ev: Numeric[D]): Boolean = {
    import ev._

    // TODO: use codec?
    range.map { case (lower, upper) => value >= lower && value <= upper }.getOrElse(true)
  }
}

/**
 * Schema for continuous variables.
 *
 * @param range The optional range of the variable.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class ContinuousSchema[D : Numeric] private (range: Option[(D, D)]) extends NumericalSchema[D] {
  val classification = ContinuousType

  def validate(codec: Codec[D], value: D): Boolean = validateRange(value)

  override protected def paramString(
    short: Boolean,
    f: (D) => String
  ): String = SchemaParameters.writeRange(short, range, f)
}

/** Companion object to `ContinuousSchema` case class. */
object ContinuousSchema {
  /** Pattern for matching short string continuous schema. */
  val Pattern = (ContinuousType.name + """(?:\((?:(-?\d+\.?\d*:-?\d+\.?\d*))?\))?""").r

  /** Construct a continuous schema with unbounded range. */
  def apply[D : Numeric](): ContinuousSchema[D] = ContinuousSchema(None)

  /**
   * Construct a continuous schema with bounded range.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[D : Numeric](lower: D, upper: D): ContinuousSchema[D] = ContinuousSchema(Option((lower, upper)))

  /**
   * Parse a continuous schema from string.
   *
   * @param cdc The codec to parse with.
   * @param str The string to parse.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromShortString[D](cdc: Codec[D], str: String): Option[ContinuousSchema[D]] = (cdc.numeric, str) match {
    case (Some(num), Pattern(null)) => Option(ContinuousSchema()(num))
    case (Some(num), Pattern(range)) => SchemaParameters.splitRange(range)
      .flatMap { case (min, max) => fromComponents(cdc, min, max) }
    case _ => None
  }

  /**
   * Parse a continuous schema from components.
   *
   * @param cdc The codec to parse with.
   * @param min The minimum value string to parse.
   * @param max The maximum value string to parse.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], min: String, max: String): Option[ContinuousSchema[D]] = for {
    low <- SchemaParameters.parse(cdc, min)
    upp <- SchemaParameters.parse(cdc, max)
    num <- cdc.numeric
  } yield ContinuousSchema(low, upp)(num)
}

/**
 * Schema for discrete variables.
 *
 * @param range The optional range of the variable
 * @param step  The optional step size.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class DiscreteSchema[D : Integral] private (range: Option[(D, D)], step: Option[D]) extends NumericalSchema[D] {
  val classification = DiscreteType

  def validate(codec: Codec[D], value: D): Boolean = {
    val ev = implicitly[Integral[D]]

    import ev._

    // TODO: use codec?
    validateRange(value) && step.map(s => (value % s) == 0).getOrElse(true)
  }

  override protected def paramString(
    short: Boolean,
    f: (D) => String
  ): String = SchemaParameters.writeRange(short, range, f) + step.map(s => "," + f(s)).getOrElse("")
}

/** Companion object to `DiscreteSchema` case class. */
object DiscreteSchema {
  /** Pattern for matching short string discrete schema without step. */
  val Pattern = (DiscreteType.name + """(?:\((?:(-?\d+:-?\d+))?\))?""").r

  /** Pattern for matching short string discrete schema with step. */
  val PatternStep = (DiscreteType.name + """(?:\((?:(-?\d+:-?\d+),(\d+))?\))?""").r

  /** Construct a discrete schema with unbounded range and no step size. */
  def apply[D : Integral](): DiscreteSchema[D] = DiscreteSchema(None, None)

  /**
   * Construct a discrete schema with bounded range and no step size.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[D : Integral](lower: D, upper: D): DiscreteSchema[D] = DiscreteSchema(Option((lower, upper)), None)

  /**
   * Construct a discrete schema with bounded range and step size.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   * @param step  The step size.
   */
  def apply[
    D : Integral
  ](
    lower: D,
    upper: D,
    step: D
  ): DiscreteSchema[D] = DiscreteSchema(Option((lower, upper)), Option(step))

  /**
   * Parse a discrete schema from string.
   *
   * @param cdc The codec to parse with.
   * @param str The string to parse.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromShortString[D](cdc: Codec[D], str: String): Option[DiscreteSchema[D]] = (cdc.integral, str) match {
    case (Some(int), Pattern(null)) => Option(DiscreteSchema()(int))
    case (Some(int), PatternStep(range, step)) => SchemaParameters.splitRange(range)
      .flatMap { case (min, max) => fromComponents(cdc, min, max, step) }
    case (Some(int), Pattern(range)) => SchemaParameters.splitRange(range)
      .flatMap { case (min, max) => fromComponents(cdc, min, max) }
    case _ => None
  }

  /**
   * Parse a discrete schema from components.
   *
   * @param cdc  The codec to parse with.
   * @param min  The minimum value string to parse.
   * @param max  The maximum value string to parse.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], min: String, max: String): Option[DiscreteSchema[D]] = for {
    low <- SchemaParameters.parse(cdc, min)
    upp <- SchemaParameters.parse(cdc, max)
    int <- cdc.integral
  } yield DiscreteSchema(low, upp)(int)

  /**
   * Parse a discrete schema from components.
   *
   * @param cdc  The codec to parse with.
   * @param min  The minimum value string to parse.
   * @param max  The maximum value string to parse.
   * @param step The step size string to parse.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], min: String, max: String, step: String): Option[DiscreteSchema[D]] = for {
    low <- SchemaParameters.parse(cdc, min)
    upp <- SchemaParameters.parse(cdc, max)
    stp <- SchemaParameters.parse(cdc, step)
    int <- cdc.integral
  } yield DiscreteSchema(low, upp, stp)(int)
}

/** Trait for schemas for categorical variables. */
trait CategoricalSchema[D] extends Schema[D] {
  /** Values the variable can take. */
  val domain: Set[D]

  def validate(codec: Codec[D], value: D): Boolean = domain.isEmpty || domain.contains(value)
}

/**
 * Schema for nominal variables.
 *
 * @param domain The values of the variable.
 */
case class NominalSchema[D](domain: Set[D] = Set.empty[D]) extends CategoricalSchema[D] {
  val classification = NominalType

  override protected def paramString(
    short: Boolean,
    f: (D) => String
  ): String = SchemaParameters.writeSet(short, domain, f)
}

/** Companion object to `NominalSchema` case class. */
object NominalSchema {
  /** Pattern for matching short string nominal schema. */
  val Pattern = (NominalType.name + """(?:\((?:(.*?))?\))?""").r

  /**
   * Parse a nominal schema from string.
   *
   * @param cdc The codec to parse with.
   * @param str The string to parse.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromShortString[D](cdc: Codec[D], str: String): Option[NominalSchema[D]] = str match {
    case Pattern(null) => Option(NominalSchema())
    case Pattern("") => Option(NominalSchema())
    case Pattern(domain) => fromComponents(cdc, SchemaParameters.splitSet(domain))
    case _ => None
  }

  /**
   * Parse a nominal schema from string components.
   *
   * @param cdc The codec to parse with.
   * @param dom The domain value strings to parse.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], dom: Set[String]): Option[NominalSchema[D]] = {
    val values = dom.flatMap(SchemaParameters.parse(cdc, _))

    if (values.isEmpty || values.size != dom.size) None else Option(NominalSchema(values))
  }
}

/**
 * Schema for ordinal variables.
 *
 * @param domain The optional values of the variable.
 */
case class OrdinalSchema[D : Ordering](domain: Set[D] = Set.empty[D]) extends CategoricalSchema[D] {
  val classification = OrdinalType

  override protected def paramString(
    short: Boolean,
    f: (D) => String
  ): String = SchemaParameters.writeOrderedSet(short, domain, f)
}

/** Companion object to `OrdinalSchema`. */
object OrdinalSchema {
  /** Pattern for matching short string ordinal schema. */
  val Pattern = (OrdinalType.name + """(?:\((?:(.*?))?\))?""").r

  /**
   * Parse a ordinal schema from string.
   *
   * @param cdc The codec to parse with.
   * @param str The string to parse.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromShortString[D](cdc: Codec[D], str: String): Option[OrdinalSchema[D]] = (cdc.ordering, str) match {
    case (Some(ord), Pattern(null)) => Option(OrdinalSchema()(ord))
    case (Some(ord), Pattern("")) => Option(OrdinalSchema()(ord))
    case (Some(ord), Pattern(domain)) => fromComponents(cdc, SchemaParameters.splitSet(domain))
    case _ => None
  }

  /**
   * Parse a ordinal schema from string components.
   *
   * @param cdc The codec to parse with.
   * @param dom The domain value strings to parse.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], dom: Set[String]): Option[OrdinalSchema[D]] = {
    val values = dom.flatMap(SchemaParameters.parse(cdc, _))

    (cdc.ordering, values.isEmpty || values.size != dom.size) match {
      case (Some(ord), false) => Option(OrdinalSchema(values)(ord))
      case _ => None
    }
  }
}

/**
 * Schema for date variables.
 *
 * @param dates The optional values of the variable.
 *
 * @note The constructor is private to ensure a clean interface as provided by the `apply` methods of the companion
 *       object.
 */
case class DateSchema[D] private (dates: Option[Either[(D, D), Set[D]]])(implicit ev: D => Date) extends Schema[D] {
  val classification = DateType

  def validate(codec: Codec[D], value: D): Boolean = dates match {
    case None => true
    case Some(Left((lower, upper))) => (codec.compare(lower, value) <= 0) && (codec.compare(upper, value) >= 0)
    case Some(Right(domain)) => domain.contains(value)
  }

  override protected def paramString(
    short: Boolean,
    f: (D) => String
  ): String = dates match {
    case None => ""
    case Some(Left(range)) => SchemaParameters.writeRange(short, Option(range), f)
    case Some(Right(domain)) => SchemaParameters.writeSet(short, domain, f)
  }
}

/** Companion object to `DateSchema`. */
object DateSchema {
  /** Pattern for matching short string date schema. */
  val Pattern = (DateType.name + """(?:\((?:(.*?))?\))?""").r

  /** Construct an unbounded date schema. */
  def apply[D]()(implicit ev: D => Date): DateSchema[D] = DateSchema(None)

  /**
   * Construct a date schema with bounded range.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[D](lower: D, upper: D)(implicit ev: D => Date): DateSchema[D] = DateSchema(Option(Left((lower, upper))))

  /**
   * Construct a date schema with a set of valid dates.
   *
   * @param domain The set of legal values.
   */
  def apply[D](domain: Set[D])(implicit ev: D => Date): DateSchema[D] = DateSchema(Option(Right(domain)))

  /**
   * Parse a date schema from string.
   *
   * @param cdc The codec to parse with.
   * @param str The string to parse.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromShortString[D](cdc: Codec[D], str: String): Option[DateSchema[D]] = (cdc.date, str) match {
    case (Some(ev), Pattern(null)) => Option(DateSchema()(ev))
    case (Some(ev), Pattern("")) => Option(DateSchema()(ev))
    case (Some(ev), RangePattern(range)) => SchemaParameters.splitRange(range)
      .flatMap { case (lower, upper) => fromComponents(cdc, lower, upper) }
    case (Some(ev), Pattern(domain)) => fromComponents(cdc, SchemaParameters.splitSet(domain))
    case _ => None
  }

  /**
   * Parse a date schema from components.
   *
   * @param cdc The codec to parse with.
   * @param min The minimum value string to parse.
   * @param max The maximum value string to parse.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], min: String, max: String): Option[DateSchema[D]] = for {
    low <- SchemaParameters.parse(cdc, min)
    upp <- SchemaParameters.parse(cdc, max)
    ev <- cdc.date
  } yield DateSchema(low, upp)(ev)

  /**
   * Parse a date schema from string components.
   *
   * @param cdc The codec to parse with.
   * @param dom The domain value strings to parse.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents[D](cdc: Codec[D], dom: Set[String]): Option[DateSchema[D]] = {
    val values = dom.flatMap(SchemaParameters.parse(cdc, _))

    (cdc.date, values.isEmpty || values.size != dom.size) match {
      case (Some(ev), false) => Option(DateSchema(values)(ev))
      case _ => None
    }
  }

  private val RangePattern = (DateType.name + """(?:\((?:(.*?:.*))?\))?""").r
}

/** Functions for dealing with schema parameters. */
private object SchemaParameters {
  def parse[D](codec: Codec[D], value: String): Option[D] = codec.decode(value)

  def splitRange(range: String): Option[(String, String)] = range.split(":") match {
    case Array(lower, upper) => Option((lower, upper))
    case _ => None
  }

  def splitSet(set: String): Set[String] = set.split("(?<!\\\\),").toSet

  def writeRange[D](short: Boolean, range: Option[(D, D)], f: (D) => String): String = range
    .map { case (lower, upper) => (if (short) "" else ",") + f(lower) + (if (short) ":" else ",") + f(upper) }
    .getOrElse("")

  def writeSet[D](short: Boolean, set: Set[D], f: (D) => String): String = writeList(short, set.toList, f, "Set")

  def writeOrderedSet[
    D : Ordering
  ](
    short: Boolean,
    set: Set[D],
    f: (D) => String
  ): String = writeList(short, set.toList.sorted, f, "Set")

  def writeList[D](short: Boolean, list: List[D], f: (D) => String, name: String): String =
    if (list.isEmpty)
      ""
    else {
      val args = list.map(d => f(d).replaceAll(",", "\\\\,")).mkString(",")

      if (short) args else "," + name + "(" + args + ")"
    }
}

