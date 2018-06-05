package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.gmail.a.e.tsvetkov.driver.sql.{ComparisionOperarion, _}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure}
import com.sksamuel.elastic4s.mappings.FieldDefinition

object SqlExecutor {

  def connect(host: String): HttpClient = {
    val client = HttpClient(ElasticsearchClientUri(host, 9200))
    client.execute(clusterHealth()).await match {
      case Left(value) => throw new SQLException(value.toString)
      case Right(value) =>
    }
    client
  }

  def execute(client: HttpClient, statement: SqlStatement): Unit = {
    statement match {
      case s: SqlCreateTableStatement => CreateTableExecutor.createTable(client, s)
      case s: SqlInsertStatement => InsertExecutor.execute(client, s)
    }
  }
}

object Util {
  def err(msg: String) = {
    throw new SQLException(msg)
  }
}

import com.gmail.a.e.tsvetkov.driver.sql.executor.Util._

trait Util {
  def check[T](value: Either[RequestFailure, T]) = {
    value match {
      case Left(err) => throw new SQLException(err.error.reason)
      case Right(res) => res
    }
  }
}

object CreateTableExecutor extends Util {

  def processType(dataType: DataType): String => FieldDefinition = {
    dataType match {
      case DataTypeChar(length) => keywordField
      case DataTypeNumeric(spec) => intField
      case DataTypeFloat(precission) => floatField
      case DataTypeReal => doubleField
      case DataTypeDouble => doubleField
    }
  }

  def fieldMapping(column: ColumnDefenition) = {
    column.dataType match {
      case Some(dataType) => processType(dataType)(column.name)
      case None => throw new SQLException("colum")
    }
  }

  def createTable(client: HttpClient, statement: SqlCreateTableStatement): Unit = {
    val req = createIndex(statement.name)
      .mappings(
        mapping("doc")
          .fields(statement.columnDefenition.map(fieldMapping))
      )
    check(client.execute(req).await)
  }
}

sealed trait ValueType {
  val comparator: (ComparisionOperarion, Value, Value) => Boolean
}

sealed trait Value {
  val valueType: ValueType
}

case class BooleanValue(v: Boolean) extends Value {

  case object BooleanValueType extends ValueType {
    override val comparator = (op: ComparisionOperarion, l: Value, r: Value) =>
      compare(op, l.asInstanceOf[BooleanValue], r.asInstanceOf[BooleanValue])
  }

  def compare(op: ComparisionOperarion, l: BooleanValue, r: BooleanValue) = {
    op match {
      case ComparisionOperarionEq => l.v == r.v
      case ComparisionOperarionNe => l.v != r.v
      case ComparisionOperarionGt => err("Not supported for boolean")
      case ComparisionOperarionLt => err("Not supported for boolean")
      case ComparisionOperarionGe => err("Not supported for boolean")
      case ComparisionOperarionLe => err("Not supported for boolean")
    }
  }

  override val valueType = BooleanValueType
}

case class StringValue(v: String) extends Value {

  case object StringValueType extends ValueType {
    override val comparator = (op: ComparisionOperarion, l: Value, r: Value) =>
      compare(op, l.asInstanceOf[StringValue], r.asInstanceOf[StringValue])
  }

  def compare(op: ComparisionOperarion, l: StringValue, r: StringValue) = {
    op match {
      case ComparisionOperarionEq => l.v == r.v
      case ComparisionOperarionNe => l.v != r.v
      case ComparisionOperarionGt => l.v > r.v
      case ComparisionOperarionLt => l.v < r.v
      case ComparisionOperarionGe => l.v >= r.v
      case ComparisionOperarionLe => l.v <= r.v
    }
  }


  override val valueType = StringValueType
}

case class NumericValue(v: BigDecimal) extends Value {

  case object NumericValueType extends ValueType {
    override val comparator = (op: ComparisionOperarion, l: Value, r: Value) =>
      compare(op, l.asInstanceOf[NumericValue], r.asInstanceOf[NumericValue])
  }

  def compare(op: ComparisionOperarion, l: NumericValue, r: NumericValue) = {
    op match {
      case ComparisionOperarionEq => l.v == r.v
      case ComparisionOperarionNe => l.v != r.v
      case ComparisionOperarionGt => l.v > r.v
      case ComparisionOperarionLt => l.v < r.v
      case ComparisionOperarionGe => l.v >= r.v
      case ComparisionOperarionLe => l.v <= r.v
    }
  }

  override val valueType = NumericValueType
}

object InsertExecutor extends Util {

  def asBoolean(value: Value) = {
    value match {
      case v: BooleanValue => v
      case v: StringValue => err("Is not boolean: " + v.v)
      case v: NumericValue => err("Is not boolean: " + v.v)
    }
  }

  def asNumeric(value: Value) = {
    value match {
      case v: BooleanValue => err("Is not numeric: " + v.v)
      case v: StringValue => err("Is not numeric: " + v.v)
      case v: NumericValue => v
    }
  }

  def asString(value: Value) = {
    value match {
      case v: BooleanValue => err("Is not string: " + v.v)
      case v: StringValue => v
      case v: NumericValue => err("Is not string: " + v.v)
    }
  }

  def extractValue(v: BooleanExpression): BooleanValue = {
    BooleanValue(v match {
      case BooleanExpressionBinary(operation, le, re) =>
        var l = asBoolean(extractValue(le))
        var r = asBoolean(extractValue(re))
        operation match {
          case BooleanOperarionOr => l.v | r.v
          case BooleanOperarionAnd => l.v & r.v
        }
      case BooleanExpressionNot(e) =>
        var v = asBoolean(extractValue(e))
        !v.v
      case BooleanExpressionComparision(operation, le, re) =>
        var l = extractValue(le)
        var r = extractValue(re)
        if (l.valueType != r.valueType) {
          err("Unable to compare: " + l + " and " + r)
        }
        l.valueType.comparator(operation, l, r)
    })
  }

  def extractValue(v: NumericExpression): NumericValue = {
    NumericValue(v match {
      case NumericExpressionBinary(operation, le, re) =>
        var l = asNumeric(extractValue(le))
        var r = asNumeric(extractValue(re))
        operation match {
          case NumericOperarionPlus => l.v + r.v
          case NumericOperarionMinus => l.v - r.v
          case NumericOperarionMult => l.v * r.v
          case NumericOperarionDiv => l.v / r.v
        }
      case NumericExpressionUnaryMinus(ve) =>
        var v = asNumeric(extractValue(ve))
        -v.v
      case NumericExpressionConstant(value) => BigDecimal(value)
    })
  }

  def extractValue(v: StringExpression): StringValue = {
    StringValue(v match {
      case StringExpressionBinary(operation, le, re) =>
        val l = asString(extractValue(le))
        val r = asString(extractValue(re))
        operation match {
          case StringOperarionConcat => l.v + r.v
        }
    })
  }

  def extractValue(v: ValueExpression): Value = {
    v match {
      case b: BooleanExpression => extractValue(b)
      case n: NumericExpression => extractValue(n)
      case s: StringExpression => extractValue(s)
      case b: ValueExpressionBinary => extractValue(b)
      case ValueExpressionColumnReference(id) => err("No column reference allowed")
    }
  }

  def toJsonValue(value: Value) = {
    value match {
      case BooleanValue(v) => v
      case StringValue(v) => v
      case NumericValue(v) => v.toString
    }
  }

  def createDoc(columns: Seq[String], values: Seq[ValueExpression]) = {
    columns.zip(values).map {
      case (c, v) => (c, toJsonValue(extractValue(v)))
    }
  }

  def insertReq(tableName: String, columns: Seq[String])(values: Seq[ValueExpression]) = {
    indexInto(tableName, "doc")
      .fields(createDoc(columns, values))

  }

  def execute(client: HttpClient, s: SqlInsertStatement): Unit = {
    val req = bulk(s.sources.map(insertReq(s.tableName, s.columns.get)))
    check(client.execute(req).await)
  }
}