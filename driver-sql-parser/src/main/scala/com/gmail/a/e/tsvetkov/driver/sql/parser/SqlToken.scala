package com.gmail.a.e.tsvetkov.driver.sql.parser

sealed trait SqlToken

object SqlToken {

  case class ERROR(msg: String) extends SqlToken

  case class IDENTIFIER(value: String) extends SqlToken

  case class LITERAL_NUMERIC(value: String) extends SqlToken

  case object OP_PLUS extends SqlToken

  case object OP_MINUS extends SqlToken

  case object OP_MUL extends SqlToken

  case object OP_DIV extends SqlToken

  case object OP_EQ extends SqlToken

  case object OP_NE extends SqlToken

  case object OP_LT extends SqlToken

  case object OP_GT extends SqlToken

  case object OP_LE extends SqlToken

  case object OP_GE extends SqlToken

  case object OP_CONCAT extends SqlToken

  case object PERIOD extends SqlToken

  case object COMMA extends SqlToken

  case object BRACKET_LEFT extends SqlToken

  case object BRACKET_RIGHT extends SqlToken

  //Keywords
  case object CREATE extends SqlToken

  case object CHAR extends SqlToken

  case object CHARACTER extends SqlToken

  case object VARCHAR extends SqlToken

  case object TABLE extends SqlToken

  case object VARYING extends SqlToken

  case object NUMERIC extends SqlToken

  case object DECIMAL extends SqlToken

  case object DEC extends SqlToken

  case object SMALLINT extends SqlToken

  case object INTEGER extends SqlToken

  case object INT extends SqlToken

  case object BIGINT extends SqlToken

  case object FLOAT extends SqlToken

  case object REAL extends SqlToken

  case object DOUBLE extends SqlToken

  case object PRECISION extends SqlToken

  case object SELECT extends SqlToken

  case object AS extends SqlToken

  case object FROM extends SqlToken

  case object ONLY extends SqlToken

  case object JOIN extends SqlToken

  case object INNER extends SqlToken

  case object OUTER extends SqlToken

  case object LEFT extends SqlToken

  case object RIGHT extends SqlToken

  case object FULL extends SqlToken

  case object ON extends SqlToken

  case object OR extends SqlToken

  case object AND extends SqlToken

  case object TRUE extends SqlToken

  case object FALSE extends SqlToken

  case object UNKNOWN extends SqlToken

  case object IS extends SqlToken

  case object NOT extends SqlToken

  case object COLLATE extends SqlToken

  case object INSERT extends SqlToken

  case object INTO extends SqlToken

  case object VALUES extends SqlToken

  case object WHERE extends SqlToken

  case object ORDER extends SqlToken

  case object BY extends SqlToken

  case object ASC extends SqlToken

  case object DESC extends SqlToken

}