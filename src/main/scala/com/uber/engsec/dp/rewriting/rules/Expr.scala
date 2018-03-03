package com.uber.engsec.dp.rewriting.rules

import com.uber.engsec.dp.dataflow.column.AbstractColumnAnalysis.ColumnFacts
import com.uber.engsec.dp.dataflow.column.NodeColumnFacts
import com.uber.engsec.dp.rewriting.rules.Expr.{ColumnReference, SqlOperatorExpr, rexBuilder}
import com.uber.engsec.dp.sql.relational_algebra.Relation
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{InferTypes, OperandTypes, ReturnTypes, SqlTypeFactoryImpl}
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import scala.collection.JavaConverters._

/** A simple DSL for defining expressions for rewriters in Scala. */

sealed trait Expr {
  def toRex(target: Relation): RexNode
}

/** Base class for value expressions.
  */
abstract class ValueExpr extends Expr {
  /** Infix notation for operators on value expressions. */
  def +(other: ValueExpr): Expr.Plus = Expr.Plus(this, other)
  def -(other: ValueExpr): Expr.Minus = Expr.Minus(this, other)
  def /(other: ValueExpr): Expr.Divide = Expr.Divide(this, other)
  def *(other: ValueExpr): Expr.Multiply = Expr.Multiply(this, other)
  def %(other: ValueExpr): Expr.Modulus = Expr.Modulus(this, other)
  def ==(other: ValueExpr): Expr.Equals = Expr.Equals(this, other)
  def <(other: ValueExpr): Expr.LessThan = Expr.LessThan(this, other)
  def >(other: ValueExpr): Expr.GreaterThan = Expr.GreaterThan(this, other)
  def <=(other: ValueExpr): Expr.LessThanOrEqual = Expr.LessThanOrEqual(this, other)
  def >=(other: ValueExpr): Expr.GreaterThanOrEqual = Expr.GreaterThanOrEqual(this, other)
}

/** Base class for aggregation expressions.
  */
abstract class AggExpr(val aggFunction: SqlAggFunction, val col: Option[ColumnReference]) extends Expr {
  override def toRex(target: Relation): RexNode = rexBuilder.makeCall(aggFunction, col.map{_.toRex(target)}.toList.asJava)
}

object Expr {
  sealed trait Predicate extends ValueExpr

  import scala.collection.JavaConverters._
  import scala.language.implicitConversions

  val rexBuilder: RexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT))

  class SimpleValueExpr(rexNode: RexNode) extends ValueExpr {
    override def toRex(target: Relation): RexNode = rexNode
  }

  class SqlOperatorExpr(op: SqlOperator, args: ValueExpr*) extends ValueExpr {
    override def toRex(target: Relation): RexNode = rexBuilder.makeCall(op, args.map{ _.toRex(target) }.asJava)
  }

  // Column reference
  trait ColumnReference extends ValueExpr {
    override def toRex(target: Relation): RexNode = {
      val colIdx = Helpers.lookupColumnOrdinal(target.unwrap, this)
      rexBuilder.makeInputRef(target.getRowType.getFieldList.get(colIdx).getType, colIdx)
    }
  }
  case class ColumnReferenceByOrdinal(idx: Int) extends ColumnReference
  case class ColumnReferenceByName(name: String) extends ColumnReference

  sealed trait QualifiedColumnReference{
    val qualifier: RelationQualifier
  }

  class QualifiedColumnReferenceByName(override val name: String, val qualifier: RelationQualifier) extends ColumnReferenceByName(name) with QualifiedColumnReference
  class QualifiedColumnReferenceByOrdinal(override val idx: Int, val qualifier: RelationQualifier) extends ColumnReferenceByOrdinal(idx) with QualifiedColumnReference

  /** Expression literal for use in column references, e.g., project(my_col, *) and function-star syntax, e.g., COUNT(*)
    * Handled specially in places where its use is legal, so toRex should never be invoked on this object.
    */
  object * extends ColumnReferenceByName("*") {
    override def toRex(target: Relation): RexNode = throw new RuntimeException("*.toRex")
  }

  // SQL operators
  case class Plus(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.PLUS, left, right)
  case class Minus(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.MINUS, left, right)
  case class Multiply(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.MULTIPLY, left, right)
  case class Modulus(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.MOD, left, right)
  case class Divide(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.DIVIDE, left, right)
  case class Abs(expr: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.ABS, expr)
  case class Round(expr: ValueExpr, length: Int) extends SqlOperatorExpr(SqlStdOperatorTable.ROUND, expr, int2Expr(length))
  case class Ceiling(expr: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.CEIL, expr)
  case class Floor(expr: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.FLOOR, expr)
  case object Rand extends SqlOperatorExpr(SqlStdOperatorTable.RAND)

  // Literals
  case class LiteralBool(value: Boolean) extends SimpleValueExpr(rexBuilder.makeLiteral(value)) with Predicate
  case class LiteralString(value: String) extends SimpleValueExpr(rexBuilder.makeLiteral(value)) with Predicate
  case class LiteralInt(value: Int) extends SimpleValueExpr(rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(value)))
  case class LiteralDouble(value: Double) extends SimpleValueExpr(rexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(value)))

  // Case
  case class Case(condition: ValueExpr, ifTrue: ValueExpr, ifFalse: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.CASE, condition, ifTrue, ifFalse)

  // Comparison operators
  case class Equals(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.EQUALS, left, right) with Predicate
  case class NotEquals(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.NOT_EQUALS, left, right) with Predicate
  case class LessThan(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.LESS_THAN, left, right) with Predicate
  case class LessThanOrEqual(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, left, right) with Predicate
  case class GreaterThan(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.GREATER_THAN, left, right) with Predicate
  case class GreaterThanOrEqual(left: ValueExpr, right: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, left, right) with Predicate

  // Postfix operators
  case class IsNull(expr: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.IS_NULL, expr) with Predicate
  case class IsNotNull(expr: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.IS_NOT_NULL, expr) with Predicate

  // Math functions
  case class Ln(arg: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.LN, arg)
  case class Exp(arg: ValueExpr) extends SqlOperatorExpr(SqlStdOperatorTable.EXP, arg)

  // Special functions
  case object RowNumber extends SqlOperatorExpr(SqlStdOperatorTable.ROW_NUMBER)

  // Functions to convert Scala primitive data types into expression nodes
  implicit def int2Expr(value: Int): LiteralInt = LiteralInt(value)
  implicit def double2Expr(value: Double): LiteralDouble = LiteralDouble(value)
  implicit def bool2Expr(value: Boolean): LiteralBool = LiteralBool(value)
  implicit def string2Expr(value: String): LiteralString = LiteralString(value)
  implicit def rex2Expr(expr: RexNode): SimpleValueExpr = new SimpleValueExpr(expr)

  // Fetch a reference to all columns satisfying a given predicate
  implicit class ColsWhereNodeColumnFacts[T](facts: NodeColumnFacts[_,T]) {
    def colsWhere(predicate: T => Boolean): Seq[ColumnReferenceByOrdinal] = {
      facts.colFacts.colsWhere(predicate)
    }
  }
  implicit class ColsWhereColumnFacts[T](facts: ColumnFacts[T]) {
    def colsWhere(predicate: T => Boolean): Seq[ColumnReferenceByOrdinal] = {
      facts.zipWithIndex.filter{ case (fact, _) => predicate(fact) }.map{ case (_, idx) => col(idx) }
    }
  }

  // Wildcard selection
  implicit class ColsFromRelation(relation: Relation) {
    def * : Seq[ColumnDefinitionWithAlias[ColumnReferenceByName]] = {
      relation.getRowType.getFieldNames.asScala.map { name => ColumnDefinition.columnReferenceToColumnDefinitionWithName( ColumnReferenceByName(name) ) }
    }
  }

  sealed trait RelationQualifier {
    def apply(name: String): QualifiedColumnReferenceByName = new QualifiedColumnReferenceByName(name, this)
    def apply(idx: Int): QualifiedColumnReferenceByOrdinal = new QualifiedColumnReferenceByOrdinal(idx, this)
    def apply(ref: ColumnReferenceByName): QualifiedColumnReferenceByName = new QualifiedColumnReferenceByName(ref.name, this)
    def apply(ord: ColumnReferenceByOrdinal): QualifiedColumnReferenceByOrdinal = new QualifiedColumnReferenceByOrdinal(ord.idx, this)
    def apply(col: ColumnDefinitionWithAlias[_]): QualifiedColumnReferenceByName = new QualifiedColumnReferenceByName(col.alias, this)
  }
  case object left extends RelationQualifier
  case object right extends RelationQualifier

  def col(ordinal: Int): ColumnReferenceByOrdinal = ColumnReferenceByOrdinal(ordinal)
  def col(name: String): ColumnReferenceByName = ColumnReferenceByName(name)
  def col(ref: ColumnReferenceByName): ColumnReferenceByName = ColumnReferenceByName(ref.name)
  def col(col: ColumnDefinitionWithOrdinal[_]): ColumnReferenceByOrdinal = ColumnReferenceByOrdinal(col.idx)
  def col(col: ColumnDefinitionWithAlias[_]): ColumnReferenceByName = ColumnReferenceByName(col.alias)

  // Aggregation functions
  case class Sum(_col: ColumnReference) extends AggExpr(SqlStdOperatorTable.SUM, Some(_col))
  case class Avg(_col: ColumnReference) extends AggExpr(SqlStdOperatorTable.AVG, Some(_col))
  case class Min(_col: ColumnReference) extends AggExpr(SqlStdOperatorTable.MIN, Some(_col))
  case class Max(_col: ColumnReference) extends AggExpr(SqlStdOperatorTable.MAX, Some(_col))
  case class Count(_col: ColumnReference) extends AggExpr(SqlStdOperatorTable.COUNT, if (_col eq *) None else Some(_col))
  case class Median(_col: ColumnReference) extends SqlOperatorExpr(SqlExtFunctions.MEDIAN, _col)

  object True extends LiteralBool(true)
  object False extends LiteralBool(false)

  /** Dummy function to ensure a column alias survives to SQL output. This is due to quirk in Calcite's RelToSql code;
    * if a relation projects only column references, Calcite will ignore the aliases specified in the projection
    * and instead rely on the schema (and column name) of the projection's input. Projecting any compound expression
    * prevents this from happening, so we call a dummy function which is defined below to have no impact on the output
    * query. Rewriters should use this function on simple column projections whenever correct functionality requires
    * the column alias to be preserved.
    *
    * Note that creating a call of type SqlStdOperatorTable.AS does not resolve this problem, as it instead produces
    * _two_ AS clauses in the output query -- one from the AS function itself, and the other because the presence of the
    * call prevents this bug from happening.
    */
  case class EnsureAlias(expr: ValueExpr) extends SqlOperatorExpr(SqlExtFunctions.ENSURE_ALIAS, expr)
}

/** Extensions to Calcite's built-in SQL function definitions.
  */
object SqlExtFunctions {
  val MEDIAN = new SqlAggFunction("MEDIAN", null, SqlKind.OTHER, ReturnTypes.ARG0, null, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC, false, true) {}

  val ENSURE_ALIAS = new SqlSpecialOperator("ensure_alias", SqlKind.DEFAULT, 20, true, ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.ANY_ANY) {
    override def unparse(writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int): Unit = call.operand[SqlNode](0).unparse(writer, leftPrec, rightPrec)
  }

  /** Generic function with caller-provided name and arguments. Use with caution as this type of expression is not
    * validated, hence emitted query is not guaranteed to conform to standard SQL and may not run on any database.
    */
  case class GenericFunction(name: String, args: ValueExpr*) extends SqlOperatorExpr(new SqlFunction(name, SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0, null, OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM), args: _*)
}
