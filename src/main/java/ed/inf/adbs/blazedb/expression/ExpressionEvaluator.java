package ed.inf.adbs.blazedb.expression;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.Map;

public class ExpressionEvaluator implements ExpressionVisitor {

    private Tuple tuple;
    // Schema mapping from qualified column names (e.g., "Student.sid") to tuple field indexes.
    private Map<String, Integer> schemaMapping;
    // Current evaluation result.
    private Object currentValue;

    public ExpressionEvaluator(Tuple tuple, Map<String, Integer> schemaMapping) {
        this.tuple = tuple;
        this.schemaMapping = schemaMapping;
    }


    public boolean evaluate(Expression expr) {
        expr.accept(this);
        if (currentValue instanceof Boolean) {
            return (Boolean) currentValue;
        } else {
            throw new IllegalStateException("Expression did not evaluate to a boolean: " + expr);
        }
    }


    // Helper method for evaluating subexpressions.
    private Object evaluateSubExpression(Expression expr) {
        ExpressionEvaluator subEvaluator = new ExpressionEvaluator(tuple, schemaMapping);
        expr.accept(subEvaluator);
        return subEvaluator.currentValue;
    }

    @Override
    public void visit(LongValue longValue) {
        currentValue = longValue.getValue();
    }


    @Override
    public void visit(HexValue hexValue) {

    }


    @Override
    public void visit(Column column) {
        // Get the fully qualified name (e.g., "Car.Price")
        String fullColumnName = column.getFullyQualifiedName();
        // Look up the index using the schemaMapping.
        Integer columnIndex = schemaMapping.get(fullColumnName);
        if (columnIndex == null) {
            throw new RuntimeException("Column " + fullColumnName + " not found in schema mapping.");
        }
        String fieldValue = tuple.getFields().get(columnIndex);

        // Attempt to parse the field value as a number.
        try {
            // You can adjust the parsing as needed (Long/Double) based on your schema.
            currentValue = Long.parseLong(fieldValue);
        } catch (NumberFormatException e) {
            // If not numeric, retain the string value.
            currentValue = fieldValue;
        }
    }


    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(MemberOfExpression memberOfExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }

    @Override
    public void visit(CastExpression castExpression) {

    }

    @Override
    public void visit(Modulo modulo) {

    }

    @Override
    public void visit(AnalyticExpression analyticExpression) {

    }

    @Override
    public void visit(ExtractExpression extractExpression) {

    }

    @Override
    public void visit(IntervalExpression intervalExpression) {

    }

    @Override
    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

    }

    @Override
    public void visit(RegExpMatchOperator regExpMatchOperator) {

    }

    @Override
    public void visit(JsonExpression jsonExpression) {

    }

    @Override
    public void visit(JsonOperator jsonOperator) {

    }

    @Override
    public void visit(UserVariable userVariable) {

    }

    @Override
    public void visit(NumericBind numericBind) {

    }

    @Override
    public void visit(KeepExpression keepExpression) {

    }

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {

    }

    @Override
    public void visit(ExpressionList<?> expressionList) {

    }

    @Override
    public void visit(RowConstructor<?> rowConstructor) {

    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {

    }

    @Override
    public void visit(OracleHint oracleHint) {

    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {

    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

    }

    @Override
    public void visit(NotExpression notExpression) {

    }

    @Override
    public void visit(NextValExpression nextValExpression) {

    }

    @Override
    public void visit(CollateExpression collateExpression) {

    }

    @Override
    public void visit(SimilarToExpression similarToExpression) {

    }

    @Override
    public void visit(ArrayExpression arrayExpression) {

    }

    @Override
    public void visit(ArrayConstructor arrayConstructor) {

    }

    @Override
    public void visit(VariableAssignment variableAssignment) {

    }

    @Override
    public void visit(XMLSerializeExpr xmlSerializeExpr) {

    }

    @Override
    public void visit(TimezoneExpression timezoneExpression) {

    }

    @Override
    public void visit(JsonAggregateFunction jsonAggregateFunction) {

    }

    @Override
    public void visit(JsonFunction jsonFunction) {

    }

    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {

    }

    @Override
    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {

    }

    @Override
    public void visit(AllColumns allColumns) {

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {

    }

    @Override
    public void visit(AllValue allValue) {

    }

    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {

    }

    @Override
    public void visit(GeometryDistance geometryDistance) {

    }

    @Override
    public void visit(Select select) {

    }

    @Override
    public void visit(TranscodingFunction transcodingFunction) {

    }

    @Override
    public void visit(TrimFunction trimFunction) {

    }

    @Override
    public void visit(RangeExpression rangeExpression) {

    }

    @Override
    public void visit(AndExpression andExpression) {
        Object leftResult = evaluateSubExpression(andExpression.getLeftExpression());
        Object rightResult = evaluateSubExpression(andExpression.getRightExpression());
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw new IllegalArgumentException("AND expression operands must be boolean.");
        }
        currentValue = ((Boolean) leftResult) && ((Boolean) rightResult);
    }

    @Override
    public void visit(OrExpression orExpression) {

    }

    @Override
    public void visit(XorExpression xorExpression) {

    }

    @Override
    public void visit(Between between) {

    }

    @Override
    public void visit(OverlapsCondition overlapsCondition) {

    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Object leftResult = evaluateSubExpression(equalsTo.getLeftExpression());
        Object rightResult = evaluateSubExpression(equalsTo.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            currentValue = (((Number) leftResult).longValue() == ((Number) rightResult).longValue());
        } else {
            currentValue = leftResult.equals(rightResult);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        Object leftResult = evaluateSubExpression(greaterThan.getLeftExpression());
        Object rightResult = evaluateSubExpression(greaterThan.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            currentValue = (((Number) leftResult).longValue() > ((Number) rightResult).longValue());
        } else {
            throw new IllegalArgumentException("Operands of '>' must be numeric.");
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        // Evaluate left expression.
        greaterThanEquals.getLeftExpression().accept(this);
        Object leftValue = currentValue;
        // Evaluate right expression.
        greaterThanEquals.getRightExpression().accept(this);
        Object rightValue = currentValue;

        if (leftValue instanceof Number && rightValue instanceof Number) {
            // Compare using double values.
            currentValue = ((Number) leftValue).doubleValue() >= ((Number) rightValue).doubleValue();
        } else {
            throw new RuntimeException("Expression did not evaluate to a boolean: " + greaterThanEquals);
        }
    }


    @Override
    public void visit(InExpression inExpression) {

    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {

    }

    @Override
    public void visit(MinorThan minorThan) {
        Object leftResult = evaluateSubExpression(minorThan.getLeftExpression());
        Object rightResult = evaluateSubExpression(minorThan.getRightExpression());

        if (leftResult instanceof Number && rightResult instanceof Number) {
            currentValue = (((Number) leftResult).longValue() < ((Number) rightResult).longValue());
        } else {
            throw new IllegalArgumentException("Operands of '<' must be numeric.");
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        // Evaluate left expression.
        minorThanEquals.getLeftExpression().accept(this);
        Object leftValue = currentValue;
        // Evaluate right expression.
        minorThanEquals.getRightExpression().accept(this);
        Object rightValue = currentValue;

        if (leftValue instanceof Number && rightValue instanceof Number) {
            // Compare using double values.
            currentValue = ((Number) leftValue).doubleValue() <= ((Number) rightValue).doubleValue();
        } else {
            throw new RuntimeException("Expression did not evaluate to a boolean: " + minorThanEquals);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        Object leftResult = evaluateSubExpression(notEqualsTo.getLeftExpression());
        Object rightResult = evaluateSubExpression(notEqualsTo.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            currentValue = (((Number) leftResult).longValue() != ((Number) rightResult).longValue());
        } else {
            currentValue = !leftResult.equals(rightResult);
        }
    }

    @Override
    public void visit(ParenthesedSelect parenthesedSelect) {

    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(Addition addition) {
        Object leftResult = evaluateSubExpression(addition.getLeftExpression());
        Object rightResult = evaluateSubExpression(addition.getRightExpression());
        if (leftResult instanceof Number && rightResult instanceof Number) {
            currentValue = ((Number) leftResult).longValue() + ((Number) rightResult).longValue();
        } else {
            throw new IllegalArgumentException("Operands of '+' must be numeric.");
        }
    }

    @Override
    public void visit(Division division) {
        throw new UnsupportedOperationException("Division not supported yet.");
    }

    @Override
    public void visit(IntegerDivision integerDivision) {

    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        int left = convertToInt(currentValue);
        // System.out.println("Left: " + left);
        multiplication.getRightExpression().accept(this);
        int right = convertToInt(currentValue);
        // System.out.println("Right: " + right);
        currentValue = left * right;
        // System.out.println("Multiplication result: " + currentValue);
    }


    private int convertToInt(Object value) {
        if (value instanceof Number) {
            return ((Number)value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String)value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Error converting to int: " + value);
            }
        } else {
            throw new RuntimeException("Unexpected type for arithmetic operation: " + value);
        }
    }

    @Override
    public void visit(Subtraction subtraction) {
        throw new UnsupportedOperationException("Subtraction not supported yet.");
    }

    @Override
    public void visit(BitwiseRightShift bitwiseRightShift) {
        throw new UnsupportedOperationException("BitwiseRightShift not supported yet.");
    }

    @Override
    public void visit(BitwiseLeftShift bitwiseLeftShift) {
        throw new UnsupportedOperationException("BitwiseLeftShift not supported yet.");
    }

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {
        throw new UnsupportedOperationException("Function not supported yet.");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        throw new UnsupportedOperationException("SignedExpression not supported yet.");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new UnsupportedOperationException("JdbcParameter not supported yet.");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw new UnsupportedOperationException("JdbcNamedParameter not supported yet.");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new UnsupportedOperationException("DoubleValue not supported yet.");
    }

    @Override
    public void visit(StringValue stringValue) {
        throw new UnsupportedOperationException("StringValue not supported yet.");
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new UnsupportedOperationException("DateValue not supported yet.");
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new UnsupportedOperationException("TimeValue not supported yet.");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new UnsupportedOperationException("TimestampValue not supported yet.");
    }

    public Object getCurrentValue() {
        return currentValue;
    }

    // Implement any additional methods required by the ExpressionVisitor interface.
}