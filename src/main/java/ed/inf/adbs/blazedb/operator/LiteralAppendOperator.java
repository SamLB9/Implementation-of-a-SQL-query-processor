package ed.inf.adbs.blazedb.operator;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.DoubleValue;

public class LiteralAppendOperator extends Operator {

    private final Operator child;
    private final LinkedHashMap<Expression, String> literalMapping;
    private final Map<String, Integer> schemaMapping;

    public LiteralAppendOperator(Operator child, Map<Expression, String> literalMapping, Map<String, Integer> schemaMapping) {
        this.child = child;
        if (literalMapping instanceof LinkedHashMap) {
            this.literalMapping = (LinkedHashMap<Expression, String>) literalMapping;
        } else {
            this.literalMapping = new LinkedHashMap<>(literalMapping);
        }
        this.schemaMapping = schemaMapping;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = child.getNextTuple();
        if (tuple == null) {
            return null;
        }

        // Instead of tuple.getData(), we assume your Tuple provides a method getValues()
        // that returns a List of String values representing the tuple's columns.
        List<String> originalValues = tuple.getFields();

        // Create a new list to hold the extended tuple.
        List<String> extendedValues = new ArrayList<>(originalValues);

        // Append the literal values.
        for (Map.Entry<Expression, String> entry : literalMapping.entrySet()) {
            Expression literalExpr = entry.getKey();
            String value;
            if (literalExpr instanceof LongValue) {
                value = Long.toString(((LongValue) literalExpr).getValue());
            } else if (literalExpr instanceof DoubleValue) {
                value = Double.toString(((DoubleValue) literalExpr).getValue());
            } else {
                value = literalExpr.toString();
            }
            extendedValues.add(value);
        }

        // Construct and return a new Tuple using the extended values.
        return new Tuple(extendedValues);
    }

    @Override
    public void reset() {
        child.reset();
    }
}