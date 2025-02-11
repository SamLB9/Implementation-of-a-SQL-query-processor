package ed.inf.adbs.blazedb.operator;

import java.util.*;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.expression.ExpressionEvaluator;
import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.expression.Expression;

/**
 * SumOperator implements a blocking operator that performs group-by aggregation with the SUM function.
 * It reads all tuples from its child operator, organizes them into groups based on given group-by expressions,
 * and computes SUM aggregates (each of which may be a single term or a product of terms).
 */
public class SumOperator extends Operator {

    private final Operator child;
    private final List<Expression> groupByExpressions;
    private final List<Expression> sumExpressions; // expressions for SUM aggregates
    private List<Tuple> outputTuples;
    private int currentIndex;
    private Map<String, Integer> schemaMapping;

    /**
     * Constructor.
     *
     * @param child             the child operator
     * @param groupByExpressions list of expressions to group by (e.g., columns)
     * @param sumExpressions     list of expressions representing the SUM aggregates
     */
    public SumOperator(Operator child, List<Expression> groupByExpressions, List<Expression> sumExpressions, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.groupByExpressions = groupByExpressions;
        this.sumExpressions = sumExpressions;
        this.outputTuples = new ArrayList<>();
        this.currentIndex = 0;
        this.schemaMapping = schemaMapping;
        computeAggregation();  // block and compute all aggregations on construction or first getNextTuple call
    }

    /**
     * Reads all input tuples from the child operator, organizes them into groups, and computes the aggregate sums.
     */
    private void computeAggregation() {
        // Create a map to store the grouping key and its aggregated sum.
        // The key is the result of evaluating the group-by column (Enrolled.E),
        // and the value is the sum aggregate.
        Map<String, Integer> groups = new HashMap<>();

        // Read all tuples from the child operator.
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            // Evaluate the grouping key, e.g., Enrolled.E.
            // Make sure that groupByExpressions.get(0) corresponds to Enrolled.E.
            String groupKey = evaluateExpressionAsString(tuple, groupByExpressions.get(0));

            // Evaluate the SUM expression on the tuple, e.g., (Enrolled.H * Enrolled.H).
            int sumValue = evaluateExpressionAsInt(tuple, sumExpressions.get(0));

            // Update the running sum for the current group.
            int currentSum = groups.getOrDefault(groupKey, 0);
            groups.put(groupKey, currentSum + sumValue);
        }

        // After processing all tuples, build the aggregated output
        // Each output tuple should contain both the grouping value and the aggregate.
        outputTuples = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : groups.entrySet()) {
            // Create a tuple where the first field is the group key and the second is the sum.
            List<String> outputFields = new ArrayList<>();
            outputFields.add(entry.getKey());              // Expected to be 101, 102, etc.
            outputFields.add(String.valueOf(entry.getValue())); // The computed sum.
            outputTuples.add(new Tuple(outputFields));
        }

        Map<String, Integer> aggSchemaMapping = new LinkedHashMap<>();
        aggSchemaMapping.put("Group", 0);   // representing the grouping column (e.g., Enrolled.E)
        aggSchemaMapping.put("SUM", 1);     // representing the computed sum

        // Update the operator's schema mapping to the new aggregated one.
        this.schemaMapping = aggSchemaMapping;
        System.out.println("Schema mapping after aggregation: " + schemaMapping);
    }

    public Map<String, Integer> getSchemaMapping() {
        return this.schemaMapping;
    }

    /**
     * Evaluates an expression on a tuple and returns the result as a String.
     * This is a placeholder helper method; you need to implement a proper expression evaluation
     * (possibly using a visitor) based on your system's design.
     *
     * @param tuple the input tuple
     * @param expr  the expression to evaluate (e.g., a column reference)
     * @return the result as a String
     */
    private String evaluateExpressionAsString(Tuple tuple, Expression expr) {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schemaMapping);
        expr.accept(evaluator);
        Object value = evaluator.getCurrentValue();
        // System.out.println("Evaluated expression: " + expr + " -> " + value);
        return value.toString();
    }


    /**
     * Evaluates an expression on a tuple and returns the result as an integer.
     * This works for expressions used in the SUM aggregates.
     *
     * @param tuple the input tuple
     * @param expr  the expression to evaluate
     * @return the integer value of the evaluated expression
     */
    private int evaluateExpressionAsInt(Tuple tuple, Expression expr) {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schemaMapping);
        expr.accept(evaluator);
        Object value = evaluator.getCurrentValue();
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Cannot convert expression result to int: " + value);
            }
        } else {
            throw new RuntimeException("Unexpected expression evaluation result type: " + value);
        }
    }

    /**
     * Return the next aggregated tuple. Since this is a blocking operator, all
     * input is processed during initialization (or the first call).
     *
     * @return the next aggregated tuple or null if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        if (currentIndex < outputTuples.size()) {
            return outputTuples.get(currentIndex++);
        }
        return null;
    }

    /**
     * Resets the operator by letting the consumer read the aggregated results again.
     */
    @Override
    public void reset() {
        currentIndex = 0;
    }
}