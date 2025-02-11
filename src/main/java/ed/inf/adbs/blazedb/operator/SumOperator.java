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
        // Check if there is no GROUP BY clause.
        if (groupByExpressions == null || groupByExpressions.isEmpty()) {
            // This block performs a global aggregation.
            // Initialize a list to hold the aggregates.
            // We assume one aggregate value per sum expression. Adjust accordingly.
            List<Integer> aggregatedSums = new ArrayList<>();
            // Initialize each sum accumulator to 0.
            for (int i = 0; i < sumExpressions.size(); i++) {
                aggregatedSums.add(0);
            }

            // Read and aggregate all tuples produced by the child.
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                // For each SUM expression, evaluate and add the value.
                for (int i = 0; i < sumExpressions.size(); i++) {
                    int value = evaluateExpressionAsInt(tuple, sumExpressions.get(i));
                    aggregatedSums.set(i, aggregatedSums.get(i) + value);
                }
            }

            // Build the output tuple.
            List<String> outputFields = new ArrayList<>();
            for (int sum : aggregatedSums) {
                outputFields.add(String.valueOf(sum));
            }
            outputTuples = new ArrayList<>();
            outputTuples.add(new Tuple(outputFields));

            // Update the schema mapping accordingly. For example, label the fields as SUM_0, SUM_1, etc.
            Map<String, Integer> aggSchemaMapping = new LinkedHashMap<>();
            for (int i = 0; i < sumExpressions.size(); i++) {
                aggSchemaMapping.put("SUM_" + i, i);
            }
            this.schemaMapping = aggSchemaMapping;

            // Optionally, you can print the new schema mapping for debugging.
            // System.out.println("Schema mapping after global aggregation: " + schemaMapping);
        } else {
            // Existing implementation: process aggregation using a grouping key.
            Map<String, Integer> groups = new HashMap<>();
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                // Evaluate the grouping key (assumed to be in groupByExpressions.get(0)).
                String groupKey = evaluateExpressionAsString(tuple, groupByExpressions.get(0));
                // Evaluate the SUM expression; adjust if multiple SUM expressions are needed.
                int sumValue = evaluateExpressionAsInt(tuple, sumExpressions.get(0));

                int currentSum = groups.getOrDefault(groupKey, 0);
                groups.put(groupKey, currentSum + sumValue);
            }

            // Build the output tuples for each group.
            outputTuples = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : groups.entrySet()) {
                List<String> outputFields = new ArrayList<>();
                outputFields.add(entry.getKey());              // Group key value.
                outputFields.add(String.valueOf(entry.getValue())); // The computed sum.
                outputTuples.add(new Tuple(outputFields));
            }

            // Update the schema mapping for grouped aggregation.
            Map<String, Integer> aggSchemaMapping = new LinkedHashMap<>();
            aggSchemaMapping.put("Group", 0);
            aggSchemaMapping.put("SUM", 1);
            this.schemaMapping = aggSchemaMapping;

            // System.out.println("Schema mapping after grouped aggregation: " + schemaMapping);
        }
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
        // If outputTuples is null then aggregate the results:
        if (outputTuples == null) {
            computeAggregation();
        }

        // If the current index is past the list of aggregated tuples, return null.
        if (currentIndex < outputTuples.size()){
            Tuple t = outputTuples.get(currentIndex);
            // System.out.println("Returning aggregated tuple: " + t.getFields());
            currentIndex++;
            return t;
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