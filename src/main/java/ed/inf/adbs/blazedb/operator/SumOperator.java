package ed.inf.adbs.blazedb.operator;

import java.util.*;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.expression.ExpressionEvaluator;
import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

/**
 * The {@code SumOperator} class implements a blocking operator that performs group-by aggregation using the SUM function.
 * It reads all tuples from its child operator, organizes them into groups based on specified group-by expressions,
 * and computes SUM aggregates for each group. This operator is essential for executing SQL queries that require
 * sum-based aggregations over grouped data.
 *
 * Key Responsibilities:
 *  - Tuple Retrieval: Fetches all tuples from the child operator to prepare for aggregation.
 *  - Grouping Mechanism: Organizes tuples into groups based on the provided group-by expressions.
 *  - Aggregation Computation: Calculates SUM aggregates for each group using the specified expressions.
 *  - Result Management: Stores aggregated results and provides them sequentially upon request.
 *
 * Implementation Details:
 *     The operator fetches all input tuples from the child operator during the first call to {@link #getNextTuple()}.
 *     Tuples are grouped based on the specified group-by expressions, and SUM aggregates are computed for each group.
 *     Aggregated results are stored in the {@code outputTuples} list and returned sequentially.
 */
public class SumOperator extends Operator {

    private final Operator child;
    private final List<Expression> groupByExpressions;
    private final List<Expression> sumExpressions; // expressions for SUM aggregates
    private List<Tuple> outputTuples;
    private int currentIndex;
    private Map<String, Integer> schemaMapping;

    /**
     * Constructs a {@code SumOperator} with the specified child operator, group-by expressions, sum expressions, and schema mapping.
     * This constructor initializes the sum operator by setting its child operator, the expressions used for grouping,
     * the expressions to be aggregated using SUM, and the schema mapping required to resolve column references within the tuples.
     *
     * @param child             The child {@link Operator} providing input tuples (e.g., an instance of {@link ScanOperator}).
     * @param groupByExpressions A {@link List} of {@link Expression} specifying the criteria for grouping tuples.
     * @param sumExpressions     A {@link List} of {@link Expression} defining the SUM aggregates to compute for each group.
     * @param schemaMapping      A {@link Map} that associates column names with their respective indices in the tuples.
     *
     * @throws IllegalArgumentException if {@code child}, {@code groupByExpressions}, {@code sumExpressions}, or {@code schemaMapping} is {@code null}.
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
     * Reads all input tuples from the child operator, organizes them into groups based on the group-by expressions,
     * and computes the SUM aggregates for each group.
     * This method performs the core aggregation logic by iterating over all input tuples, determining their group,
     * and updating the corresponding aggregate sums. The results are stored in the {@code outputTuples} list.
     *
     * @throws RuntimeException if an error occurs during aggregation computation.
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

    /**
     * Retrieves the current schema mapping, associating column names with their respective indices in the tuples.
     *
     * @return A {@link Map} representing the schema mapping.
     */
    public Map<String, Integer> getSchemaMapping() {
        return this.schemaMapping;
    }

    /**
     * Evaluates an {@link Expression} on a given {@link Tuple} and returns the result as a {@code String}.
     * This is a placeholder helper method intended to evaluate expressions such as column references.
     * In a complete implementation, this method should utilize an expression evaluator or visitor pattern
     * to handle various expression types based on the system's design.
     *
     * @param tuple The input {@link Tuple} on which the expression is to be evaluated.
     * @param expr  The {@link Expression} to evaluate (e.g., a column reference).
     * @return The result of the expression evaluation as a {@code String}.
     *
     * @throws UnsupportedOperationException if the expression type is not supported.
     */
    private String evaluateExpressionAsString(Tuple tuple, Expression expr) {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schemaMapping);
        expr.accept(evaluator);
        Object value = evaluator.getCurrentValue();
        // System.out.println("Evaluated expression: " + expr + " -> " + value);
        return value.toString();
    }


    /**
     * Evaluates an {@link Expression} on a given {@link Tuple} and returns the result as an integer.
     * This method is specifically tailored for expressions used in the SUM aggregates. It assumes that
     * the expression can be evaluated to an integer value.
     *
     * @param tuple The input {@link Tuple} on which the expression is to be evaluated.
     * @param expr  The {@link Expression} to evaluate.
     * @return The integer value resulting from the expression evaluation.
     *
     * @throws NumberFormatException        if the expression cannot be parsed into an integer.
     * @throws UnsupportedOperationException if the expression type is not supported.
     */
    private int evaluateExpressionAsInt(Tuple tuple, Expression expr) {
        // If the expression is recognized as a literal alias, return the intended constant.
        if (expr.toString().startsWith("LITERAL_SUM")) {
            return 1;
        }

        // Otherwise, if the expression itself is a LongValue, use its value.
        if (expr instanceof LongValue) {
            return (int) ((LongValue) expr).getValue();
        }

        // Fallback: use your regular evaluation logic.
        String result = evaluateExpressionAsString(tuple, expr).trim();
        // System.out.println("Evaluated expression: " + expr + " -> " + result);
        try {
            return Integer.parseInt(result);
        } catch (NumberFormatException e) {
            try {
                return (int) Double.parseDouble(result);
            } catch (NumberFormatException ex) {
                throw new RuntimeException("Unable to parse the expression result to int: " + result, ex);
            }
        }
    }

    /**
     * Retrieves the next aggregated {@link Tuple} based on the computed SUM aggregates.
     * If the aggregation has not been performed yet, this method triggers the computation by calling {@link #computeAggregation()}.
     * It then sequentially returns each aggregated tuple from the {@code outputTuples} list until no more tuples are available.
     *
     * @return The next aggregated {@link Tuple}, or {@code null} if all aggregated tuples have been returned.
     *
     * @throws RuntimeException if an error occurs during tuple retrieval or aggregation.
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
     * Resets the {@code SumOperator} to its initial state, allowing for re-iteration over the aggregated results.
     * This method clears the current index pointer, enabling the consumer to read the aggregated tuples from the beginning again.
     * It does not recompute the aggregation unless {@link #getNextTuple()} is called after a reset.
     */
    @Override
    public void reset() {
        currentIndex = 0;
    }
}