package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.expression.ExpressionEvaluator;

import java.util.Map;

/**
 * The {@code SelectOperator} class implements the selection operation within the BlazeDB framework.
 * It filters tuples produced by its child operator based on a specified selection condition,
 * typically derived from the WHERE clause of a SQL query. Only tuples that satisfy the condition
 * are propagated downstream for further processing.
 *
 * Key Responsibilities:
 * - Tuple Filtering: Evaluates each tuple against the selection condition and filters out those that do not satisfy it.
 * - Condition Evaluation: Utilizes an {@link ExpressionEvaluator} to dynamically assess complex expressions.
 * - Schema Mapping: Maintains a mapping between qualified column names and their indices to accurately reference tuple fields during evaluation.
 * - State Management: Supports resetting of the operator's state, allowing for re-execution or iteration over the filtered data.
 *
 * Implementation Details:
 *  - The class maintains a reference to its child operator, from which it retrieves input tuples.
 *  - The {@code selectionCondition} represents the filtering logic that determines whether a tuple should be included in the output.
 *  - A schema mapping ({@code schemaMapping}) is used to resolve column references within the selection condition, ensuring accurate field evaluation.
 *  - The selection process is performed iteratively, evaluating each tuple until one satisfies the condition or the input is exhausted.
 */
public class SelectOperator extends Operator {
    private Operator child;
    private Expression selectionCondition;
    // Map for resolving column references to tuple fields. Keys are in the form "TableName.ColumnName".
    private Map<String, Integer> schemaMapping;

    /**
     * Constructs a {@code SelectOperator} with the specified child operator, selection condition, and schema mapping.
     * This constructor initializes the select operator by setting its child operator, the condition for
     * tuple selection, and the schema mapping required to resolve column references within expressions.
     *
     * @param child               The child {@link Operator} providing input tuples (e.g., an instance of {@link ScanOperator}).
     * @param selectionCondition  The {@link Expression} representing the selection condition from the WHERE clause.
     * @param schemaMapping       A {@link Map} that associates qualified column names (e.g., "Student.sid") with their respective field indices in tuples.
     *
     * @throws IllegalArgumentException if {@code child}, {@code selectionCondition}, or {@code schemaMapping} is {@code null}.
     */
    public SelectOperator(Operator child, Expression selectionCondition, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.selectionCondition = selectionCondition;
        this.schemaMapping = schemaMapping;
    }

    /**
     * Retrieves the next tuple from the child operator that satisfies the selection condition.
     * This method iteratively fetches tuples from the child operator and evaluates each one against
     * the selection condition using an {@link ExpressionEvaluator}. If a tuple satisfies the condition,
     * it is returned; otherwise, the method continues to the next tuple. The process repeats until a
     * satisfying tuple is found or the end of the input is reached.
     *
     * @return A {@link Tuple} that satisfies the selection condition, or {@code null} if no such tuple exists.
     *
     * @throws RuntimeException if an error occurs during expression evaluation.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            ExpressionEvaluator evaluator = new ExpressionEvaluator(tuple, schemaMapping);
            try {
                boolean conditionHolds = evaluator.evaluate(selectionCondition);
                //System.out.println("Tuple: " + tuple + " -> Condition holds: " + conditionHolds);
                System.out.flush();
                if (conditionHolds) {
                    return tuple;
                }
            } catch (Exception e) {
                System.err.println("Error evaluating expression on tuple " + tuple + ": " + e.getMessage());
            }
        }
        return null;
    }


    /**
     * Resets the {@code SelectOperator} and its child operator to their initial states.
     * This method allows the selection operator to be reused by resetting its child operator. After a reset,
     * tuple retrieval will start from the beginning of the child operator's data stream, enabling
     * re-execution or iteration over the filtered data.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     *
     * @implSpec Implementations should ensure that all internal states are appropriately reinitialized.
     */
    @Override
    public void reset() {
        child.reset();
    }
}
