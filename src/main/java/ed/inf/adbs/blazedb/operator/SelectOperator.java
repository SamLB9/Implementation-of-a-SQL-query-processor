package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.expression.ExpressionEvaluator;

import java.util.Map;


public class SelectOperator extends Operator {
    private Operator child;
    private Expression selectionCondition;
    // Map for resolving column references to tuple fields. Keys are in the form "TableName.ColumnName".
    private Map<String, Integer> schemaMapping;

    /**
     * Constructs a SelectOperator.
     *
     * @param child The child operator (for example, an instance of ScanOperator).
     * @param selectionCondition The selection condition from the WHERE clause.
     * @param schemaMapping A mapping from qualified column names (e.g., "Student.sid") to field indexes.
     */
    public SelectOperator(Operator child, Expression selectionCondition, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.selectionCondition = selectionCondition;
        this.schemaMapping = schemaMapping;
    }

    /**
     * Retrieves the next tuple from the child operator that satisfies
     * the selection condition.
     *
     * @return a Tuple if one satisfies the selection, otherwise null.
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
     * Resets the operator to the beginning by resetting its child.
     */
    @Override
    public void reset() {
        child.reset();
    }
}
