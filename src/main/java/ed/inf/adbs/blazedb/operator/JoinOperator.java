package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.expression.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;


/**
 * The JoinOperator implements a tuple-nested-loop join. It takes two child operators:
 * a left (outer) operator and a right (inner) operator. It also accepts an optional join condition.
 *
 * The join algorithm operates as follows:
 * 1. For each tuple from the left (outer) operator, reset and scan the entire right (inner) operator.
 * 2. For every (left, right) pair, combine the fields from both tuples into a new joined tuple.
 * 3. If a join condition was provided, evaluate the combined tuple using an ExpressionEvaluator.
 *    Only return the tuple if it satisfies the join condition.
 * 4. If the join condition is null, then return every joined tuple.
 *
 * Note about the extraction of join conditions:
 * To avoid computing cross products unnecessarily, the query planner is assumed to inspect the WHERE clause:
 * - Conditions that reference only one table (selections) are applied as early as possible (e.g., at Scan/Select operators).
 * - Conditions that reference columns from two different tables are extracted as join conditions and attached to this operator.
 * In this way, in a left-deep join tree the joins are performed in the order specified in the FROM clause.
 */
public class JoinOperator extends Operator {
    private Operator left;         // left (outer) child operator
    private Operator right;        // right (inner) child operator
    private Expression joinCondition;  // join condition as an Expression (null for cross join)
    private Map<String, Integer> schemaMapping; // combined schema mapping for the joined tuple
    private Tuple currentLeft;     // current tuple from left operator
    private Queue<Tuple> bufferedTuples = new LinkedList<>();

    /**
     * Constructs a JoinOperator.
     *
     * @param left           The left (outer) operator.
     * @param right          The right (inner) operator.
     * @param joinCondition  The join condition (can be null for a cross join).
     * @param schemaMapping  A combined schema mapping that maps fully qualified column names
     *                       (e.g., "Student.sid" or "Course.cid") to their index in the joined tuple.
     */
    public JoinOperator(Operator left, Operator right, Expression joinCondition, Map<String, Integer> schemaMapping) {
        this.left = left;
        this.right = right;
        this.joinCondition = joinCondition;
        this.schemaMapping = schemaMapping;
        this.currentLeft = null;
    }

    /**
     * Implements the tuple-nested-loop join.
     *
     * @return the next joined Tuple that satisfies the join condition (if provided), or null if no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        // Return already buffered tuples first.
        if (!bufferedTuples.isEmpty()) {
            return bufferedTuples.poll();
        }

        // Process left tuples until we accumulate join pairs.
        while ((currentLeft = left.getNextTuple()) != null) {
            // Optional: Log or debug print the left tuple
            // System.out.println("Processing left tuple: " + currentLeft);

            // Reset the right operator for each new left tuple.
            right.reset();

            Tuple rightTuple;
            while ((rightTuple = right.getNextTuple()) != null) {
                Tuple joinedTuple = combineTuples(currentLeft, rightTuple);
                // System.out.println("Joined tuple: " + joinedTuple);

                // If no join condition, enqueue the tuple.
                if (joinCondition == null) {
                    bufferedTuples.add(joinedTuple);
                } else {
                    ExpressionEvaluator evaluator = new ExpressionEvaluator(joinedTuple, schemaMapping);
                    try {
                        if (evaluator.evaluate(joinCondition)) {
                            bufferedTuples.add(joinedTuple);
                        }
                    } catch (Exception e) {
                        System.err.println("Error evaluating join condition for tuple: " + joinedTuple);
                    }
                }
            }
            if (!bufferedTuples.isEmpty()) {
                // Break out if we have found any join pairs for the current left tuple.
                return bufferedTuples.poll();
            }
        }

        // If no more join pairs exist, return null.
        return null;
    }

    // A helper method to combine two tuples (implement as needed to merge the fields).
    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        List<String> combinedFields = new ArrayList<>(leftTuple.getFields());
        combinedFields.addAll(rightTuple.getFields());
        return new Tuple(combinedFields);
    }

    /**
     * Resets the JoinOperator and its child operators.
     */
    @Override
    public void reset() {
        left.reset();
        right.reset();
        currentLeft = null;
    }
}

