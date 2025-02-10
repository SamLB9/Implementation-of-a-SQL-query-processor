package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.HashSet;
import java.util.Set;

/**
 * DuplicateEliminationOperator removes duplicate tuples from its child operator.
 * It extends the abstract Operator class (as defined in BlazeDB) and relies on the tuple's
 * toString() method to check for duplicate occurrences.
 */
public class DuplicateEliminationOperator extends Operator {

    private final Operator child;
    private final Set<String> seenTuples;

    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        this.seenTuples = new HashSet<>();
    }

    /**
     * Returns the next distinct tuple from the child operator.
     * It uses the tuple's string representation (via toString()) to determine if a tuple
     * has already been seen. If so, it skips over duplicates.
     *
     * @return the next unique tuple, or null if no more tuples are available.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            // Use tuple.toString() as a unique signature for duplicate detection.
            if (seenTuples.add(tuple.toString())) {
                return tuple;
            }
        }
        return null;
    }

    /**
     * Resets the DuplicateEliminationOperator.
     * This method resets the child operator and clears the internal cache of seen tuples,
     * allowing the operator to be re-used.
     */
    @Override
    public void reset() {
        child.reset();
        seenTuples.clear();
    }
}