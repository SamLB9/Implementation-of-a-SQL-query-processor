package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@code DuplicateEliminationOperator} class is responsible for removing duplicate tuples
 * from the data stream provided by its child operator. It extends the abstract {@link Operator}
 * class within the BlazeDB framework, utilizing each tuple's string representation to identify
 * and eliminate duplicates efficiently.
 *
 * This operator is essential in query execution plans where duplicate records need to be
 * filtered out to ensure the correctness and integrity of the resulting dataset. By leveraging
 * the {@code toString()} method of the {@link Tuple} class, it uniquely identifies tuples that
 * have already been processed, thereby preventing redundant data from propagating further
 * through the operator pipeline.
 *
 * Key Features:
 *  - Duplicate Detection: Utilizes a {@link Set} to track and identify unique tuples
 *         based on their string representations.
 *  - Child Operator Integration: Works in conjunction with a child operator to
 *         seamlessly eliminate duplicates from the data stream.
 *  - Reset Capability: Provides the ability to reset its state, allowing for
 *         reusability in different query contexts without residual data from previous executions.
 *
 *
 * Implementation Details:
 * The class maintains an internal {@code HashSet<String>} called {@code seenTuples} to store
 * the string representations of tuples that have already been encountered.
 * During iteration, each incoming tuple from the child operator is converted to a string and
 * checked against {@code seenTuples}. If it is not present, it's added to the set and returned;
 * otherwise, it's skipped to eliminate the duplicate.
 * The {@code reset()} method clears the {@code seenTuples} set and resets the child operator,
 * allowing the elimination process to start fresh for subsequent queries.
 */
public class DuplicateEliminationOperator extends Operator {

    private final Operator child;
    private final Set<String> seenTuples;


    /**
     * Constructs a {@code DuplicateEliminationOperator} with the specified child operator.
     *
     * @param child The child {@link Operator} from which tuples are retrieved.
     *              Must not be {@code null}.
     *
     * @throws IllegalArgumentException if {@code child} is {@code null}.
     */
    public DuplicateEliminationOperator(Operator child) {
        this.child = child;
        this.seenTuples = new HashSet<>();
    }

    /**
     * Retrieves the next unique tuple from the child operator.
     *
     * This method iteratively fetches tuples from the child operator and checks whether
     * each tuple has been seen before by examining its string representation. If the tuple
     * is unique, it is returned; otherwise, it is skipped to prevent duplicate entries.
     *
     * @return The next distinct {@link Tuple}, or {@code null} if no more tuples are available.
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
     * Resets the {@code DuplicateEliminationOperator} to its initial state.
     *
     * This method clears the internal cache of seen tuples and resets the child operator,
     * enabling the operator to be reused for processing a new set of tuples without retaining
     * any information from previous executions.
     */

    @Override
    public void reset() {
        child.reset();
        seenTuples.clear();
    }
}