package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;


/**
 * The {@code Operator} abstract class serves as the foundational component for implementing
 * various relational algebra operations within the BlazeDB framework's iterator model.
 * It defines the essential contract for all operator implementations, facilitating the
 * retrieval and management of tuples during query execution.
 *
 * Key Responsibilities:
 *  - Tuple Retrieval: Provides a standardized method for fetching the next available
 *         tuple from the data source or the result of a preceding operation.
 *  - State Management**: Offers a mechanism to reset the operator's state, allowing
 *         for re-execution or iteration over the data from the beginning.
 *
 * Subclasses of {@code Operator} must provide concrete implementations for the
 * {@link #getNextTuple()} and {@link #reset()} methods, encapsulating the specific logic
 * for different relational operations such as selection, projection, joins, and more.
 */
public abstract class Operator {


    /**
     * Retrieves the next {@link Tuple} from the operator's data stream.
     *
     * @return A {@link Tuple} object representing the next row of data, or {@code null} if
     *         the end of the data stream is reached.
     *
     * @throws RuntimeException if an error occurs during tuple retrieval.
     *
     * @implSpec Subclasses must provide concrete implementations of this method, adhering to
     *           the expected behavior as defined by the operator's semantics.
     */
    public abstract Tuple getNextTuple();


    /**
     * Resets the operator's state, allowing the iteration over its data stream to start
     * from the beginning.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     *
     * @implSpec Subclasses must provide concrete implementations of this method, ensuring that
     *           the operator's state is fully reinitialized and that subsequent calls to
     *           {@link #getNextTuple()} behave as if the operator is being accessed for the first time.
     */
    public abstract void reset();
}