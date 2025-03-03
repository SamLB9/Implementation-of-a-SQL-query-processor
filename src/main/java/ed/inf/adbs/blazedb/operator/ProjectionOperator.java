package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The {@code ProjectionOperator} class implements the projection operation within the BlazeDB framework.
 * It is responsible for selecting a subset of columns from tuples produced by its child operator,
 * effectively reducing the number of fields in each tuple to only those specified in the projection.
 *
 * Key Features:
 *  - Column Selection: Allows the selection of specific columns from input tuples based on
 *         a predefined list of projection columns.
 *  - Duplicate Elimination: Removes duplicate columns in the projection list while maintaining
 *         the original order of columns.
 *  - Schema Mapping: Utilizes a schema mapping to accurately identify and retrieve the
 *         corresponding fields from input tuples.
 *  - Efficiency: Pre-calculates the unique projection columns to optimize the projection process.
 *  - State Management: Supports resetting of the operator's state, allowing for re-execution
 *         or iteration over the projected data.
 */
public class ProjectionOperator extends Operator {
    private Operator child;
    private String[] projectionColumns;
    private Map<String, Integer> schemaMapping;

    // Pre-calculate the unique projection column order.
    private String[] uniqueProjectionColumns;

    public ProjectionOperator(Operator child, String[] projectionColumns, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.projectionColumns = projectionColumns;
        this.schemaMapping = schemaMapping;
        this.uniqueProjectionColumns = removeDuplicateColumns(projectionColumns);
    }

    /**
     * Removes duplicate columns from the given array while maintaining the original order.
     * This method ensures that each column in the projection list is unique, preventing redundant
     * data in the projected tuples.
     *
     * @param columns An array of column names that may contain duplicates.
     * @return A new array of column names with duplicates removed, preserving the original order.
     */
    private String[] removeDuplicateColumns(String[] columns) {
        Set<String> unique = new LinkedHashSet<>();
        for (String col : columns) {
            unique.add(col);
        }
        return unique.toArray(new String[0]);
    }

    /**
     * Retrieves the next projected {@link Tuple} from the operator's data stream.
     * This method fetches the next tuple from the child operator, extracts the specified projection
     * columns based on the schema mapping, and returns a new tuple containing only those columns.
     * If the input tuple already matches the projection criteria, it is returned as-is to avoid
     * redundant processing.
     *
     * @return A {@link Tuple} object containing only the projected columns, or {@code null} if
     *         there are no more tuples to retrieve.
     *
     * @throws RuntimeException if an error occurs during tuple retrieval or projection.
     *
     * @implSpec Implementations should ensure that the projection is performed efficiently,
     *           especially when dealing with large datasets or complex schemas.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple fullTuple = child.getNextTuple();
        if (fullTuple == null) {
            return null;
        }

        // If the tuple is already projected, we assume it has been done.
        if(fullTuple.getFields().size() == uniqueProjectionColumns.length) {
            return fullTuple;
        }

        // Build the projected tuple using the unique projection columns.
        List<String> fullFields = fullTuple.getFields();
        List<String> projectedFields = new ArrayList<>();

        for (String col : uniqueProjectionColumns) {
            Integer index = schemaMapping.get(col);
            if (index == null || index >= fullFields.size()) {
                projectedFields.add("");
            } else {
                projectedFields.add(fullFields.get(index));
            }
        }

        return new Tuple(projectedFields);
    }

    /**
     * Resets the {@code ProjectionOperator} and its child operator to their initial states.
     *
     * This method allows the projection operator to be reused by clearing any internal state
     * and resetting the child operator. After a reset, tuple retrieval will start from the beginning
     * of the child operator's data stream.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     *
     * @implSpec Implementations should ensure that all internal buffers and stateful components
     *           are appropriately reinitialized.
     */
    @Override
    public void reset() {
        child.reset();
    }
}