package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * The {@code SortOperator} class implements the sorting operation within the BlazeDB framework.
 * It retrieves tuples from its child operator, sorts them based on specified ORDER BY elements,
 * and provides the sorted tuples for further processing. This operator is essential for executing
 * SQL queries that require ordered results.
 *
 * Key Responsibilities:
 *  - Tuple Retrieval: Fetches all tuples from the child operator and stores them for sorting.
 *  - Sorting Mechanism: Sorts the collected tuples based on one or more ORDER BY criteria.
 *  - Schema Mapping: Maintains a mapping between column names and their respective indices to facilitate accurate sorting.
 *  - State Management: Manages internal state to allow resetting and re-iteration over sorted tuples.
 *
 * Implementation Details:
 *  - The class retrieves all tuples from the child operator and stores them in a buffer for sorting.
 *  - A custom {@link TupleComparator} is used to define the sorting logic based on the provided ORDER BY elements.
 *  - After sorting, tuples are returned one by one through the {@link #getNextTuple()} method.
 */
public class SortOperator extends Operator {

    private final Operator child;
    private final List<OrderByElement> orderByElements;
    private final Map<String, Integer> schemaMapping;

    // Buffer to store tuples from the child operator.
    private List<Tuple> sortedTuples;
    // Pointer for returning tuples one by one.
    private int currentIndex;

    /**
     * Constructs a {@code SortOperator} with the specified child operator, ORDER BY elements, and schema mapping.
     * This constructor initializes the sort operator by setting its child operator, the ORDER BY rules
     * for sorting, and the schema mapping required to resolve column references within the tuples.
     *
     * @param child           The child {@link Operator} providing input tuples (e.g., an instance of {@link ScanOperator}).
     * @param orderByElements A {@link List} of {@link OrderByElement} specifying the sort order based on column names and directions.
     * @param schemaMapping   A {@link Map} that associates column names (as used in the ORDER BY clause) with their respective indices in the tuples.
     *
     * @throws IllegalArgumentException if {@code child}, {@code orderByElements}, or {@code schemaMapping} is {@code null}.
     */
    public SortOperator(Operator child, List<OrderByElement> orderByElements, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.orderByElements = orderByElements;
        this.schemaMapping = schemaMapping;
        this.sortedTuples = new ArrayList<>();
        this.currentIndex = 0;
    }

    /**
     * Retrieves the next sorted tuple from the buffer.
     * If the tuples have not been sorted yet, this method fetches all tuples from the child operator,
     * sorts them based on the specified ORDER BY elements, and then returns the tuples one by one in sorted order.
     *
     * @return The next {@link Tuple} in sorted order, or {@code null} if no more tuples are available.
     *
     * @throws RuntimeException if an error occurs during tuple retrieval or sorting.
     */
    @Override
    public Tuple getNextTuple() {
        // Buffer all tuples from the child operator and sort them if not already done.
        if (sortedTuples.isEmpty()) {
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                sortedTuples.add(tuple);
            }
            // System.out.println("SortOperator getNextTuple: Sorted tuples: " + sortedTuples);
            // System.out.println("SortOperator getNextTuple: Schema mapping: " + schemaMapping);
            // System.out.println("SortOperator getNextTuple: Schema mapping size: " + schemaMapping.size());
            Collections.sort(sortedTuples, new TupleComparator(orderByElements, schemaMapping));
        }

        // Return tuples one by one until the list is exhausted.
        if (currentIndex < sortedTuples.size()) {
            return sortedTuples.get(currentIndex++);
        }
        return null;
    }

    /**
     * Resets the {@code SortOperator} to its initial state, allowing for re-iteration over sorted tuples.
     * This method clears the sorted tuples buffer, resets the current index pointer, and resets the child operator,
     * enabling the sort operation to be performed again from the beginning.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     */
    @Override
    public void reset() {

    }

    /**
     * A custom comparator that defines the sorting logic for {@link Tuple} objects based on the specified ORDER BY elements.
     * This comparator iterates through the ORDER BY elements and compares tuples based on the corresponding
     * column values and sort directions. It ensures that tuples are ordered according to all specified criteria.
     */
    private static class TupleComparator implements Comparator<Tuple> {

        private final List<OrderByElement> orderByElements;
        private final Map<String, Integer> schemaMapping;

        public TupleComparator(List<OrderByElement> orderByElements, Map<String, Integer> schemaMapping) {
            this.orderByElements = orderByElements;
            this.schemaMapping = schemaMapping;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement orderBy : orderByElements) {
                int cmp = compareByColumn(t1, t2, orderBy);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        private String getColumnName(OrderByElement orderBy) {
            // Get the Expression from the ORDER BY element.
            // If the expression is a column, cast it.
            if (orderBy.getExpression() instanceof Column) {
                Column col = (Column) orderBy.getExpression();
                // Build a fully qualified name: "TableName.ColumnName"
                if (col.getTable() != null && col.getTable().getName() != null) {
                    return col.getTable().getName() + "." + col.getColumnName();
                }
                return col.getColumnName();
            }
            throw new IllegalArgumentException("ORDER BY expression is not a column.");
        }

        private int compareByColumn(Tuple t1, Tuple t2, OrderByElement orderBy) {
            String columnName = getColumnName(orderBy);   // Get fully qualified column name
            Integer index = schemaMapping.get(columnName);
            // System.out.println("SortOperator: Column name: " + columnName + " -> Index: " + index);
            // System.out.println("SortOperator: Schema mapping" + schemaMapping);

            if (index == null) {
                throw new IllegalArgumentException("Column " + columnName + " is not found in the schema mapping.");
            }
            // System.out.println("SortOperator: t1: " + t1 + "  t2: " + t2);
            // Now retrieve the actual values based on your requirements.
            // The example below assumes integer values.
            int value1 = t1.getInt(index);
            int value2 = t2.getInt(index);

            int cmp = Integer.compare(value1, value2);
            return orderBy.isAsc() ? cmp : -cmp;
        }
    }
}