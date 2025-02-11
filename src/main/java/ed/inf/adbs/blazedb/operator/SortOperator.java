package ed.inf.adbs.blazedb.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends Operator {

    private final Operator child;
    private final List<OrderByElement> orderByElements;
    private final Map<String, Integer> schemaMapping;

    // Buffer to store tuples from the child operator.
    private List<Tuple> sortedTuples;
    // Pointer for returning tuples one by one.
    private int currentIndex;

    /**
     * Constructor for the SortOperator.
     *
     * @param child           the child operator to fetch tuples from.
     * @param orderByElements a list of ORDER BY elements specifying sort order.
     * @param schemaMapping   a mapping from column names (as used in the ORDER BY clause)
     *                        to the corresponding position in the tuple.
     */
    public SortOperator(Operator child, List<OrderByElement> orderByElements, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.orderByElements = orderByElements;
        this.schemaMapping = schemaMapping;
        this.sortedTuples = new ArrayList<>();
        this.currentIndex = 0;
    }

    @Override
    public Tuple getNextTuple() {
        // Buffer all tuples from the child operator and sort them if not already done.
        if (sortedTuples.isEmpty()) {
            Tuple tuple;
            while ((tuple = child.getNextTuple()) != null) {
                sortedTuples.add(tuple);
            }
            Collections.sort(sortedTuples, new TupleComparator(orderByElements, schemaMapping));
        }

        // Return tuples one by one until the list is exhausted.
        if (currentIndex < sortedTuples.size()) {
            return sortedTuples.get(currentIndex++);
        }
        return null;
    }

    @Override
    public void reset() {

    }

    /**
     * A custom comparator that compares two tuples based on the ORDER BY elements.
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

            if (index == null) {
                throw new IllegalArgumentException("Column " + columnName + " is not found in the schema mapping.");
            }

            // Now retrieve the actual values based on your requirements.
            // The example below assumes integer values.
            int value1 = t1.getInt(index);
            int value2 = t2.getInt(index);

            int cmp = Integer.compare(value1, value2);
            return orderBy.isAsc() ? cmp : -cmp;
        }
    }
}