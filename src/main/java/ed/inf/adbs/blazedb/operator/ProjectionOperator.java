package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;

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
     * Removes duplicate columns while maintaining order.
     */
    private String[] removeDuplicateColumns(String[] columns) {
        Set<String> unique = new LinkedHashSet<>();
        for (String col : columns) {
            unique.add(col);
        }
        return unique.toArray(new String[0]);
    }

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

    @Override
    public void reset() {
        child.reset();
    }
}