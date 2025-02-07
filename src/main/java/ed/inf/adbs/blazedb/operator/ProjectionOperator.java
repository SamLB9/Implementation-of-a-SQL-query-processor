// Java
package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

public class ProjectionOperator extends Operator {

    private Operator child;
    private String[] projectionColumns;

    // In a real application, this mapping should be created based on the actual table schema.
    // Here we assume a default mapping where, for example, column "A" is at index 0, "B" is at index 1,
    // "C" is at index 2, "D" is at index 3, etc.
    private static final Map<String, Integer> defaultMapping;
    static {
        defaultMapping = new HashMap<>();
        defaultMapping.put("A", 0);
        defaultMapping.put("B", 1);
        defaultMapping.put("C", 2);
        defaultMapping.put("D", 3);
        // Add additional mappings as needed.
    }

    /**
     * Constructs a ProjectionOperator.
     *
     * @param child The underlying operator that produces full tuples.
     * @param projectionColumns An array of column names to project. For example: {"D", "B", "A"}.
     */
    public ProjectionOperator(Operator child, String[] projectionColumns) {
        this.child = child;
        this.projectionColumns = projectionColumns;
    }

    /**
     * Retrieves the next projected tuple.
     *
     * @return A new Tuple with only the projected fields, or null if no more tuples exist.
     */
    @Override
    public Tuple getNextTuple() {
        // Retrieve the next full tuple from the child operator.
        Tuple fullTuple = child.getNextTuple();
        if (fullTuple == null) {
            return null;
        }

        List<String> fullFields = fullTuple.getFields();
        List<String> projectedFields = new ArrayList<>();

        // For each column specified in the projection, look up the index from the default mapping,
        // then add the corresponding field.
        for (String col : projectionColumns) {
            Integer index = defaultMapping.get(col);
            if (index == null || index >= fullFields.size()) {
                // In a real use case you might want to throw an exception here.
                projectedFields.add("");
            } else {
                projectedFields.add(fullFields.get(index));
            }
        }

        return new Tuple(projectedFields);
    }

    /**
     * Resets the operator by resetting its child operator.
     */
    @Override
    public void reset() {
        child.reset();
    }
}