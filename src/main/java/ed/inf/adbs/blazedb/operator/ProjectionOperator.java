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
    private Map<String, Integer> schemaMapping;


    /**
     * Constructs a ProjectionOperator.
     *
     * @param child            The underlying operator that produces full tuples.
     * @param projectionColumns An array of column names to project. For example: {"Student.D", "Student.B", "Student.A"}.
     *                          Note: Ensure these names are compatible with the keys in the dynamic schema mapping.
     * @param schemaMapping    A mapping from fully qualified column names (e.g., "Student.A") to their indexes.
     */
    public ProjectionOperator(Operator child, String[] projectionColumns, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.projectionColumns = projectionColumns;
        this.schemaMapping = schemaMapping;
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

        // For each column specified in the projection, look up the index from the dynamic schema mapping,
        // then add the corresponding field.
        for (String col : projectionColumns) {
            // Lookup the index for the fully qualified column name.
            Integer index = schemaMapping.get(col);
            if (index == null || index >= fullFields.size()) {
                // Optionally handle the case where the column is not found.
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
