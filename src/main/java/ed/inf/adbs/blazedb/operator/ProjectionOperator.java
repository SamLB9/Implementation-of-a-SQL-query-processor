package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class ProjectionOperator extends Operator {
    private Operator child;
    private String[] projectionColumns;
    private Map<String, Integer> schemaMapping;

    public ProjectionOperator(Operator child, String[] projectionColumns, Map<String, Integer> schemaMapping) {
        this.child = child;
        this.projectionColumns = projectionColumns;
        this.schemaMapping = schemaMapping;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple fullTuple = child.getNextTuple();
        if (fullTuple == null) {
            return null;
        }

        List<String> fullFields = fullTuple.getFields();

        // If the tuple is already projected (i.e. its size equals the number of projection columns),
        // we assume that projection has been applied already and return it.
        if(fullFields.size() == projectionColumns.length) {
            return fullTuple;
        }

        // System.out.println("Full Tuple: " + fullFields);
        List<String> projectedFields = new ArrayList<>();
        for (String col : projectionColumns) {
            Integer index = schemaMapping.get(col);
            // System.out.println("Mapping for " + col + " = " + index);
            if (index == null || index >= fullFields.size()) {
                projectedFields.add("");
            } else {
                projectedFields.add(fullFields.get(index));
            }
        }
        // System.out.println("Projected Tuple: " + projectedFields);
        return new Tuple(projectedFields);
    }

    @Override
    public void reset() {
        child.reset();
    }
}
