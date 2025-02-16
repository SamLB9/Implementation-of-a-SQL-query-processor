package ed.inf.adbs.blazedb.result;

import ed.inf.adbs.blazedb.operator.Operator;

import java.util.Map;

/**
 * A helper class to encapsulate projection processing results.
 */
public class ProjectionResult {
    private Operator rootOperator;
    private Map<String, Integer> schemaMapping;

    public ProjectionResult(Operator rootOperator, Map<String, Integer> schemaMapping) {
        this.rootOperator = rootOperator;
        this.schemaMapping = schemaMapping;
    }

    public Operator getRootOperator() {
        return rootOperator;
    }

    public Map<String, Integer> getSchemaMapping() {
        return schemaMapping;
    }
}
