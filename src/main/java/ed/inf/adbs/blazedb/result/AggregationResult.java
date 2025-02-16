package ed.inf.adbs.blazedb.result;

import net.sf.jsqlparser.expression.Expression;
import java.util.List;
import java.util.Map;

public class AggregationResult {
    private List<Expression> sumExpressions;
    private boolean hasAggregation;
    private Map<Expression, String> literalSumMapping;

    public AggregationResult(List<Expression> sumExpressions, boolean hasAggregation, Map<Expression, String> literalSumMapping) {
        this.sumExpressions = sumExpressions;
        this.hasAggregation = hasAggregation;
        this.literalSumMapping = literalSumMapping;
    }

    public List<Expression> getSumExpressions() {
        return sumExpressions;
    }

    public boolean hasAggregation() {
        return hasAggregation;
    }

    public Map<Expression, String> getLiteralSumMapping() {
        return literalSumMapping;
    }
}

