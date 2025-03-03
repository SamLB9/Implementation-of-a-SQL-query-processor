package ed.inf.adbs.blazedb;

import java.util.List;

/**
 * The {@code SchemaProvider} interface defines a contract for classes that provide
 * schema information, specifically the list of output columns required for database operations
 * within the BlazeDB system.
 * Implementing classes are responsible for supplying the necessary column names that
 * constitute the output schema of a query or data retrieval operation. This abstraction
 * allows for flexible and interchangeable schema providers, facilitating varied data sources
 * and query configurations.
 */
public interface SchemaProvider {
    List<String> getOutputColumns();
}
