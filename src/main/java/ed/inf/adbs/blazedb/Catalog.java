package ed.inf.adbs.blazedb;

import java.util.HashMap;
import java.util.Map;


public class Catalog {
    // Singleton instance
    private static Catalog instance = null;
    // Mapping of table names to file paths
    private Map<String, String> tableFilePaths;


    // Private constructor prevents instantiation from other classes
    private Catalog() {
        tableFilePaths = new HashMap<>();
        // Initialize with table information.
        // For example:
        tableFilePaths.put("Student", "samples/db/data/Student.csv");
        tableFilePaths.put("Course", "samples/db/data/Course.csv");
        tableFilePaths.put("Enrolled", "samples/db/data/Enrolled.csv");
    }


    /**
     * Retrieves the single instance of Catalog.
     * @return The Catalog instance.
     */
    public static Catalog getInstance() {
        if (instance == null) {
            instance = new Catalog();
        }
        return instance;
    }


    /**
     * Returns the file path associated with the given table name.
     *
     * @param tableName name of the table whose file path is to be retrieved.
     * @return file path as a String, or null if the table name is not found.
     */
    public String getFilePathForTable(String tableName) {
        return tableFilePaths.get(tableName);
    }


    /**
     * Adds or updates a table entry in the catalog.
     *
     * @param tableName The name of the table.
     * @param filePath The file path where the table data is stored.
     */
    public void addTable(String tableName, String filePath) {
        tableFilePaths.put(tableName, filePath);
    }
}