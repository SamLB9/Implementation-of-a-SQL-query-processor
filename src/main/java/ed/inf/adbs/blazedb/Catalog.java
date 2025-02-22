package ed.inf.adbs.blazedb;

import java.io.File;

public class Catalog {
    // Singleton instance
    private static Catalog instance = null;

    // Base directory where all table files are stored.
    private final String baseDir;

    private Catalog() {
        // The base directory
        this.baseDir = "samples/db/data/";
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
     * It dynamically constructs the path with the assumption that each table
     * is stored as a CSV file in the base directory.
     *
     * @param tableName name of the table whose file path is to be retrieved.
     * @return file path as a String if the file exists, otherwise null.
     */
    public String getFilePathForTable(String tableName) {
        String filePath = baseDir + tableName + ".csv";
        File file = new File(filePath);
        if (file.exists()) {
            return filePath;
        } else {
            System.err.println("Table file not found for table: " + tableName);
            return null;
        }
    }

    /**
     * Adds or updates a table entry in the catalog.
     * With dynamic file path resolution, this method can be used to override
     * the default file location if needed.
     *
     * @param tableName The name of the table.
     * @param filePath The file path where the table data is stored.
     */
    public void addTable(String tableName, String filePath) {
        // This method can be implemented to override the default file path resolution
        // For now, it could simply log a warning that dynamic file lookup is being used.
        System.out.println("Warning: Overriding default file lookup. New file path for table "
                + tableName + " is " + filePath);
        // If you wish to support this behavior, you can add an internal map for overrides.
    }
}