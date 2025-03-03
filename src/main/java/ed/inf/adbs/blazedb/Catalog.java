package ed.inf.adbs.blazedb;

import java.io.File;

/**
 * The {@code Catalog} class serves as a centralized repository for managing table metadata within the BlazeDB system.
 * Implemented as a Singleton, it ensures that only one instance of the catalog exists throughout the application's lifecycle.
 * The catalog is responsible for maintaining mappings between table names and their corresponding file paths,
 * facilitating efficient data retrieval and management.
 *
 * Key Responsibilities:
 *  - Singleton Enforcement: Guarantees a single instance of the catalog, providing a global point of access.
 *  - File Path Management: Maintains and resolves file paths for tables, allowing dynamic lookup and overriding.
 *  - Metadata Storage: Stores essential metadata about tables, such as their storage locations.
 */
public class Catalog {
    // Singleton instance
    private static Catalog instance = null;

    // Base directory where all table files are stored.
    private final String baseDir;

    /**
     * Private constructor to enforce Singleton pattern.
     * Initializes the base directory for table storage.
     */
    private Catalog() {
        // The base directory
        this.baseDir = "samples/db/data/";
    }

    /**
     * Retrieves the single instance of {@code Catalog}.
     * If the instance does not exist, it initializes a new one.
     * This method ensures that only one instance of the catalog is created and provides global access to it.
     *
     * @return The singleton {@code Catalog} instance.
     */
    public static Catalog getInstance() {
        if (instance == null) {
            instance = new Catalog();
        }
        return instance;
    }

    /**
     * Returns the file path associated with the given table name.
     * It dynamically constructs the path with the assumption that each table is stored as a CSV file in the base directory.
     * If the specified table file exists in the base directory, the method returns its absolute path.
     * Otherwise, it logs an error message and returns {@code null}.
     *
     * @param tableName The name of the table whose file path is to be retrieved.
     * @return The file path as a {@code String} if the file exists; {@code null} otherwise.
     *
     * @throws IllegalArgumentException if {@code tableName} is {@code null} or empty.
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
}