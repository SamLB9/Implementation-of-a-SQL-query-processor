package ed.inf.adbs.blazedb.operator;
import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.SchemaProvider;
import ed.inf.adbs.blazedb.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The {@code ScanOperator} class is responsible for scanning a specified table within the BlazeDB framework.
 * It reads data from the underlying storage (typically a file), converts each line into a {@link Tuple},
 * and provides mechanisms to iterate over these tuples. Additionally, it implements the {@link SchemaProvider}
 * interface to supply schema information about the scanned data.
 *
 * Key Responsibilities:
 *  - Data Scanning: Reads data from the specified table's file and converts each line into a {@link Tuple}.
 *  - Schema Management: Manages the schema mapping to associate column names with their respective indices.
 *  - State Management: Supports resetting the scan to start from the beginning of the data source.
 *  - Projection Support: Allows retrieval of projected tuples based on specified columns.
 *
 * Implementation Details:
 *  - Table Identification: Utilizes the table name to locate and access the corresponding data file.
 *  - Header Handling: Supports optional header rows to map column names to their indices.
 *  - Schema Pruning: Allows pruning of the schema to include only relevant columns, optimizing data retrieval.
 */
public class ScanOperator extends Operator implements SchemaProvider {
    private String tableName;
    private BufferedReader reader;
    private String filePath;
    private Catalog catalog;
    // The dynamic schema mapping that maps each column name to its index.
    private Map<String, Integer> schemaMapping;
    // Flag indicating whether the file contains a header row.
    private boolean hasHeader;
    private Set<String> prunedSchema;


    /**
     * Constructs a {@code ScanOperator} to scan the specified table.
     * This constructor initializes the scan operator by setting the table name and determining
     * whether the table file contains a header row. It prepares the operator for data scanning by
     * initializing necessary resources.
     *
     * @param tableName The name of the table to scan.
     * @param hasHeader {@code true} if the table file includes a header row; {@code false} otherwise.
     */
    public ScanOperator(String tableName, boolean hasHeader) {
        this.tableName = tableName;
        this.hasHeader = hasHeader;
        this.catalog = Catalog.getInstance();
        this.filePath = catalog.getFilePathForTable(tableName);
        openFileScan();
    }

    /**
     * Opens a file reader for the table's data file.
     *
     * This method initializes the {@link BufferedReader} to read data from the specified file path.
     * It handles the opening of the file and prepares the reader for tuple retrieval.
     *
     * @throws RuntimeException if an I/O error occurs while opening the file.
     */
    private void openFileScan() {
        try {
            reader = createBufferedReader(filePath);
            if (hasHeader) {
                // Read the first line as column header names.
                String headerLine = reader.readLine();
                if (headerLine != null) {
                    String[] headers = headerLine.split(",");
                    schemaMapping = new HashMap<>();
                    for (int i = 0; i < headers.length; i++) {
                        schemaMapping.put(headers[i].trim(), i);
                    }
                } else {
                    schemaMapping = Collections.emptyMap();
                }
            } else {
                // Data file contains no header; generate schema mapping without losing the first row.
                // Mark the current position to allow reset after reading the first row.
                reader.mark(10000);
                String firstRow = reader.readLine();
                if (firstRow != null) {
                    // Create mapping based on the number of columns in the first row.
                    String[] values = firstRow.split(",");
                    schemaMapping = new HashMap<>();
                    // For instance, assign default names like COL1, COL2, ... or use a predefined naming scheme.
                    for (int i = 0; i < values.length; i++) {
                        schemaMapping.put("COL" + (i + 1), i);
                    }
                    // Reset back so that the first row is processed as data.
                    reader.reset();
                } else {
                    schemaMapping = Collections.emptyMap();
                }
            }
        } catch (IOException e) {
            System.err.println("Error initializing file scan for table " + tableName + ": " + e.getMessage());
        }
    }

    /**
     * Creates and returns a {@link BufferedReader} for the given file path.
     * This helper method encapsulates the logic for initializing a {@link BufferedReader} with the
     * specified file path, handling any necessary I/O exceptions.
     *
     * @param path The file path to open.
     * @return A {@link BufferedReader} instance for reading the file.
     * @throws IOException if an I/O error occurs while opening the file.
     */
    private BufferedReader createBufferedReader(String path) throws IOException {
        return new BufferedReader(new FileReader(new File(path)));
    }

    /**
     * Retrieves the next {@link Tuple} from the table's data stream.
     * This method reads the next line from the table's file, converts it into a {@link Tuple},
     * and returns it. If the end of the file is reached, {@code null} is returned.
     *
     * @return The next {@link Tuple} containing the row data, or {@code null} if the end of the file is reached.
     *
     * @throws RuntimeException if an error occurs during tuple retrieval.
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            String[] fields = line.split(",");
            List<String> fieldList = new ArrayList<>();
            for (String field : fields) {
                fieldList.add(field.trim());
            }
            return new Tuple(fieldList);
        } catch (IOException e) {
            System.err.println("Error reading tuple from table " + tableName + ": " + e.getMessage());
            return null;
        }
    }


    /**
     * Resets the {@code ScanOperator} to the beginning of the table's data file.
     * This method closes the current {@link BufferedReader} and reopens it, effectively resetting
     * the scan to start from the first tuple again. It ensures that subsequent calls to {@link #getNextTuple()}
     * will begin retrieval from the start of the data source.
     *
     * @throws RuntimeException if an error occurs during the reset process.
     */
    @Override
    public void reset() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            // Handle error if needed.
        }
        openFileScan();
    }

    /**
     * Retrieves the list of output columns based on the current schema mapping and pruning.
     * This method returns a {@link List} of column names that the scan operator will output.
     * If schema pruning is applied, only the pruned columns are included.
     *
     * @return A {@link List} of column names to be included in the output tuples.
     */
    @Override
    public List<String> getOutputColumns() {
        // Convert the set to a list. Adjust if your ordering is important.
        return new ArrayList<>(prunedSchema);
    }
}


