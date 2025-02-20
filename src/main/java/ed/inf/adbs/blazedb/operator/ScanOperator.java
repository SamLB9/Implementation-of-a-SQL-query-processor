package ed.inf.adbs.blazedb.operator;
import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.SchemaProvider;
import ed.inf.adbs.blazedb.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


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
     * Constructs a ScanOperator to scan the table with the given name.
     *
     * @param tableName The name of the table to scan.
     */
    public ScanOperator(String tableName, boolean hasHeader) {
        this.tableName = tableName;
        this.hasHeader = hasHeader;
        this.catalog = Catalog.getInstance();
        this.filePath = catalog.getFilePathForTable(tableName);
        openFileScan();
    }

    /**
     * Opens a file reader for the table file.
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
     * Creates and returns a BufferedReader for the given file path.
     *
     * @param path The file path to open.
     * @return a BufferedReader for the file.
     * @throws IOException if an I/O error occurs.
     */
    private BufferedReader createBufferedReader(String path) throws IOException {
        return new BufferedReader(new FileReader(new File(path)));
    }

    /**
    * Reads the next line from the file and returns it as a Tuple.
    * @return the next Tuple or null if end of file is reached.
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
     * Resets the operator to the beginning of the file by closing and reopening the file reader.
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

    public void setPrunedSchema(Set<String> schema) {
        this.prunedSchema = schema;
    }

    @Override
    public List<String> getOutputColumns() {
        // Convert the set to a list. Adjust if your ordering is important.
        return new ArrayList<>(prunedSchema);
    }

    /**
     * Reads the next tuple and returns a tuple containing only the values of the specified column.
     * This is the single-column version.
     *
     * @param columnName The name of the column to project.
     * @return a Tuple with the projected field, or null if no more tuples.
     */
    public Tuple getNextProjectedTuple(String columnName) {
        Tuple tuple = getNextTuple();
        if (tuple == null) {
            return null;
        }
        List<String> fullFields = tuple.getFields();
        List<String> projectedFields = new ArrayList<>();
        Integer index = getColumnIndex(columnName);
        if (index != null && index < fullFields.size()) {
            projectedFields.add(fullFields.get(index));
        } else {
            // Optionally: throw an exception or log an error if column not found.
            projectedFields.add("");
        }
        return new Tuple(projectedFields);
    }


    /**
     * Reads the next tuple and returns a tuple containing only the values of the specified columns.
     *
     * @param columnNames An array of column names to project.
     * @return a new Tuple containing only the projected values in the order of columnNames,
     *         or null if no more tuples.
     */
    public Tuple getNextProjectedTuple(String[] columnNames) {
        Tuple tuple = getNextTuple();
        if (tuple == null) {
            return null;
        }
        List<String> fullFields = tuple.getFields();
        List<String> projectedFields = new ArrayList<>();
        for (String col : columnNames) {
            Integer index = getColumnIndex(col);
            if (index != null && index < fullFields.size()) {
                projectedFields.add(fullFields.get(index));
            } else {
                projectedFields.add("");
            }
        }
        return new Tuple(projectedFields);
    }


    /**
     * Returns the index corresponding to the given column name.
     * This sample mapping assumes "A" is index 0, "B" is index 1, "C" is index 2, etc.
     *
     * @param columnName the name of the column.
     * @return the index of the column.
     */
    private Integer getColumnIndex(String columnName) {
        Integer index = schemaMapping.get(columnName);
        if (index == null) {
            throw new IllegalArgumentException("Column " + columnName + " not found in dynamic schema mapping for table " + tableName);
        }
        return index;
    }
}


