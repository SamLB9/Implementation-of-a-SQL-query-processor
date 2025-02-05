package ed.inf.adbs.blazedb.operator;
import ed.inf.adbs.blazedb.catalog.Catalog;
import ed.inf.adbs.blazedb.Tuple;

// import javax.xml.catalog.Catalog;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;


public class ScanOperator extends Operator {
    private String tableName;
    private BufferedReader reader;
    private String filePath;
    private Catalog catalog;

    /**
     * Constructs a ScanOperator to scan the table with the given name.
     *
     * @param tableName The name of the table to scan.
     */
    public ScanOperator(String tableName) {
        this.tableName = tableName;
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
        } catch (IOException e) {
            System.err.println("Error opening file for table " + tableName + " at path " + filePath);
            e.printStackTrace();
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
            if (line != null) {
                return new Tuple(line);
            }
        } catch (IOException e) {
            System.err.println("Error reading next tuple from file " + filePath);
            e.printStackTrace();
        }
        return null;
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
            System.err.println("Error closing file " + filePath);
            e.printStackTrace();
        }
        openFileScan();
    }

    /**
     * Reads the next tuple and returns a tuple containing only the values of the specified column.
     * This is the single-column version.
     *
     * @param columnName The name of the column to project.
     * @return a Tuple with the projected field, or null if no more tuples.
     */
    public Tuple getNextProjectedTuple(String columnName) {
        Tuple rawTuple = getNextTuple();
        if (rawTuple == null) {
            return null;
        }
        String[] fields = rawTuple.toString().split(",");
        int projIndex = getColumnIndex(columnName);
        if (projIndex < 0 || projIndex >= fields.length) {
            System.err.println("Projection error: Column " + columnName + " not present in tuple: " + rawTuple);
            return null;
        }
        String projectedValue = fields[projIndex].trim();
        return new Tuple(projectedValue);
    }

    /**
     * Reads the next tuple and returns a tuple containing only the values of the specified columns.
     *
     * @param columnNames An array of column names to project.
     * @return a new Tuple containing only the projected values in the order of columnNames,
     *         or null if no more tuples.
     */
    public Tuple getNextProjectedTuple(String[] columnNames) {
        Tuple rawTuple = getNextTuple();
        if (rawTuple == null) {
            return null;
        }
        String[] fields = rawTuple.toString().split(",");
        StringBuilder projectedBuilder = new StringBuilder();
        boolean first = true;
        for (String col : columnNames) {
            int projIndex = getColumnIndex(col.trim());
            if (projIndex < 0 || projIndex >= fields.length) {
                System.err.println("Projection error: Column " + col + " not found in tuple: " + rawTuple);
                continue;
            }
            if (!first) {
                projectedBuilder.append(", ");
            }
            first = false;
            projectedBuilder.append(fields[projIndex].trim());
        }
        return new Tuple(projectedBuilder.toString());
    }

    /**
     * Returns the index corresponding to the given column name.
     * This sample mapping assumes "A" is index 0, "B" is index 1, "C" is index 2, etc.
     *
     * @param columnName the name of the column.
     * @return the index of the column.
     */
    private int getColumnIndex(String columnName) {
        switch (columnName.toUpperCase()) {
            case "A":
                return 0;
            case "B":
                return 1;
            case "C":
                return 2;
            case "D":
                return 3;
            default:
                System.err.println("Column " + columnName + " not found in schema.");
                return -1;
        }
    }
}


