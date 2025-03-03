package ed.inf.adbs.blazedb;

import java.util.List;

/**
 * The {@code Tuple} class represents a single record or row in a database table within the BlazeDB system.
 * It encapsulates the data fields of the record and provides methods to access and manipulate these fields.
 * This class is designed to handle both raw data representations and structured field lists,
 * offering flexibility in how data is stored and retrieved. It serves as a fundamental building block
 * for query processing, data manipulation, and storage operations within the BlazeDB framework.
 */
public class Tuple {
    private String rawData;
    private List<String> fields;

    /**
     * Constructs a new {@code Tuple} instance with the specified list of fields.
     * This constructor is typically used when the tuple data is already parsed and available
     * as a list of individual fields.
     *
     * @param fields A {@code List<String>} containing the fields of the tuple.
     * @throws IllegalArgumentException if {@code fields} is {@code null} or empty.
     */
    public Tuple(List<String> fields) {
        this.fields = fields;
    }

    /**
     * Retrieves the list of fields contained in this tuple.
     * The returned list contains each field as a {@code String}, representing the values
     * of the tuple's columns.
     *
     * @return A {@code List<String>} representing the tuple's fields.
     */
    public List<String> getFields() {
        return fields;
    }

    /**
     * Constructs a new {@code Tuple} instance with the specified raw data string.
     * This constructor is useful when the tuple data is available as a single raw
     * {@code String} and needs to be parsed or processed later.
     *
     * @param rawData A {@code String} containing the raw data of the tuple.
     * @throws IllegalArgumentException if {@code rawData} is {@code null} or empty.
     */
    public Tuple(String rawData) {
        this.rawData = rawData;
    }

    /**
     * Returns a string representation of the tuple by joining all fields with a comma and space.
     * This method provides a human-readable format of the tuple, suitable for logging,
     * debugging, or display purposes.
     *
     * @return A {@code String} representing the concatenated fields of the tuple.
     */
    @Override
    public String toString() {
        return String.join(", ", fields);
    }


    /**
     * Retrieves the integer value of the field at the specified index within the tuple.
     * This method parses the field at the given index from a {@code String} to an {@code int}.
     * It is useful for accessing numerical data stored in the tuple.
     *
     * @param index The zero-based index of the field to retrieve as an integer.
     * @return The integer value of the field at the specified index.
     *
     * @throws IndexOutOfBoundsException if the provided index is out of the tuple's field bounds.
     */
    public int getInt(int index) {
        if (index < 0 || index >= fields.size()) {
            throw new IndexOutOfBoundsException("Index " + index + " is out of bounds for tuple: " + rawData);
        }
        return Integer.parseInt(fields.get(index));
    }
}
