package ed.inf.adbs.blazedb;

import java.util.List;

public class Tuple {
    private String rawData;
    private List<String> fields;

    public Tuple(List<String> fields) {
        this.fields = fields;
    }

    public String getField(int index) {
        return fields.get(index);
    }

    public Tuple(String rawData) {
        this.rawData = rawData;
    }

    @Override
    public String toString() {
        return rawData;
    }

    /**
     * Returns the integer value in this tuple at the given index.
     *
     * @param index The zero-based index of the value.
     * @return The integer value at the specified index.
     * @throws NumberFormatException if the field cannot be parsed as an integer.
     * @throws IndexOutOfBoundsException if the index is out of the bounds of the tuple.
     */
    public int getInt(int index) {
        if (index < 0 || index >= fields.size()) {
            throw new IndexOutOfBoundsException("Index " + index + " is out of bounds for tuple: " + rawData);
        }
        return Integer.parseInt(fields.get(index));
    }


    // Other Tuple methods can be added here.
}
