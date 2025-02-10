package ed.inf.adbs.blazedb.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileComparator {

    /**
     * Compares two files regardless of the order of the lines.
     * It first checks if both files have the same number of lines and then whether
     * each line in the query output appears in the expected output (ignoring surrounding whitespace).
     *
     * @param queryOutputPath The path to the file containing the query output.
     * @param expectedOutputPath The path to the file containing the expected output.
     * @return true if both files have the same number of lines and contain the same lines regardless of the order,
     *         false otherwise.
     */
    public static boolean compareFiles(String queryOutputPath, String expectedOutputPath) {
        try (BufferedReader queryReader = new BufferedReader(new FileReader(queryOutputPath));
             BufferedReader expectedReader = new BufferedReader(new FileReader(expectedOutputPath))) {

            // Build frequency maps for both files.
            Map<String, Integer> queryLines = buildFrequencyMap(queryReader);
            Map<String, Integer> expectedLines = buildFrequencyMap(expectedReader);

            // Check if both maps have the same total number of lines.
            int queryTotal = queryLines.values().stream().mapToInt(Integer::intValue).sum();
            int expectedTotal = expectedLines.values().stream().mapToInt(Integer::intValue).sum();

            if (queryTotal != expectedTotal) {
                System.out.println("❌" + " Files have different numbers of lines.");
                return false;
            }

            if (!queryLines.equals(expectedLines)) {
                System.out.println("❌" + " The contents of the files differ.");
                System.out.println("Query output frequencies: " + queryLines);
                System.out.println("Expected output frequencies: " + expectedLines);
                return false;
            }

            System.out.println("✅ " + " The query output matches the expected output.");
            return true;

        } catch (IOException e) {
            System.err.println("❌ An error occurred while comparing files: " + e.getMessage());
            return false;
        }
    }

    /**
     * Reads all lines from the given BufferedReader, trims them, and builds a frequency map.
     *
     * @param reader The BufferedReader to read from.
     * @return A map where the key is a trimmed line and the value is its frequency.
     * @throws IOException if an I/O error occurs.
     */
    private static Map<String, Integer> buildFrequencyMap(BufferedReader reader) throws IOException {
        Map<String, Integer> frequencyMap = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String trimmedLine = line.trim();
            frequencyMap.put(trimmedLine, frequencyMap.getOrDefault(trimmedLine, 0) + 1);
        }
        return frequencyMap;
    }
}