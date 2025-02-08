package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import ed.inf.adbs.blazedb.operator.ProjectionOperator;
import ed.inf.adbs.blazedb.operator.SelectOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import net.sf.jsqlparser.statement.select.*;


/**
 * Lightweight in-memory database system.
 * <p>
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */
public class BlazeDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic
		executeQueryPlan(inputFile, outputFile);
	}


	/**
	 * Parses a query from inputFile, extracts the table name from the FROM clause,
	 * instantiates a ScanOperator for that table, and then executes the operator.
	 * <p>
	 * For demonstration purposes, this method prints the results to the console.
	 */

	public static void executeQueryPlan(String inputFile, String outputFile) {
		try {
			// Parse the SQL query from the input file.
			FileReader fileReader = new FileReader(inputFile);
			Statement statement = CCJSqlParserUtil.parse(fileReader);

			if (!(statement instanceof Select)) {
				System.err.println("Only SELECT queries are supported.");
				return;
			}

			Select select = (Select) statement;
			PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

			// Extract the table name.
			Table table = (Table) plainSelect.getFromItem();
			String tableName = table.getName();
			System.out.println("Scanning table: " + tableName);

			// Create the base scan operator.
			Operator op = new ScanOperator(tableName, false);

			// If a WHERE clause exists, wrap the scan operator with a SelectOperator.
			Expression where = plainSelect.getWhere();
			Map<String, Integer> schemaMapping = createSchemaMapping(tableName);
			if (where != null) {
				op = new SelectOperator(op, where, schemaMapping);
			}


			// Check the SELECT items to see if we need to perform a projection.
			List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
			boolean projectSpecific = true;
			if (selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*")) {
				projectSpecific = false;
			}

			if (projectSpecific) {
				// Build the list of column names to project.
				List<String> columnsToProject = new ArrayList<>();
				for (SelectItem item : selectItems) {
					String itemStr = item.toString().trim();
					// If the column is prefixed with the table name, keep the fully qualified name.
					if (itemStr.contains(".")) {
						// Check if it already starts with the table name, otherwise prepend it.
						if (itemStr.startsWith(tableName + ".")) {
							columnsToProject.add(itemStr);
						} else {
							columnsToProject.add(tableName + "." + itemStr.substring(itemStr.indexOf('.') + 1));
						}
					} else {
						// If the column is not qualified, prepend the table name.
						columnsToProject.add(tableName + "." + itemStr);
					}
				}
				// Wrap the operator tree with a projection operator.
				// Make sure the projection columns match the keys in the schema mapping.
				op = new ProjectionOperator(op, columnsToProject.toArray(new String[0]), schemaMapping);
			}

			// Execute the operator tree.
			execute(op, outputFile);
		} catch (Exception e) {
			System.err.println("Error executing query plan: " + e.getMessage());
			e.printStackTrace();
		}
	}


	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root operator and writing the result to outputFile.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try (java.io.BufferedWriter writer = new java.io.BufferedWriter(new java.io.FileWriter(outputFile))) {
			Tuple tuple = root.getNextTuple();

			while (tuple != null) {
                System.out.println(tuple);
                writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}
	}

	private static Map<String, Integer> createSchemaMapping(String tableName) {
		Map<String, Integer> mapping = new HashMap<>();
		// Specify the absolute path to your schema file
		String schemaFilePath = "/Users/samlaborde-balen/Desktop/BlazeDB/samples/db/schema.txt";

		try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				// Check if the line starts with the table name followed by a space.
				if (line.startsWith(tableName + " ")) {
					// Tokenize the line; the first token is the table name and the rest are column names.
					String[] tokens = line.trim().split("\\s+");
					// Starting from index 1 since tokens[0] is already the table name.
					for (int i = 1; i < tokens.length; i++) {
						// Use i - 1 as the index if your tuple structure starts at 0
						mapping.put(tableName + "." + tokens[i], i - 1);
					}
					break; // Stop after processing the relevant table
				}
			}
		} catch (IOException e) {
			System.err.println("Error reading the schema file: " + e.getMessage());
		}

		System.out.println("Schema mapping: " + mapping);
		return mapping;
	}
}
