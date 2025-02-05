package ed.inf.adbs.blazedb;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.Column;



/**
 * Lightweight in-memory database system.
 *
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
	 *
	 * For demonstration purposes, this method prints the results to the console.
	 */

	public static void executeQueryPlan(String inputFile, String outputFile) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (!(statement instanceof Select)) {
				System.err.println("Only SELECT queries are supported.");
				return;
			}
			Select select = (Select) statement;
			PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

			// Extract the table from the FROM clause.
			Table table = (Table) plainSelect.getFromItem();
			String tableName = table.getName();
			ScanOperator scanOperator = new ScanOperator(tableName);
			System.out.println("Scanning table: " + tableName);

			// Retrieve the SELECT items.
			List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
			boolean projectSpecific = true;

			// Check whether the query is SELECT * or projecting specific columns.
			if (selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*")) {
				projectSpecific = false;
			}

			if (projectSpecific) {
				// Create a list of column names to project.
				List<String> columnsToProject = new ArrayList<>();
				for (SelectItem<?> item : selectItems) {
					// The item's toString makes something like "Student.D" or "D".
					String itemStr = item.toString().trim();
					if (itemStr.contains(".")) {
						columnsToProject.add(itemStr.substring(itemStr.indexOf('.') + 1));
					} else {
						columnsToProject.add(itemStr);
					}
				}
				// Convert the list to an array.
				String[] columnArray = columnsToProject.toArray(new String[0]);
				System.out.println("Projecting columns: " + String.join(", ", columnArray));

				// Retrieve and print each projected tuple.
				Tuple tuple = scanOperator.getNextProjectedTuple(columnArray);
				while (tuple != null) {
					System.out.println(tuple);
					tuple = scanOperator.getNextProjectedTuple(columnArray);
				}
			} else {
				// No projection, print full tuples.
				Tuple tuple = scanOperator.getNextTuple();
				while (tuple != null) {
					System.out.println(tuple);
					tuple = scanOperator.getNextTuple();
				}
			}
		} catch (Exception e) {
			System.err.println("Exception occurred while executing the query plan:");
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
				writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}
	}
}

