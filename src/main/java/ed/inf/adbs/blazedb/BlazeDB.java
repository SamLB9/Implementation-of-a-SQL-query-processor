package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ed.inf.adbs.blazedb.operator.*;
import ed.inf.adbs.blazedb.result.AggregationResult;
import ed.inf.adbs.blazedb.result.OperatorInitializationResult;
import ed.inf.adbs.blazedb.result.ProjectionResult;
import ed.inf.adbs.blazedb.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import ed.inf.adbs.blazedb.operator.SumOperator;
import ed.inf.adbs.blazedb.operator.Operator;
import java.util.ArrayList;
import java.util.List;



/**
 * BlazeDB is a lightweight, in-memory database system designed to execute SQL queries efficiently.
 *
 * Key Features and Functionalities:
 *   SQL Query Parsing: Parses SQL SELECT queries from input files using a SQL parser.
 *   Operator Tree Initialization: Constructs an operator tree that represents the execution plan of the parsed query.
 *   Aggregation Handling: Processes aggregation functions such as SUM and manages grouping operations.
 *   Projection Processing: Selects the required columns based on the SELECT clause of the query.
 *   Duplicate Elimination: Removes duplicate records when DISTINCT or GROUP BY clauses are present.
 *   Sorting: Implements ORDER BY functionality to sort the query results as specified.
 *   Result Execution and Comparison: Executes the operator tree to generate query results and compares them against expected outputs for verification.
 *
 * Usage Example:
 *   java -jar BlazeDB.jar queries/query1.sql results/output1.csv
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
	 * Executes the SQL query plan defined in the specified input file and writes the results to the output file.
	 * This method performs the following steps:
	 *   Parses the SQL SELECT query from the input file.
	 *   Initializes the operator tree and schema mapping based on the parsed query.
	 *   Processes any aggregations, handling SUM expressions and grouping as necessary.
	 *   Applies projections to select the required columns.
	 *   Handles duplicate elimination if the query includes DISTINCT or GROUP BY clauses.
	 *   Applies sorting based on ORDER BY clauses.
	 *   Executes the operator tree to generate the query results.
	 *   Compares the generated output with expected results to verify correctness.
	 *
	 * @param inputFile  The path to the input file containing the SQL SELECT query.
	 * @param outputFile The path to the output file where the query results will be written.
	 */

	public static void executeQueryPlan(String inputFile, String outputFile) {
		try (FileReader fileReader = new FileReader(inputFile)) {
			// Initialize operator tree and schema mapping
			List<String> tableNames = new ArrayList<>();
			OperatorInitializationResult initResult = initializeAndParse(fileReader, tableNames);
			Operator rootOperator = initResult.getRootOperator();
			Map<String, Integer> schemaMapping = initResult.getSchemaMapping();

			// Parse the SQL query again to get PlainSelect (could be optimized to pass it from initializeAndParse)
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			Select selectStatement = (Select) statement;
			PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

			// --- Aggregation Processing ---
			AggregationResult aggregationResult = processAggregations(plainSelect);
			List<Expression> sumExpressions = aggregationResult.getSumExpressions();
			boolean hasAggregation = aggregationResult.hasAggregation();
			Map<Expression, String> literalSumMapping = aggregationResult.getLiteralSumMapping();
			// ---

			// If there is aggregation, and some SUM expressions referenced a literal value,
			// wrap the current operator with a LiteralAppendOperator.
			if (hasAggregation && !literalSumMapping.isEmpty()) {
				rootOperator = new LiteralAppendOperator(rootOperator, literalSumMapping, schemaMapping);
				// Update schema mapping with new literal columns.
				int baseSize = schemaMapping.size();
				for (String alias : literalSumMapping.values()) {
					schemaMapping.put(alias, baseSize++);
				}
			}

			// Wrap the base operator with a SumOperator if there is any SUM aggregation.
			if (hasAggregation) {
				List<Expression> groupByExpressions = new ArrayList<>();
				if (plainSelect.getGroupBy() != null &&
						plainSelect.getGroupBy().getGroupByExpressions() != null) {
					groupByExpressions.addAll(plainSelect.getGroupBy().getGroupByExpressions());
				}
				rootOperator = new SumOperator(rootOperator, groupByExpressions, sumExpressions, schemaMapping);
				if (rootOperator instanceof SumOperator) {
					schemaMapping = ((SumOperator) rootOperator).getSchemaMapping();
				}
			}

			// --- Projection Processing ---
			ProjectionResult projectionResult = processProjections(
					plainSelect,
					rootOperator,
					schemaMapping,
					hasAggregation,
					tableNames
			);
			rootOperator = projectionResult.getRootOperator();
			schemaMapping = projectionResult.getSchemaMapping();
			// ---

			// --- Duplicate Elimination Handling ---
			rootOperator = handleDuplicateElimination(plainSelect, rootOperator);
			// ---

			// --- Order By Handling ---
			rootOperator = handleOrderBy(plainSelect, rootOperator, schemaMapping);
			// ---

			// --- Execute and Compare Output ---
			executeAndCompareOutput(rootOperator, outputFile);
			// ---

		} catch (Exception e) {
			System.err.println("Error executing query plan: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Handles the wrapping of the operator tree with DuplicateEliminationOperator if needed.
	 *
	 * @param plainSelect   The parsed SQL select statement.
	 * @param rootOperator  The current root operator in the operator tree.
	 * @return The updated root operator after applying duplicate elimination if required.
	 */
	private static Operator handleDuplicateElimination(PlainSelect plainSelect, Operator rootOperator) {
		boolean hasDistinct = (plainSelect.getDistinct() != null);
		boolean hasGroupBy = plainSelect.getGroupBy() != null &&
				plainSelect.getGroupBy().getGroupByExpressions() != null &&
				!plainSelect.getGroupBy().getGroupByExpressions().isEmpty();

		if (hasDistinct || hasGroupBy) {
			rootOperator = new DuplicateEliminationOperator(rootOperator);
		}

		return rootOperator;
	}

	/**
	 * Handles the wrapping of the operator tree with SortOperator if ORDER BY clauses are present.
	 *
	 * @param plainSelect   The parsed SQL select statement.
	 * @param rootOperator  The current root operator in the operator tree.
	 * @param schemaMapping The current schema mapping of column names to their indices.
	 * @return The updated root operator after applying sorting if required.
	 */
	private static Operator handleOrderBy(PlainSelect plainSelect, Operator rootOperator, Map<String, Integer> schemaMapping) {
		List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
		if (orderByElements != null && !orderByElements.isEmpty()) {
			rootOperator = new SortOperator(rootOperator, orderByElements, schemaMapping);
		}
		return rootOperator;
	}

	/**
	 * Executes the final operator tree and compares the output with the expected results.
	 *
	 * @param rootOperator The final root operator of the operator tree.
	 * @param outputFile   The path to the output file where results are written.
	 */
	private static void executeAndCompareOutput(Operator rootOperator, String outputFile) {
		// Execute the final operator tree
		execute(rootOperator, outputFile);

		// Extract query number from output file path
		Pattern pattern = Pattern.compile("query(\\d+)\\.csv");
		Matcher matcher = pattern.matcher(outputFile);
		String queryNumber = "";
		if (matcher.find()) {
			queryNumber = matcher.group(1);
		} else {
			System.err.println("Could not extract query number from output file path: " + outputFile);
		}

		// Define expected output path
		String expectedOutputPath = "samples/expected_output/query"
				+ queryNumber + ".csv";

		// Compare the actual output with the expected output
		boolean comparisonResult = FileComparator.compareFiles(outputFile, expectedOutputPath);

		if (comparisonResult) {
			System.out.println("Output matches the expected results.");
		} else {
			System.err.println("Output does not match the expected results.");
		}
	}

	/**
	 * Initializes the operator tree and schema mapping by parsing the SQL query and setting up joins.
	 *
	 * @param fileReader   The FileReader for the input SQL file.
	 * @param tableNames   The list to populate with table names involved in the query.
	 * @return An OperatorInitializationResult containing the root operator and schema mapping.
	 * @throws Exception If an error occurs during parsing or operator initialization.
	 */
	private static OperatorInitializationResult initializeAndParse(FileReader fileReader, List<String> tableNames) throws Exception {
		Statement statement = CCJSqlParserUtil.parse(fileReader);

		if (!(statement instanceof Select)) {
			throw new IllegalArgumentException("Only SELECT queries are supported.");
		}

		Select selectStatement = (Select) statement;
		PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
		List<Join> joins = plainSelect.getJoins();

		Table fromTable = (Table) plainSelect.getFromItem();
		tableNames.add(fromTable.getName());

		OperatorInitializationResult initResult = initializeOperators(joins, plainSelect, tableNames, fromTable);

		if (initResult.getSchemaMapping() == null) {
			throw new IllegalStateException("Schema mapping could not be initialized.");
		}

		return initResult;
	}

	/**
	 * Processes the projection part of the query, handling both aggregated and non-aggregated projections.
	 *
	 * @param plainSelect    The parsed SQL select statement.
	 * @param rootOperator   The current root operator in the operator tree.
	 * @param schemaMapping  The current schema mapping of column names to their indices.
	 * @param hasAggregation A flag indicating whether the query includes aggregation.
	 * @param tableNames     The list of table names involved in the query.
	 * @return A ProjectionResult containing the updated root operator and schema mapping.
	 */
	private static ProjectionResult processProjections(
			PlainSelect plainSelect,
			Operator rootOperator,
			Map<String, Integer> schemaMapping,
			boolean hasAggregation,
			List<String> tableNames) {

		List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
		boolean projectSpecific = true;

		if (selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*")) {
			projectSpecific = false;
		}

		if (projectSpecific) {
			if (hasAggregation) {
				if (plainSelect.getGroupBy() != null &&
						plainSelect.getGroupBy().getGroupByExpressions() != null &&
						!plainSelect.getGroupBy().getGroupByExpressions().isEmpty()) {
					// Build the projected column names from the SELECT items.
					List<String> aggregatedColumns = new ArrayList<>();
					for (SelectItem item : selectItems) {
						String itemStr = item.toString().trim().toUpperCase();
						if (itemStr.startsWith("SUM(")) {
							aggregatedColumns.add("SUM");
						} else {
							aggregatedColumns.add("Group");
						}
					}
					rootOperator = new ProjectionOperator(
							rootOperator,
							aggregatedColumns.toArray(new String[0]),
							schemaMapping
					);

					// Build a matching schema mapping.
					Map<String, Integer> projectedSchemaMapping = new LinkedHashMap<>();
					for (int i = 0; i < aggregatedColumns.size(); i++) {
						projectedSchemaMapping.put(aggregatedColumns.get(i), i);
					}
					schemaMapping = projectedSchemaMapping;
				} else {
					// Global aggregation (no GROUP BY).
					List<String> aggregatedColumns = new ArrayList<>(schemaMapping.keySet());
					rootOperator = new ProjectionOperator(
							rootOperator,
							aggregatedColumns.toArray(new String[0]),
							schemaMapping
					);

					// Update schema mapping accordingly.
					Map<String, Integer> projectedSchemaMapping = new LinkedHashMap<>();
					for (int i = 0; i < aggregatedColumns.size(); i++) {
						projectedSchemaMapping.put(aggregatedColumns.get(i), i);
					}
					schemaMapping = projectedSchemaMapping;
				}
			} else {
				// Non-aggregated query: use fully qualified column names.
				List<String> columnsToProject = new ArrayList<>();
				for (SelectItem item : selectItems) {
					String itemStr = item.toString().trim();
					if (itemStr.contains("."))
						columnsToProject.add(itemStr);
					else
						columnsToProject.add(tableNames.get(0) + "." + itemStr);
				}
				rootOperator = new ProjectionOperator(
						rootOperator,
						columnsToProject.toArray(new String[0]),
						schemaMapping
				);

				// Update schema mapping.
				Map<String, Integer> projectedSchemaMapping = new HashMap<>();
				for (int i = 0; i < columnsToProject.size(); i++) {
					projectedSchemaMapping.put(columnsToProject.get(i), i);
				}
				schemaMapping = projectedSchemaMapping;
			}
		}

		return new ProjectionResult(rootOperator, schemaMapping);
	}

	/**
	 * Processes aggregation by identifying SUM expressions and handling literal sum mappings.
	 *
	 * @param plainSelect          The parsed SQL select statement.
	 * @return AggregationResult containing sum expressions, aggregation flag, and literal sum mappings.
	 */
	private static AggregationResult processAggregations(PlainSelect plainSelect) {
		List<Expression> sumExpressions = new ArrayList<>();
		boolean hasAggregation = false;
		Map<Expression, String> literalSumMapping = new HashMap<>();
		int literalCounter = 0;

		List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
		for (SelectItem item : selectItems) {
			String itemStr = item.toString();
			if (itemStr.trim().toUpperCase().startsWith("SUM(")) {
				hasAggregation = true;
				int start = itemStr.indexOf("(");
				int end = itemStr.lastIndexOf(")");
				if (start != -1 && end != -1 && end > start) {
					String innerExpStr = itemStr.substring(start + 1, end);
					try {
						Expression innerExp = CCJSqlParserUtil.parseExpression(innerExpStr);
						// If the sum's inner expression is a literal, then generate a unique alias
						if (innerExp instanceof LongValue || innerExp instanceof DoubleValue) {
							String alias = "LITERAL_SUM_" + literalCounter++;
							// Save the mapping so that later we can add a constant column to the tuple.
							literalSumMapping.put(innerExp, alias);
							// Replace the literal expression with a column reference to the alias.
							innerExp = new Column(alias);
						}
						sumExpressions.add(innerExp);
					} catch (Exception e) {
						System.err.println("Error parsing SUM inner expression: " + innerExpStr);
					}
				}
			}
		}

		return new AggregationResult(sumExpressions, hasAggregation, literalSumMapping);
	}

    /**
     * Initializes the root operator and schema mapping based on the provided joins and plainSelect.
     *
     * @param joins        The list of join conditions.
     * @param plainSelect  The plain select statement containing the WHERE clause.
     * @param tableNames   The list to populate with table names involved in joins.
     * @return The initialized root operator.
     * @throws Exception If an error occurs during initialization.
     */
	private static OperatorInitializationResult initializeOperators(List<Join> joins, PlainSelect plainSelect, List<String> tableNames, Table fromTable) throws Exception {
		Operator rootOperator;
		Map<String, Integer> schemaMapping = null;

		if (joins != null && !joins.isEmpty()) {
			for (Join join : joins) {
				Table joinTable = (Table) join.getRightItem();
				tableNames.add(joinTable.getName());
			}
			// Build a join tree based on all table names and the WHERE clause.
			rootOperator = buildJoinTree(tableNames, plainSelect.getWhere());
			// Compute the merged schema mapping for the join operators.
			if (schemaMapping == null) {
				if (tableNames.size() == 1) {
					schemaMapping = createSchemaMapping(tableNames.get(0));
				} else {
					Map<String, Integer> mapping = createSchemaMapping(tableNames.get(0));
					for (int i = 1; i < tableNames.size(); i++) {
						Map<String, Integer> nextMapping = createSchemaMapping(tableNames.get(i));
						mapping = mergeSchemaMappings(mapping, nextMapping);
					}
					schemaMapping = mapping;
				}
			}
		} else {
			// No join: use a simple scan.
			String tableName = fromTable.getName();
			System.out.println("Scanning table: " + tableName);
			rootOperator = new ScanOperator(tableName, false);
			schemaMapping = createSchemaMapping(tableName);

			// Push down selection if where clause exists.
			if (plainSelect.getWhere() != null) {
				rootOperator = new SelectOperator(rootOperator, plainSelect.getWhere(), schemaMapping);
			}
		}

		return new OperatorInitializationResult(rootOperator, schemaMapping);
	}

	/**
	 * Build a join tree given a list of table names and a WHERE clause.
	 * This method creates a scan (with selection pushdown) for the first table and
	 * then iteratively joins each subsequent table.
	 */
	public static Operator buildJoinTree(List<String> tableNames, Expression whereClause) {
		if (tableNames.isEmpty()) {
			throw new IllegalArgumentException("No table in FROM clause.");
		}

		// Start with the first table.
		Map<String, Integer> currentSchemaMapping = createSchemaMapping(tableNames.get(0));
		Operator currentOperator = new ScanOperator(tableNames.get(0), false);
		Expression selectionForLeft = extractSelectionCondition(whereClause, tableNames.get(0));
		if (selectionForLeft != null) {
			currentOperator = new SelectOperator(currentOperator, selectionForLeft, currentSchemaMapping);
		}

		// Iteratively join with the remaining tables.
		for (int i = 1; i < tableNames.size(); i++) {
			String table = tableNames.get(i);
			Map<String, Integer> rightSchemaMapping = createSchemaMapping(table);
			Operator rightOperator = new ScanOperator(table, false);

			Expression selectionForRight = extractSelectionCondition(whereClause, table);
			if (selectionForRight != null) {
				rightOperator = new SelectOperator(rightOperator, selectionForRight, rightSchemaMapping);
			}

			// Extract a join condition that references columns from both current and right schemas.
			Expression joinCondition = extractJoinCondition(whereClause, currentSchemaMapping, rightSchemaMapping);

			// Merge the two schema mappings.
			Map<String, Integer> combinedMapping = mergeSchemaMappings(currentSchemaMapping, rightSchemaMapping);
			// Create the join operator.
			currentOperator = new JoinOperator(currentOperator, rightOperator, joinCondition, combinedMapping);
			currentSchemaMapping = combinedMapping;
		}

		return currentOperator;
	}

	private static boolean isConditionLocal(Expression expr, final String tableName) {
		final boolean[] isLocal = { true };

		expr.accept(new ExpressionVisitorAdapter() {
			@Override
			public void visit(Column column) {
				// Check if the column is qualified with a table name
				if (column.getTable() != null && column.getTable().getName() != null) {
					String colTableName = column.getTable().getName();
					// If the column's table does not match the given tableName, mark as not local
					if (!colTableName.equalsIgnoreCase(tableName)) {
						isLocal[0] = false;
					}
				}
			}
		});

		return isLocal[0];
	}


	/**
	 * Extracts the selection condition that references only the given table.
	 * It recursively checks the given whereClause and picks out subexpressions that
	 * mention only the columns from tableName.
	 *
	 * @param whereClause the full WHERE clause expression
	 * @param tableName the name of the table for which to extract local conditions
	 * @return a new Expression if one or more conditions refer solely to the given table;
	 *         null if none exist.
	 */
	private static Expression extractSelectionCondition(Expression whereClause, String tableName) {
		if (whereClause == null) {
			return null;
		}
		// If the entire expression is localized, return it.
		if (isConditionLocal(whereClause, tableName)) {
			return whereClause;
		}

		// If the whereClause is a conjunction, try to extract parts.
		if (whereClause instanceof AndExpression) {
			AndExpression andExpr = (AndExpression) whereClause;
			Expression leftExtracted = extractSelectionCondition(andExpr.getLeftExpression(), tableName);
			Expression rightExtracted = extractSelectionCondition(andExpr.getRightExpression(), tableName);

			if (leftExtracted != null && rightExtracted != null) {
				// Combine both local conditions.
				return new AndExpression(leftExtracted, rightExtracted);
			} else if (leftExtracted != null) {
				return leftExtracted;
			} else if (rightExtracted != null) {
				return rightExtracted;
			}
		}

		// In case it's a binary expression (other than a conjunction) which is not fully local,
		// we simply return null.
		return null;
	}

	/**
	 * Extracts the join condition from the specified WHERE clause by identifying binary expressions
	 * that involve tables from both the current and right schema mappings.
	 *
	 * <p>This method recursively traverses the WHERE clause expression. For conjunctions (AND expressions),
	 * it attempts to extract join conditions from both sides of the expression. If a binary expression references
	 * at least one table from each schema mapping, it is considered a join condition and returned.
	 *
	 * @param whereClause           The WHERE clause expression to process.
	 * @param currentSchemaMapping  A map of table names to their corresponding indices in the current schema.
	 * @param rightSchemaMapping    A map of table names to their corresponding indices in the right schema.
	 * @return The extracted join condition expression if one exists; otherwise, {@code null}.
	 */
	private static Expression extractJoinCondition(Expression whereClause,
												   Map<String, Integer> currentSchemaMapping,
												   Map<String, Integer> rightSchemaMapping) {
		if (whereClause == null) {
			return null;
		}

		// If the whereClause is a conjunction, try to extract join conditions from both sides.
		if (whereClause instanceof AndExpression) {
			AndExpression andExpr = (AndExpression) whereClause;
			Expression leftJoin = extractJoinCondition(andExpr.getLeftExpression(), currentSchemaMapping, rightSchemaMapping);
			Expression rightJoin = extractJoinCondition(andExpr.getRightExpression(), currentSchemaMapping, rightSchemaMapping);

			if (leftJoin != null && rightJoin != null) {
				return new AndExpression(leftJoin, rightJoin);
			} else if (leftJoin != null) {
				return leftJoin;
			} else {
				return rightJoin;
			}
		}

		// For a candidate expression that is a BinaryExpression (e.g., EqualsTo, GreaterThan, etc.)
		if (whereClause instanceof BinaryExpression) {
			Set<String> referencedTables = getReferencedTables(whereClause);

			// Get tables from the current and right mappings.
			Set<String> currentTables = getTablesFromMapping(currentSchemaMapping);
			Set<String> rightTables = getTablesFromMapping(rightSchemaMapping);

			// Check if the expression uses at least one table from each mapping.
			boolean usesCurrent = false;
			boolean usesRight = false;
			for (String table : referencedTables) {
				if (currentTables.contains(table)) {
					usesCurrent = true;
				}
				if (rightTables.contains(table)) {
					usesRight = true;
				}
			}

			if (usesCurrent && usesRight) {
				return whereClause;
			}
		}

		// In all other cases, return null.
		return null;
	}

	/**
	 * Collects the set of table names referenced in an expression.
	 */
	private static Set<String> getReferencedTables(Expression expr) {
		final Set<String> tables = new HashSet<>();
		expr.accept(new ExpressionVisitorAdapter() {
			@Override
			public void visit(Column column) {
				if (column.getTable() != null && column.getTable().getName() != null) {
					tables.add(column.getTable().getName());
				}
			}
		});
		return tables;
	}


	/**
	 * Extracts the table names from a schema mapping.
	 * The keys of the mapping are assumed to be qualified as "TableName.ColumnName".
	 */
	private static Set<String> getTablesFromMapping(Map<String, Integer> mapping) {
		Set<String> tables = new HashSet<>();
		for (String qualifiedName : mapping.keySet()) {
			// Assuming the key format is "TableName.ColumnName"
			String[] parts = qualifiedName.split("\\.");
			if (parts.length >= 1) {
				tables.add(parts[0]);
			}
		}
		return tables;
	}


	/**
	 * Placeholder for a method to merge two schema mappings.
	 */
	private static Map<String, Integer> mergeSchemaMappings(Map<String, Integer> leftMapping,
															Map<String, Integer> rightMapping) {
		Map<String, Integer> merged = new HashMap<>();
		merged.putAll(leftMapping);
		// The indices for the rightMapping are shifted by the size of leftMapping.
		int offset = leftMapping.size();
		for (Map.Entry<String, Integer> entry : rightMapping.entrySet()) {
			merged.put(entry.getKey(), entry.getValue() + offset);
		}
		return merged;
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

	/**
	 * Creates a schema mapping for the specified table by reading its column information from the schema file.
	 *
	 * <p>This method reads the schema definition from "samples/db/schema.txt" and constructs a map where
	 * each key is a fully qualified column name (e.g., "tableName.columnName") and the value is the
	 * corresponding column index. It iterates through the schema file to find the line that starts with
	 * the given table name and processes its columns to build the mapping.
	 *
	 * @param tableName The name of the table for which the schema mapping is to be created.
	 * @return A map containing column names as keys and their respective indices as values.
	 */
	private static Map<String, Integer> createSchemaMapping(String tableName) {
		Map<String, Integer> mapping = new HashMap<>();
		// Specify the absolute path to your schema file
		String schemaFilePath = "samples/db/schema.txt";

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

		// System.out.println("Schema mapping: " + mapping);
		return mapping;
	}
}
