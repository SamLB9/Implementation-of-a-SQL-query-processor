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
 *   - SQL Query Parsing: Uses a SQL parser to read and parse SQL SELECT queries from input files.
 *   - Operator Tree Initialization: Builds a left-deep operator tree representing the execution plan, including optimized join processing where join conditions are applied during tuple combination.
 *   - Aggregation Handling: Evaluates aggregation functions such as SUM and manages grouping operations.
 *   - Projection Processing: Prunes and re-indexes the schema mappings to select the required columns as per the SELECT clause.
 *   - Duplicate Elimination: Removes duplicate records when DISTINCT or GROUP BY clauses are specified.
 *   - Sorting: Implements ORDER BY functionality to sort query results as required.
 *   - Result Execution and Verification: Executes the operator tree to generate query results and compares them with expected outputs.
 *   - Database Directory: Accepts a database directory parameter for locating the underlying data files.
 *
 * Usage Example:
 *   java -jar BlazeDB.jar queries/query1.sql results/output1.csv /path/to/databaseDir
 */
public class BlazeDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		final String databaseDir;
        databaseDir = args[0];
        String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic
		executeQueryPlan(inputFile, outputFile, databaseDir);
	}


	/**
	 * Executes the SQL query plan defined in the specified input file and writes the results to the output file.
	 * This method performs the following steps:
	 *   1. Parse the SQL SELECT query from the input file.
	 *   2. Initialize the operator tree and generate a full schema mapping using the provided database directory.
	 *   3. Collect the required columns from the SELECT, WHERE, GROUP BY, and ORDER BY clauses, using the first table as the default.
	 *   4. Process aggregations (if any), including handling SUM expressions and grouping operations.
	 *      - For aggregation queries, build a pruned mapping and wrap the operator tree with the SumOperator.
	 *      - Rebuild the final schema mapping to match the SELECT items when aggregations are present.
	 *   5. For non-aggregation queries, apply early projection processing and handle duplicate elimination if DISTINCT or GROUP BY clauses are specified.
	 *   6. Handle ORDER BY processing to sort the final results.
	 *   7. Execute the operator tree and compare the generated output with the expected results.
	 *
	 * @param inputFile   The path to the input file containing the SQL SELECT query.
	 * @param outputFile  The path to the output file where the query results will be written.
	 * @param databaseDir The directory containing the database data files.
	 */
	public static void executeQueryPlan(String inputFile, String outputFile, String databaseDir) {
		try (FileReader fileReader = new FileReader(inputFile)) {
			// 1. Initialize operator tree and full schema mapping.
			List<String> tableNames = new ArrayList<>();
			OperatorInitializationResult initResult = initializeAndParse(fileReader, tableNames, databaseDir);
			Operator rootOperator = initResult.getRootOperator();
			Map<String, Integer> schemaMapping = initResult.getSchemaMapping();

			// 2. Parse the SQL query.
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			Select selectStatement = (Select) statement;
			PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

			// 3. Collect required columns.
			String defaultTableName = tableNames.get(0);
			Set<String> requiredColumns = collectRequiredColumns(plainSelect, defaultTableName, databaseDir, schemaMapping);
			System.out.println("Required columns: " + requiredColumns);

			// 4. Process aggregations.
			AggregationResult aggregationResult = processAggregations(plainSelect);
			boolean hasAggregation = aggregationResult.hasAggregation();
			List<Expression> sumExpressions = aggregationResult.getSumExpressions();
			Map<Expression, String> literalSumMapping = aggregationResult.getLiteralSumMapping();

			// 5. Non-aggregation branch: project early if no aggregation.
			if (!hasAggregation) {
				ProjectionResult projResult = processNonAggregationProjection(plainSelect, rootOperator, schemaMapping, requiredColumns);
				rootOperator = projResult.getRootOperator();
				schemaMapping = projResult.getSchemaMapping();
			}
			// 6. Aggregation branch: build pruned mapping and wrap with SumOperator.
			if (hasAggregation) {
				Map<String, Integer> prunedMapping = processAggregationBranch(plainSelect, requiredColumns, schemaMapping, sumExpressions, literalSumMapping);
				// Retrieve GROUP BY expressions.
				List<Expression> groupByExpressions = new ArrayList<>();
				if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressions() != null) {
					groupByExpressions.addAll(plainSelect.getGroupBy().getGroupByExpressions());
				}
				rootOperator = new SumOperator(rootOperator, groupByExpressions, sumExpressions, prunedMapping);
				if (rootOperator instanceof SumOperator) {
					schemaMapping = ((SumOperator) rootOperator).getSchemaMapping();
				}
			}

			// 7. For non-aggregation queries, handle duplicate elimination.
			if (!hasAggregation) {
				rootOperator = handleDuplicateElimination(plainSelect, rootOperator);
			}

			// 8. Process subsequent projection.
			ProjectionResult projectionResult = processProjections(plainSelect, rootOperator, schemaMapping, hasAggregation, tableNames);
			rootOperator = projectionResult.getRootOperator();
			schemaMapping = projectionResult.getSchemaMapping();
			System.out.println("SchemaMapping after projections: " + schemaMapping);

			// 9. For aggregation queries, rebuild final schema mapping to match SELECT items.
			if (hasAggregation) {
				schemaMapping = rebuildSchemaMappingForSelect(plainSelect, schemaMapping);
				System.out.println("Final schemaMapping for ORDER BY: " + schemaMapping);
			}

			// 10. Order By processing.
			rootOperator = handleOrderBy(plainSelect, rootOperator, schemaMapping);

			// 11. Execute query plan.
			executeAndCompareOutput(rootOperator, outputFile);
		} catch (Exception e) {
			System.err.println("Error executing query plan: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Processes the aggregation branch by building a pruned mapping that includes only the required columns
	 * and any literal SUM aggregate expressions. For each required column found in the full schema mapping,
	 * an entry is added to the pruned mapping. Additionally, for each SUM aggregate expression, if its
	 * literal representation (from the provided mapping) is not already present in the pruned mapping, it is added.
	 *
	 * @param plainSelect      The parsed plain select query.
	 * @param requiredColumns  The set of columns required according to the query.
	 * @param schemaMapping    The full schema mapping with fully qualified column names and their indexes.
	 * @param sumExpressions   The list of SUM expression objects present in the query.
	 * @param literalSumMapping A mapping from SUM expressions to their literal string representations.
	 * @return A pruned mapping containing only the necessary columns and literal SUM aggregates.
	 */
	private static Map<String, Integer> processAggregationBranch(PlainSelect plainSelect, Set<String> requiredColumns,
																 Map<String, Integer> schemaMapping,
																 List<Expression> sumExpressions,
																 Map<Expression, String> literalSumMapping) {
		Map<String, Integer> prunedMapping = new LinkedHashMap<>();
		for (String col : requiredColumns) {
			Integer originalIndex = schemaMapping.get(col);
			if (originalIndex != null) {
				prunedMapping.put(col, originalIndex);
			} else {
				System.err.println("Column " + col + " not found in full schema mapping.");
			}
		}
		if (!literalSumMapping.isEmpty()) {
			int baseSize = prunedMapping.size();
			// For each SUM aggregate, add an entry with its literal string as key.
			for (Expression sumExpr : literalSumMapping.keySet()) {
				String sumExprStr = sumExpr.toString().trim();
				if (!prunedMapping.containsKey(sumExprStr)) {
					prunedMapping.put(sumExprStr, baseSize++);
				}
			}
		}
		return prunedMapping;
	}

	/**
	 * Rebuilds the final schema mapping so that it contains exactly the SELECT items (re-indexed from 0).
	 * This method processes each select item, using its alias if defined or the underlying column/expression
	 * name as a fallback. It then searches for a matching entry in the original schema mapping using a case-insensitive
	 * comparison before adding it to the final mapping.
	 *
	 * @param plainSelect   The parsed plain select query.
	 * @param schemaMapping The original full schema mapping with column names and their indexes.
	 * @return A new schema mapping containing only the SELECT items, re-indexed from 0.
	 */
	private static Map<String, Integer> rebuildSchemaMappingForSelect(PlainSelect plainSelect, Map<String, Integer> schemaMapping) {
		Map<String, Integer> finalMapping = new LinkedHashMap<>();
		List<? extends SelectItem> selectItems = plainSelect.getSelectItems();
		for (SelectItem item : selectItems) {
			String key = item.toString().trim();
			Integer idx = schemaMapping.get(key);
			if (idx == null) {
				for (Map.Entry<String, Integer> entry : schemaMapping.entrySet()) {
					if (entry.getKey().equalsIgnoreCase(key)) {
						idx = entry.getValue();
						break;
					}
				}
			}
			if (idx == null) {
				System.err.println("Cannot find column " + key + " in schema mapping.");
			}
			finalMapping.put(key, finalMapping.size());
		}
		return finalMapping;
	}

	/**
	 * Processes the projection for non-aggregation queries by selecting the required columns
	 * from the operator tree and updating the schema mapping accordingly.
	 *
	 * @param plainSelect      The parsed plain select query.
	 * @param rootOperator     The current root operator in the operator tree.
	 * @param schemaMapping    The current schema mapping with column names and their indexes.
	 * @param requiredColumns  The set of columns required according to the query.
	 * @return A {@link ProjectionResult} containing the updated root operator and schema mapping.
	 */
	private static ProjectionResult processNonAggregationProjection(
			PlainSelect plainSelect,
			Operator rootOperator,
			Map<String, Integer> schemaMapping,
			Set<String> requiredColumns) {

		List<String> queryColumnsOrdered = new ArrayList<>();

		// Handle SELECT * scenario.
		if (plainSelect.getSelectItems().size() == 1 &&
				plainSelect.getSelectItems().get(0).toString().trim().equals("*")) {
			queryColumnsOrdered.addAll(schemaMapping.keySet());
		} else {
			// Add columns as specified in the SELECT clause.
			for (SelectItem item : plainSelect.getSelectItems()) {
				String colName = item.toString().trim();
				if (requiredColumns.contains(colName)) {
					queryColumnsOrdered.add(colName);
					System.out.println("Required column: " + colName);
				} else {
					System.err.println("Column " + colName + " not found in requiredColumns.");
				}
			}
		}

		// Add missing ORDER BY columns.
		if (plainSelect.getOrderByElements() != null) {
			for (OrderByElement orderBy : plainSelect.getOrderByElements()) {
				String orderCol = orderBy.toString().trim();
				if (!queryColumnsOrdered.contains(orderCol)) {
					queryColumnsOrdered.add(orderCol);
					System.out.println("Adding ORDER BY column: " + orderCol);
				}
			}
		}

		// Create the pruned schema mapping based on the ordered query columns.
		Map<String, Integer> prunedMapping = new LinkedHashMap<>();
		for (String col : queryColumnsOrdered) {
			Integer originalIndex = schemaMapping.get(col);
			if (originalIndex != null) {
				prunedMapping.put(col, originalIndex);
			}
		}

		// Apply the ProjectionOperator.
		rootOperator = new ProjectionOperator(
				rootOperator,
				queryColumnsOrdered.toArray(new String[0]),
				prunedMapping
		);

		return new ProjectionResult(rootOperator, prunedMapping);
	}



	/**
	 * Handles the wrapping of the operator tree with a DuplicateEliminationOperator when required.
	 * This method checks whether the SQL query includes DISTINCT or GROUP BY clauses and applies
	 * duplicate elimination accordingly to ensure that the result set adheres to the query's specifications.
	 *
	 * @param plainSelect   The parsed SQL SELECT statement.
	 * @param rootOperator  The current root operator in the operator tree prior to duplicate elimination.
	 * @return The updated root operator after applying duplicate elimination if required. If duplicate
	 *         elimination is not needed, returns the original root operator unchanged.
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
	 * Collects the required columns from the SELECT, WHERE, GROUP BY, and ORDER BY clauses.
	 * When a SELECT * is encountered, it uses the initializeOperators method to obtain the full schema mapping.
	 *
	 * @param plainSelect      The parsed SQL SELECT query.
	 * @param defaultTableName The default table name to use when no table alias is provided.
	 * @param inputFile        The file path of the input SQL query.
	 * @return A set of fully qualified column names required for the query.
	 * @throws Exception if any step during processing fails.
	 */
	public static Set<String> collectRequiredColumns(PlainSelect plainSelect, String defaultTableName, String inputFile, Map<String, Integer> schemaMapping) throws Exception {
		Set<String> requiredCols = new HashSet<>();

		List<? extends SelectItem> selectItems = plainSelect.getSelectItems();
		if (selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*")) {
			requiredCols.addAll(schemaMapping.keySet());
		} else {
			// Process specific SELECT items
			for (SelectItem item : selectItems) {
				String itemStr = item.toString().trim();
				try {
					Expression expr = CCJSqlParserUtil.parseExpression(itemStr);
					if (expr != null) {
						expr.accept(new ExpressionVisitorAdapter() {
							@Override
							public void visit(Column column) {
								String tableAlias = (column.getTable() != null)
										? column.getTable().getName()
										: defaultTableName;
								String colName = column.getColumnName();
								requiredCols.add(tableAlias + "." + colName);
							}

							@Override
							public void visit(Function function) {
								// Visit each parameter by casting to Expression
								if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
									for (Object obj : function.getParameters().getExpressions()) {
										Expression exprParam = (Expression) obj;
										exprParam.accept(this);
									}
								}
							}
						});
					}
				} catch (Exception e) {
					System.err.println("Error parsing SELECT item expression: " + itemStr);
				}
			}
		}

		// Process WHERE clause
		if (plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
				@Override
				public void visit(Column column) {
					String tableAlias = (column.getTable() != null)
							? column.getTable().getName()
							: defaultTableName;
					String colName = column.getColumnName();
					requiredCols.add(tableAlias + "." + colName);
				}

				@Override
				public void visit(Function function) {
					if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
						for (Object obj : function.getParameters().getExpressions()) {
							Expression exprParam = (Expression) obj;
							exprParam.accept(this);
						}
					}
				}
			});
		}

		// Process GROUP BY clause
		if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressions() != null) {
			List<?> groupByExprs = plainSelect.getGroupBy().getGroupByExpressions();
			for (Object obj : groupByExprs) {
				Expression expr = (Expression) obj;
				expr.accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						String tableAlias = (column.getTable() != null)
								? column.getTable().getName()
								: defaultTableName;
						String colName = column.getColumnName();
						requiredCols.add(tableAlias + "." + colName);
					}

					@Override
					public void visit(Function function) {
						if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
							for (Object obj : function.getParameters().getExpressions()) {
								Expression exprParam = (Expression) obj;
								exprParam.accept(this);
							}
						}
					}
				});
			}
		}

		// Process ORDER BY clause
		List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
		if (orderByElements != null) {
			for (OrderByElement obe : orderByElements) {
				Expression expr = obe.getExpression();
				expr.accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						String tableAlias = (column.getTable() != null)
								? column.getTable().getName()
								: defaultTableName;
						String colName = column.getColumnName();
						requiredCols.add(tableAlias + "." + colName);
					}

					@Override
					public void visit(Function function) {
						if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
							for (Object obj : function.getParameters().getExpressions()) {
								Expression exprParam = (Expression) obj;
								exprParam.accept(this);
							}
						}
					}
				});
			}
		}

		return requiredCols;
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
		if (orderByElements == null || orderByElements.isEmpty()) {
			return rootOperator;
		}

		// Iterate over each ORDER BY element.
		for (OrderByElement orderBy : orderByElements) {
			// If the ORDER BY expression is not already a Column, try to resolve it.
			if (!(orderBy.getExpression() instanceof Column)) {
				String exprStr = orderBy.getExpression().toString().trim();

				// If the schema mapping already has this key, we're fine.
				if (!schemaMapping.containsKey(exprStr)) {
					// If it's a SUM aggregate, try to find a matching key in the mapping.
					if (exprStr.toUpperCase().startsWith("SUM(") && exprStr.endsWith(")")) {
						String innerExpr = exprStr.substring(4, exprStr.length() - 1).trim();
						String matchingKey = null;
						// Look for a key in schemaMapping that is also a SUM aggregate with the same inner expression.
						for (String key : schemaMapping.keySet()) {
							if (key.toUpperCase().startsWith("SUM(") && key.endsWith(")")) {
								String keyInner = key.substring(4, key.length() - 1).trim();
								if (keyInner.equalsIgnoreCase(innerExpr)) {
									matchingKey = key;
									break;
								}
							}
						}
						if (matchingKey != null) {
							exprStr = matchingKey;
						} else {
							throw new IllegalArgumentException("ORDER BY expression is not a column: " + orderBy.getExpression());
						}
					} else {
						throw new IllegalArgumentException("ORDER BY expression is not a column: " + orderBy.getExpression());
					}
				}
				// Replace the expression with a Column using the resolved column name.
				orderBy.setExpression(new Column(exprStr));
			}
		}
		return new SortOperator(rootOperator, orderByElements, schemaMapping);
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
        Pattern pattern1 = Pattern.compile("test(\\d+)\\.csv");
		Pattern pattern2 = Pattern.compile("t(\\d+)\\.csv");
        Matcher matcher = pattern.matcher(outputFile);
        Matcher matcher1 = pattern1.matcher(outputFile);
		Matcher matcher2 = pattern2.matcher(outputFile);
        String queryNumber = "";
        String queryNumber1 = "";
		String queryNumber2 = "";
		boolean comparisonResult = false;
		boolean comparisonResult1 = false;
		boolean comparisonResult2 = false;
        if (matcher.find()) {
            queryNumber = matcher.group(1);
        } else if (matcher1.find()) {
            queryNumber1 = matcher1.group(1);
		} else if (matcher2.find()) {
			queryNumber2 = matcher2.group(1);
        } else {
            System.err.println("Could not extract query number from output file path: " + outputFile);
        }

        // Define expected output path
        String expectedOutputPath = null;
        String expectedOutputPath1 = null;
		String expectedOutputPath2 = null;
        if (queryNumber != "" && queryNumber1 == "" && queryNumber2 == "") {
            expectedOutputPath = "samples/expected_output/query" + queryNumber + ".csv";
            comparisonResult = FileComparator.compareFiles(outputFile, expectedOutputPath);
        } else if (queryNumber1 != "" && queryNumber == "" && queryNumber2 == "") {
            expectedOutputPath1 = "samples/expected_output/test" + queryNumber1 + ".csv";
			comparisonResult1 = FileComparator.compareFiles(outputFile, expectedOutputPath1);
        } else {
			expectedOutputPath2 = "samples/expected_output/t" + queryNumber2 + ".csv";
			comparisonResult2 = FileComparator.compareFiles(outputFile, expectedOutputPath2);
		}

        if (comparisonResult) {
            System.out.println("Output matches the expected results.");
        } else if (comparisonResult1) {
            System.out.println("Output matches the expected results.");
        } else {
            System.out.println("Output does not match the expected results.");
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
	private static OperatorInitializationResult initializeAndParse(FileReader fileReader, List<String> tableNames, String databaseDir) throws Exception {
		Statement statement = CCJSqlParserUtil.parse(fileReader);

		if (!(statement instanceof Select)) {
			throw new IllegalArgumentException("Only SELECT queries are supported.");
		}

		Select selectStatement = (Select) statement;
		PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
		List<Join> joins = plainSelect.getJoins();

		Table fromTable = (Table) plainSelect.getFromItem();
		tableNames.add(fromTable.getName());
		// Instead of passing databaseDir (a String), pass fromTable (a Table)
		OperatorInitializationResult initResult = initializeOperators(joins, plainSelect, tableNames, fromTable);

		if (initResult.getSchemaMapping() == null) {
			throw new IllegalStateException("Schema mapping could not be initialized.");
		}

		return initResult;
	}

	/*
	 * This is a placeholder for your join operator builder.
	 * Replace with your actual join logic.
	 */
	private static Operator joinOperators(Map<String, ScanOperator> scanOperators) {
		// For simplicity, assume exactly two tables are involved.
		List<Operator> operators = new ArrayList<>(scanOperators.values());
		Operator left = operators.get(0);
		Operator right = operators.get(1);

		// For now, no explicit join condition is provided (cross join),
		// or you could extract it from your parsed query if available.
		Expression joinCondition = null;

		// Build the combined schema mapping from the left and right operators using a helper.
		Map<String, Integer> combinedSchemaMapping = combineSchemaMappings(
				buildSchemaMapping(left), buildSchemaMapping(right));

		// Construct the join operator with 4 arguments.
		return new JoinOperator(left, right, joinCondition, combinedSchemaMapping);
	}

	private static Map<String, Integer> buildSchemaMapping(Operator op) {
		List<String> outputColumns = new ArrayList<>();
		if (op instanceof SchemaProvider) {
			outputColumns = ((SchemaProvider) op).getOutputColumns();
		} else {
			throw new RuntimeException("Operator does not provide output schema information.");
		}

		Map<String, Integer> mapping = new HashMap<>();
		for (int i = 0; i < outputColumns.size(); i++) {
			mapping.put(outputColumns.get(i), i);
		}
		return mapping;
	}

	/**
	 * Combines two schema mappings.
	 * The indices from the right mapping are offset by the size of the left mapping.
	 *
	 * @param leftMapping  The left operator's schema mapping.
	 * @param rightMapping The right operator's schema mapping.
	 * @return A combined mapping for the join operator.
	 */
	private static Map<String, Integer> combineSchemaMappings(Map<String, Integer> leftMapping,
															  Map<String, Integer> rightMapping) {
		Map<String, Integer> combinedMapping = new HashMap<>();
		int index = 0;
		// Add all columns from the left mapping.
		for (String column : leftMapping.keySet()) {
			combinedMapping.put(column, index++);
		}
		// Add all columns from the right mapping.
		for (String column : rightMapping.keySet()) {
			combinedMapping.put(column, index++);
		}
		return combinedMapping;
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
	 * Constructs a join tree operator based on the provided list of table names and the SQL WHERE clause.
	 *
	 * The resulting operator tree represents a left-deep join tree where the first table is progressively
	 * joined with each subsequent table in the provided list using the specified join conditions.
	 *
	 * @param tableNames   A {@code List<String>} containing the names of the tables to be joined. The order of
	 *                     tables in the list determines the sequence of JOIN operations.
	 * @param whereClause  An {@link Expression} representing the SQL WHERE clause, which may contain both
	 *                     selection predicates (filters) and join conditions.
	 *
	 * @return An {@link Operator} representing the root of the join tree operator. This operator encapsulates
	 *         the entire sequence of JOIN operations and any applied selection predicates.
	 *
	 * @throws IllegalArgumentException if the {@code tableNames} list is empty, indicating that there are
	 *                                  no tables specified in the FROM clause of the SQL query.
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

	/**
	 * Determines whether a given SQL expression is local to a specified table.
	 *
	 * @param expr       The {@link Expression} representing the SQL condition to be evaluated.
	 *                   This could be a simple column check, a complex logical expression, or any
	 *                   other form of SQL predicate.
	 * @param tableName  The {@code String} name of the table to which the condition's locality is
	 *                   being assessed. The comparison is case-insensitive.
	 *
	 * @return {@code true} if all column references within the expression are associated with the
	 *         specified {@code tableName}; {@code false} otherwise.
	 *
	 * @throws IllegalArgumentException if the {@code tableName} is {@code null} or empty.
	 */
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
	 * Extracts and combines all selection conditions from the given WHERE clause that reference only the specified table.
	 *
	 * This method implements predicate combination for selection pushdown. It recursively examines the WHERE clause
	 * and picks out subexpressions that mention only columns from the specified table (using isConditionLocal()).
	 * If the entire expression is local to the table, it returns that expression. If the WHERE clause is a conjunction
	 * (an AndExpression), it recursively extracts the left and right conditions that are local, and if both exist,
	 * it combines them into a single composite predicate using an AndExpression. If only one side is local, that local
	 * predicate is returned. If no part of the expression applies solely to the table, the method returns null.
	 *
	 * @param whereClause the full WHERE clause expression.
	 * @param tableName   the name of the table for which to extract and combine local selection conditions.
	 * @return a new Expression that combines (using AND) all conditions referring solely to the given table,
	 *         or null if no such condition exists.
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
	 * Retrieves the set of table names referenced within a given SQL expression.
	 *
	 * @param expr The {@link Expression} from which to extract referenced table names.
	 *             This expression can represent various SQL predicates, including selections,
	 *             joins, and complex logical conditions.
	 *
	 * @return A {@link Set} of {@link String} objects, each representing a unique table name
	 *         that is referenced within the provided expression. If no tables are referenced,
	 *         an empty set is returned.
	 *
	 * @throws IllegalArgumentException if the provided {@code expr} is {@code null}, indicating
	 *                                  that there is no expression to analyze.
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
	 * Merges two schema mappings into a single combined mapping.
	 *
	 * @param leftMapping  A {@link Map} where each key is a column name from the left table, and
	 *                     each value is the corresponding column index. This map represents the schema
	 *                     mapping of the first table involved in the join operation.
	 * @param rightMapping A {@link Map} where each key is a column name from the right table, and
	 *                     each value is the corresponding column index. This map represents the schema
	 *                     mapping of the second table to be joined with the first table.
	 *
	 * @return A new {@link Map} representing the merged schema mapping. This map contains all entries
	 *         from {@code leftMapping} and {@code rightMapping}, with the indices from
	 *         {@code rightMapping} appropriately offset to ensure unique indexing across the combined
	 *         schema.
	 *
	 * @throws IllegalArgumentException if either {@code leftMapping} or {@code rightMapping} is {@code null}.
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
