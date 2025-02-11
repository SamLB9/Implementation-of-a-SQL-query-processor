package ed.inf.adbs.blazedb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ed.inf.adbs.blazedb.operator.*;
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
			Operator rootOperator = null;
			Map<String, Integer> schemaMapping = null;
			FileReader fileReader = new FileReader(inputFile);
			Statement statement = CCJSqlParserUtil.parse(fileReader);

			if (!(statement instanceof Select)) {
				System.err.println("Only SELECT queries are supported.");
				return;
			}

			boolean hasDistinct = false;
			Select selectStatement = (Select) statement;
			PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
			hasDistinct = (plainSelect.getDistinct() != null);

			List<String> tableNames = new ArrayList<>();
			Table fromTable = (Table) plainSelect.getFromItem();
			tableNames.add(fromTable.getName());

			List<Join> joins = plainSelect.getJoins();
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

			// Process aggregation: Identify SUM expressions (if any).
			List<Expression> sumExpressions = new ArrayList<>();
			boolean hasAggregation = false;
			// For handling literal sum values, maintain a mapping from the literal to a generated alias.
			// Later, we insert an operator that appends these constant literal values.
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
								String alias = "LITERAL_SUM_" + literalCounter;
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

			// If there is aggregation, and some SUM expressions referenced a literal value,
			// wrap the current operator with a LiteralAppendOperator.
			// This operator is assumed to add for each tuple the constant literal columns as specified.
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
				// System.out.println("SumExpressions: " + sumExpressions);
				// System.out.println("GroupByExpressions: " + groupByExpressions);
				// System.out.println("Initial schemaMapping: " + schemaMapping);
				// System.out.println("Child Operator: " + rootOperator);
				rootOperator = new SumOperator(rootOperator, groupByExpressions, sumExpressions, schemaMapping);
				if (rootOperator instanceof SumOperator) {
					schemaMapping = ((SumOperator) rootOperator).getSchemaMapping();
					// System.out.println("Schema mapping after aggregation: " + schemaMapping);
				}
			}

			// Process projection.
			// Determine if the projection is specific or a wildcard.
			selectItems = plainSelect.getSelectItems();
			boolean projectSpecific = true;
			if (selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*")) {
				projectSpecific = false;
			}

			if (projectSpecific) {
				if (hasAggregation) {
					// Check if GROUP BY is present: in that case the aggregated tuple contains both the group key(s) and the aggregate(s)
					if (plainSelect.getGroupBy() != null &&
							plainSelect.getGroupBy().getGroupByExpressions() != null &&
							!plainSelect.getGroupBy().getGroupByExpressions().isEmpty()) {
						// Build the projected column names from the SELECT items.
						// Typically, if a SELECT item is an aggregate (like "SUM(1)"), we output the aggregate column ("SUM").
						// For non-aggregated SELECT items (if any), we output "Group".
						List<String> aggregatedColumns = new ArrayList<>();
						for (SelectItem item : selectItems) {
							String itemStr = item.toString().trim().toUpperCase();
							if (itemStr.startsWith("SUM(")) {
								aggregatedColumns.add("SUM");
							} else {
								aggregatedColumns.add("Group");
							}
						}
						rootOperator = new ProjectionOperator(rootOperator,
								aggregatedColumns.toArray(new String[0]), schemaMapping);
						// Build a matching schema mapping.
						Map<String, Integer> projectedSchemaMapping = new LinkedHashMap<>();
						for (int i = 0; i < aggregatedColumns.size(); i++) {
							projectedSchemaMapping.put(aggregatedColumns.get(i), i);
						}
						schemaMapping = projectedSchemaMapping;
					} else {
						// Global aggregation (no GROUP BY).
						// In this case the aggregated tuple should contain only the aggregated values.
						List<String> aggregatedColumns = new ArrayList<>(schemaMapping.keySet());
						rootOperator = new ProjectionOperator(rootOperator,
								aggregatedColumns.toArray(new String[0]), schemaMapping);
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
					rootOperator = new ProjectionOperator(rootOperator,
							columnsToProject.toArray(new String[0]), schemaMapping);
					// Update schema mapping.
					Map<String, Integer> projectedSchemaMapping = new HashMap<>();
					for (int i = 0; i < columnsToProject.size(); i++) {
						projectedSchemaMapping.put(columnsToProject.get(i), i);
					}
					schemaMapping = projectedSchemaMapping;
				}
			}


			// Wrap the operator tree with DuplicateEliminationOperator if DISTINCT or GROUP BY is used.
			if (hasDistinct ||
					(plainSelect.getGroupBy() != null &&
							plainSelect.getGroupBy().getGroupByExpressions() != null &&
							!plainSelect.getGroupBy().getGroupByExpressions().isEmpty())) {
				rootOperator = new DuplicateEliminationOperator(rootOperator);
			}

			// Process ORDER BY elements.
			List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
			if (orderByElements != null && !orderByElements.isEmpty()) {
				rootOperator = new SortOperator(rootOperator, orderByElements, schemaMapping);
			}

			// Execute the final operator tree.
			execute(rootOperator, outputFile);

			Pattern pattern = Pattern.compile("query(\\d+)\\.csv");
			Matcher matcher = pattern.matcher(outputFile);
			String queryNumber = "";
			if (matcher.find()) {
				queryNumber = matcher.group(1);
			} else {
				System.err.println("Could not extract query number from output file path: " + outputFile);
			}
			String expectedOutputPath = "/Users/samlaborde-balen/Desktop/BlazeDB/samples/expected_output/query"
					+ queryNumber + ".csv";
			boolean comparisonResult = FileComparator.compareFiles(outputFile, expectedOutputPath);

		} catch (Exception e) {
			System.err.println("Error executing query plan: " + e.getMessage());
			e.printStackTrace();
		}
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


	/*
	 * Placeholder for a method to merge two schema mappings.
	 * A schema mapping is expected to be a map from fully qualified column names
	 * (e.g., "Student.A") to positions in a tuple.
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

		// System.out.println("Schema mapping: " + mapping);
		return mapping;
	}
}
