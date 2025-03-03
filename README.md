# BlazeDB Query Processing

This project implements a basic query processor with a focus on efficient join evaluation. It handles the extraction of selection and join conditions from the WHERE clause and applies them in such a way as to reduce the size of intermediate results.

## Join Condition Extraction

In this project, I employ an advanced strategy for processing SQL queries by extracting join conditions from the WHERE clause. This technique is key to optimizing query performance.

**How It Works:**
- **Separation of Conditions:**
The WHERE clause often contains a mix of predicates. Some of these conditions are used to filter rows based on values from a single table (selection conditions), while others compare columns between two tables (join conditions). Our system analyzes the WHERE clause to differentiate between these two types.
- **Extraction and Application of Join Conditions:**
Conditions that involve columns from two different tables are extracted as join conditions. Instead of evaluating these after all rows have been combined, they are attached directly to the join operators. During the join process (typically a tuple-nested-loop join in our implementation), the system evaluates these conditions on-the-fly as tuples are combined. This prevents the generation of large intermediate result sets that would later require filtering, thus avoiding unnecessary Cartesian products.
- **Left-Deep Join Tree Structure:**
To further enhance performance, the join operations are organized in a left-deep tree structure. This means that joins are performed in the order specified by the FROM clause, with each join operator processing only those tuple combinations that satisfy its associated join condition.

**Benefits:**
- **Reduced Overhead:** Extracting join conditions minimizes the computational cost of handling full Cartesian products, as only promising tuple pairs are combined and evaluated.
- **Optimized Query Execution:** The clear separation between single-table and multi-table predicates enables the creation of an efficient operator tree, which is crucial for managing resource utilization and achieving fast query response times.

## Query Optimization Rules

To improve query performance and resource efficiency, I employ several optimization strategies. The main techniques implemented are **Predicate Pushdown**, **Projection Pruning**, and **Selection (Predicate) Combination**. Each of these optimizations contributes to reducing the volume and complexity of data processed during query evaluation.

### 1. Predicate Pushdown
**Description:**
Predicate pushdown moves selection conditions (typically derived from the WHERE clause) as close as possible to the data source. This optimization ensures that filtering happens at the earliest stage (e.g., during or immediately after the scan operation), thereby reducing the volume of data that flows through the operator tree.

**Why It Is Correct:**
- Applying a filter sooner in the execution plan does not change the semantics of the query because filtering conditions are logically independent of later operations like joins or projections.
- The transformation is safe as long as the pushed condition does not rely on computations performed later in the plan, ensuring the final result remains consistent with the original query.

**How It Reduces Intermediate Results:**
- By eliminating rows that do not meet the criteria at the source, subsequent operators (such as join or aggregation operators) work on fewer tuples.
- This early reduction in data minimizes processing overhead, memory consumption, and the risk of creating large intermediate result sets.

### 2. Projection Pruning
**Description:**
Projection pruning involves discarding columns that are not referenced later in the query. When a query only requires a subset of available columns, this optimization removes unnecessary data from the processing pipeline.

**Why It Is Correct:**
- The optimization follows the relational algebra principle where reducing the number of columns does not affect the correctness of the results if the omitted columns are not needed.
- It maintains query semantics by ensuring that the projected output still contains all columns explicitly required by the SELECT clause.

**How It Reduces Intermediate Results:**
- Intermediate results become smaller because each tuple contains only the essential columns.
- This reduction in data width decreases memory usage and improves I/O performance, particularly when processing large datasets or performing further operations like sorting and grouping.

### 3. Selection (Predicate) Combination
**Description:**
Selection combination involves merging multiple selection predicates into a single filtering operation. Instead of evaluating conditions separately, the query optimizer combines them into one composite predicate.

**Why It Is Correct:**
- Combining predicates is sound as long as the logical conjunction (AND) semantic is preserved.
- The unified filtering mechanism does not change the overall filtering criteria; it merely streamlines the evaluation process.

**How It Reduces Intermediate Results:**
- Evaluating a combined predicate in a single pass over the data can reduce the number of scans required, lowering CPU overhead.
- A single, integrated filtering step minimizes redundant processing and ensures that only qualifying rows continue through the execution plan.
- This consolidation minimizes the size of intermediate result sets by promptly filtering out tuples that fail any of the conditions, which is especially beneficial in complex queries with multiple filter conditions.

### 4. Early Evaluation of Join Conditions
**Description:**
When a SQL query contains join conditions, these conditions are often found within the WHERE clause. Instead of performing a complete cross product of all records from the joining tables and then filtering the results based on the join conditions, the optimizer extracts these join conditions and applies them immediately during the join process.

**Why It Is Correct:**
- **Correctness by Composition:** Since join conditions naturally relate records from two tables, applying them as soon as a pair of tuples is generated is equivalent to filtering the complete cross product later. The result remains the same, but the order of operations is rearranged.
- **Preservation of Semantics:** The early evaluation does not change the final output of the join. It simply avoids unnecessary work by filtering out unsatisfactory tuple combinations before they are fully constructed or passed on.

**How It Reduces Intermediate Results:**
- **Minimized Tuple Generation:** By evaluating the join condition on-the-fly, many combinations that would not meet the criteria are never fully materialized. This avoids the creation of large intermediate datasets that would otherwise result from a naïve cross-join first.
- **Resource Efficiency:** Early filtering reduces memory and CPU consumption because fewer tuples are passed through subsequent operators in the execution plan. This is particularly important when dealing with large datasets or complex queries.

### 5. Tuple-Nested-Loop Join Efficiency
**Description:**
The tuple-nested-loop join algorithm processes one tuple from the left (outer) relation at a time and then scans the entire right (inner) relation for matching tuples. Although this is a straightforward join algorithm, its efficiency can be significantly enhanced when combined with early selection.

**Why It Is Correct:**
- **Systematic Pairing:** The tuple-nested-loop join inherently examines all possible join pairs. By integrating the join condition evaluation within this process, the algorithm ensures that only valid pairs are fully processed.
- **Algorithmic Soundness:** Despite being one of the simplest forms of join processing, the tuple-nested-loop join remains correct for any join operation. It does not miss any valid joins, and the correctness is ensured as long as each potential pairing is considered and subjected to the join condition.

**How It Reduces Intermediate Results:**
- **Immediate Filtering:** Alongside iterating over each combination of left and right tuples, the immediate evaluation of the join condition prevents non-matching pairs from being stored or further processed. This keeps the size of the intermediate result set much smaller.
- **Reduced Redundancy:** By avoiding the accumulation of tuples that do not satisfy join conditions, the approach reduces redundant calculations. This, in turn, lessens the load on subsequent processing steps such as projection, aggregation, or sorting.
- **Efficiency in Memory Usage:** Since the unwanted tuples are discarded as soon as they are evaluated, the memory footprint remains low. This is particularly beneficial when processing large relations, as it prevents the potential ballooning of partially-joined data.
