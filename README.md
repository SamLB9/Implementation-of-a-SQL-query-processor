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

1. **Push-Down Selections**: By applying selection conditions as close to the data source as possible, the amount of data that enters the join process is significantly reduced. This helps lower the computational cost as fewer tuples are combined during the join.

2. **Early Evaluation of Join Conditions**: Join conditions are specifically extracted from the WHERE clause and are evaluated during the join process rather than after performing a full cross product. This targeted evaluation prevents the creation of large intermediate results that may otherwise occur in a naïve cross-product approach.

3. **Tuple-Nested-Loop Join Efficiency**: The join is implemented as a tuple-nested-loop join where, for each tuple from the left (outer) operator, the entire right (inner) operator is scanned. Although this is a basic join algorithm, using early selection and join condition evaluation minimizes redundant calculations, thus reducing the intermediate result size.

These optimization strategies have been implemented under the assumption of a left-deep join tree where join operations follow the order specified in the `FROM` clause.

## Known Issues

- **Performance**: For large data sets, the tuple-nested-loop join may not be the most efficient join algorithm. Future work might explore more advanced join techniques to further optimize query processing.

- **Error Handling**: Some edge cases might not be fully covered by the current validation logic. Users are advised to test the system with a variety of queries to ensure all conditions are handled as expected.
