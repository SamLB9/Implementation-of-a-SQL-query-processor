# BlazeDB Query Processing

This project implements a basic query processor with a focus on efficient join evaluation. It handles the extraction of selection and join conditions from the WHERE clause and applies them in such a way as to reduce the size of intermediate results.

## Join Condition Extraction

The application processes the WHERE clause to distinguish between:

- **Selection Conditions**: Expressions that reference only one table (e.g., `R.A < R.B` or `S.C = 1`). These are applied as early as possible (typically at the **Scan/Select** operator level) so that only relevant tuples are processed downstream.
- **Join Conditions**: Expressions that reference columns from different tables (e.g., `R.D = S.G`). These are extracted and evaluated during the join operation, thereby avoiding costly cross product computations.

### Implementation Detail
The strategy for extracting join conditions from the WHERE clause is explained in detail within the code comments of **JoinOperator.java**. Please see the comments around the class declaration and the section that discusses tuple-nested-loop join processing. Specifically, the comments at the beginning of the file describe how conditions are differentiated and handled. This should help the grader quickly locate the explanation of the logic.

## Query Optimization Rules

1. **Push-Down Selections**: By applying selection conditions as close to the data source as possible, the amount of data that enters the join process is significantly reduced. This helps lower the computational cost as fewer tuples are combined during the join.

2. **Early Evaluation of Join Conditions**: Join conditions are specifically extracted from the WHERE clause and are evaluated during the join process rather than after performing a full cross product. This targeted evaluation prevents the creation of large intermediate results that may otherwise occur in a naïve cross-product approach.

3. **Tuple-Nested-Loop Join Efficiency**: The join is implemented as a tuple-nested-loop join where, for each tuple from the left (outer) operator, the entire right (inner) operator is scanned. Although this is a basic join algorithm, using early selection and join condition evaluation minimizes redundant calculations, thus reducing the intermediate result size.

These optimization strategies have been implemented under the assumption of a left-deep join tree where join operations follow the order specified in the `FROM` clause.

## Known Issues

- **Performance**: For large data sets, the tuple-nested-loop join may not be the most efficient join algorithm. Future work might explore more advanced join techniques to further optimize query processing.

- **Error Handling**: Some edge cases might not be fully covered by the current validation logic. Users are advised to test the system with a variety of queries to ensure all conditions are handled as expected.
