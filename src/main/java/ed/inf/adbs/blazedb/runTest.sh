#!/bin/bash

# This function runs all queries and tests from 1 to 12.
run_all_tests() {
  mvn clean compile assembly:single
  echo ""
  echo ""
  echo ""
  echo ""
  echo ""
  echo ""
  echo ""
  # Run the query version tests (query1.sql ... query12.sql)
  for i in {1..12}; do
    echo ""
    echo -e "Running query file: query${i}.sql"
    java -jar target/blazedb-1.0.0-jar-with-dependencies.jar \
      samples/db \
      samples/input/query${i}.sql \
      samples/output/query${i}.csv
  done

  # Run the test version tests (test1.sql ... test12.sql)
  for i in {1..12}; do
    echo ""
    echo -e "Running test file: test${i}.sql"
    java -jar target/blazedb-1.0.0-jar-with-dependencies.jar \
      samples/db \
      samples/input/test${i}.sql \
      samples/output/test${i}.csv
  done

  # Run the test version tests (t1.sql ... t12.sql)
    for i in {1..11}; do
      echo ""
      echo -e "Running test file: t${i}.sql"
      java -jar target/blazedb-1.0.0-jar-with-dependencies.jar \
        samples/db \
        samples/input/t${i}.sql \
        samples/output/t${i}.csv
    done
}

# Call the function
run_all_tests