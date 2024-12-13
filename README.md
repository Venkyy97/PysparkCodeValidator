# Dynamic PySpark Code Executor

## Overview

The Dynamic PySpark Code Executor is a Streamlit application that enables users to input JSON schemas for two DataFrames and execute PySpark code dynamically. The app validates the code against these schemas and provides feedback on any syntax or data type mismatches.

## Features

- **Schema Input**: Specify schemas for `df1` and `df2` in JSON format.
- **Code Execution**: Write and execute PySpark code using `df1` and `df2`.
- **Validation**: Checks for syntax errors, column existence, and data type mismatches.
- **Feedback**: Provides detailed error messages for troubleshooting.

## How It Works

1. **Define Schemas**:
   - Input JSON schemas for DataFrames `df1` and `df2`.
   - Example schema:
     ```json
     {
         "customer_id": "string",
         "age": "integer",
         "salary": "double"
     }
     ```

2. **Enter PySpark Code**:
   - Provide PySpark code using `df1` and `df2`.
   - Example code:
     ```python
     result = df1.join(df2, "customer_id").filter(df1.age > 25).select("customer_id", "product_id")
     ```

3. **Validate and Execute**:
   - Click "Validate and Execute Code" to run validation checks and execute the code.
   - Feedback on execution success or error messages is displayed.

## Errors Captured

1. **Syntax Errors**: Detects invalid Python syntax in the provided code.
2. **Column Existence**: Validates that all columns used in the code exist in the schemas.
3. **Join Condition Errors**: Ensures that join columns exist in both DataFrames.
4. **Data Type Mismatch**: Validates that operations are suitable for the data types specified in the schema.
5. **Unsupported Data Types**: Identifies unsupported or misspelled data types in the schema input.

## Requirements

- Python 3.x
- Streamlit
- PySpark

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Venkyy97/PysparkCodeValidator.git

