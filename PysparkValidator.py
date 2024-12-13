import streamlit as st
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
from pyspark.sql.utils import AnalysisException

# Initialize Spark Session
spark = SparkSession.builder.appName("DynamicCodeExecutor").master("local[*]").getOrCreate()

def validate_and_execute(code: str, schema1: dict, schema2: dict):
    """Validate PySpark code and execute it."""
    try:
        # Create DataFrames from schemas
        df1 = spark.createDataFrame([], schema=generate_struct_type(schema1))
        df2 = spark.createDataFrame([], schema=generate_struct_type(schema2))

        # Extract column data types for validation
        df1_schema = {field.name: field.dataType for field in df1.schema.fields}
        df2_schema = {field.name: field.dataType for field in df2.schema.fields}

        # Prepare namespace for executing code
        namespace = {'df1': df1, 'df2': df2}

        # Execute the provided PySpark code
        exec(code, namespace)

        # Retrieve the result DataFrame
        result_df = namespace.get('result')

        if result_df is None:
            return False, "Error: The code did not produce a DataFrame named 'result'.", None

        # Perform data type validation on used columns
        validate_column_operations(code, df1_schema, df2_schema)

        return True, "Execution successful!", result_df

    except SyntaxError as e:
        return False, f"Syntax error in the provided code: {str(e)}", None
    except AnalysisException as e:
        return False, f"Analysis error: {str(e)}", None
    except Exception as e:
        return False, f"An error occurred: {str(e)}", None

def generate_struct_type(schema_dict):
    """Generate a PySpark StructType from a dictionary."""
    field_type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType()
    }

    fields = []
    for column_name, data_type in schema_dict.items():
        if data_type not in field_type_map:
            raise ValueError(f"Unsupported data type: {data_type}")
        fields.append(StructField(column_name, field_type_map[data_type]))

    return StructType(fields)

def validate_column_operations(code: str, schema1: dict, schema2: dict):
    """Validate column operations in the PySpark code against the provided schemas."""
    import re

    # Extract column names and operations (e.g., "df1.age > 5")
    operations = re.findall(r'df1\.(\w+) *[><=!]+ *[\w\'"]+', code)
    operations += re.findall(r'df2\.(\w+) *[><=!]+ *[\w\'"]+', code)

    # Check each operation against its schema
    for op in operations:
        if op in schema1:
            column_type = schema1[op]
        elif op in schema2:
            column_type = schema2[op]
        else:
            raise ValueError(f"Column '{op}' not found in either schema.")

        # Validate data type for numeric operations
        if isinstance(column_type, StringType):
            raise ValueError(f"Column '{op}' is of type 'string' but is used in a numeric operation.")

def main():
    st.title("PySpark Code Validator")

    # Step 1: Define schemas
    st.header("Step 1: Define DataFrame Schemas (JSON format)")

    schema1_input = st.text_area("Schema for DataFrame 1", value='''{
        "customer_id": "string",
        "age": "string",
        "salary": "double"
    }''')

    schema2_input = st.text_area("Schema for DataFrame 2", value='''{
        "customer_id": "string",
        "product_id": "string",
        "quantity": "integer"
    }''')

    # Step 2: Enter PySpark code
    st.header("Step 2: Enter PySpark Code")
    code_input = st.text_area("PySpark code to execute", value='''# Example PySpark code
result = df1.filter(df1.age > 5).select("customer_id", "age")''')

    # Execute button
    if st.button("Validate and Execute Code"):
        try:
            # Parse schemas
            schema1 = json.loads(schema1_input)
            schema2 = json.loads(schema2_input)

            # Validate and execute the code
            is_valid, message, result_df = validate_and_execute(code_input, schema1, schema2)

            # Display results
            st.subheader("Results")
            if is_valid:
                st.success(message)
                st.dataframe(result_df.toPandas())  # Display the DataFrame
            else:
                st.error(message)

        except json.JSONDecodeError:
            st.error("Invalid JSON format in schemas.")
        except ValueError as e:
            st.error(str(e))

if __name__ == "__main__":
    main()
