import streamlit as st
from snowflake.snowpark.context import get_active_session
import _snowflake
import json
import yaml
import io
import re
import pandas as pd
from io import StringIO, BytesIO
from fpdf import FPDF

st.set_page_config(layout="wide")


def display_content(content: list, message_index: int = None) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(
                            ["Data", "Line Chart", "Bar Chart"]
                        )
                        data_tab.dataframe(df)
                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])
                        with line_tab:
                            st.line_chart(df)
                        with bar_tab:
                            st.bar_chart(df)
                    else:
                        st.dataframe(df)


def new_page_function():
    st.title("Compare Models: CA vs GPT-4o")

    # Step 1: Create two columns for Cortex Analyst and GPT-4 Query Interface
    col1, col2 = st.columns(2)

    # Step 2: Create a single user input for both columns
    user_input = st.text_input("Ask a query to both Cortex Analyst and GPT-4:")

    # Only run the functions if the user has inputted a query
    if user_input:
        with col1:
            # Run Cortex Analyst function with user input
            cortex_analyst_for_3rd_page(user_input)

        with col2:
            gpt4_query_for_3rd_page(user_input)


def report_page_function():
    st.title("Compare Models: CA vs GPT-4")
    st.title("Combined Cortex Analyst and GPT-4 Query Interface")

    session = get_active_session()
    DATABASE = "CORTEX_ANALYST_DEMO"
    SCHEMA = "REVENUE_TIMESERIES"
    STAGE = "RAW_DATA"
    FILE = "questions.csv"

    stage_path = f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}"

    file_stream = session.file.get_stream(stage_path)

    csv_data = StringIO(file_stream.read().decode('utf-8'))
    pandas_df = pd.read_csv(csv_data)

    first_column = pandas_df.iloc[:, 0]

    # Display questions from the dataset
    st.write("Questions from your dataset:")
    st.dataframe(first_column)

    # List to store results for PDF generation
    results = []

    # Button to start processing questions
    if st.button("Submit and Process Questions"):
        for question in first_column:
            st.write(f"Processing question: {question}")
            query_result, query_result_str = gpt4_query_for_3rd_page(question)
            # Capture the query result as string
            summarized_result = summarize_gpt(query_result_str)
            results.append((question, summarized_result))

        # Generate PDF report
        pdf = FPDF()
        pdf.add_page()

        # Set up fonts and styles
        pdf.set_font("Arial", "B", 16)  # Bold font for title
        pdf.cell(200, 10, "GPT-4 Query Report", ln=True, align='C')

        pdf.set_font("Arial", "B", 12)  # Bold font for section headers

        for question, summarized_result in results:
            pdf.ln(5)  # Reduced gap between entries

            # Question section
            pdf.cell(0, 10, "Question:", ln=True)
            pdf.set_font("Arial", "B", 12)  # Regular font for question text
            pdf.multi_cell(0, 10, question)

            # Answer section
            pdf.set_font("Arial", "B", 12)
            pdf.cell(0, 10, "Answer:", ln=True)
            pdf.set_font("Arial", "B", 12)

            # Render the summarized result, including markdown headings
            render_markdown_in_pdf(pdf, summarized_result)

        # Save PDF to a string
        pdf_output = pdf.output(dest='S').encode('latin1')

        # Convert the string to a BytesIO object
        pdf_buffer = BytesIO(pdf_output)

        # Button to download PDF
        st.download_button(
            label="Download PDF Report",
            data=pdf_buffer,
            file_name="GPT4_Query_Report.pdf",
            mime="application/pdf"
        )


def render_markdown_in_pdf(pdf, text):
    """
    Render basic Markdown in the PDF.
    Supports:
      - ### Heading (large)
      - #### Heading (smaller)
      - **bold**
      - *italic*
      - Line breaks
    """
    bold_pattern = r'\*\*(.*?)\*\*'
    italic_pattern = r'\*(.*?)\*'
    heading_3_pattern = r'### (.*?)\n'
    heading_4_pattern = r'#### (.*?)\n'

    tokens = re.split(
        f"({bold_pattern}|{italic_pattern}|{heading_3_pattern}|{heading_4_pattern}|\n)", text)

    for token in tokens:
        if token is None:
            continue

        # Handle heading 3 (###)
        if re.match(heading_3_pattern, token):
            pdf.set_font("Arial", "B", 14)  # Larger font for ### headings
            pdf.multi_cell(0, 10, re.sub(r'### ', '', token))

        # Handle heading 4 (####)
        elif re.match(heading_4_pattern, token):
            # Slightly smaller for #### headings
            pdf.set_font("Arial", "B", 12)
            pdf.multi_cell(0, 10, re.sub(r'#### ', '', token))

        # Bold formatting
        elif re.match(bold_pattern, token):
            pdf.set_font("Arial", "B", 12)
            pdf.multi_cell(0, 10, re.sub(r'\*\*', '', token))

        # Italic formatting
        elif re.match(italic_pattern, token):
            pdf.set_font("Arial", "I", 12)
            pdf.multi_cell(0, 10, re.sub(r'\*', '', token))

        # New line
        elif token == '\n':
            pdf.ln(5)

        # Regular text
        else:
            pdf.set_font("Arial", "", 12)
            pdf.multi_cell(0, 10, token)


def gpt4_query_for_3rd_page(user_input):
    st.subheader("GPT-4 Query Interface")
    session = get_active_session()

    if "json_data_str" not in st.session_state:
        temp = generate_metadata_string(session)
        st.session_state.json_data_str = temp.replace("'", "")
    json_data_str = st.session_state.json_data_str

    with st.expander("Schema metadata", expanded=False):
        st.info(json_data_str)

    def run_query(query):
        try:
            result = session.sql(query).collect()
            return result
        except Exception as e:
            return str(e)

    if "messages_gpt" not in st.session_state:
        st.session_state.messages_gpt = []

    if user_input:
        st.session_state.messages_gpt.append(
            {"role": "user", "content": user_input})
        result = session.sql(
            f"SELECT CHATGPT_4('{user_input}', '{json_data_str}')").collect()
        response = result[0][0]
        response = remove_sql_markers(response)
        response = response.replace('`', '')

        with st.expander("See GPT-4 Generated SQL Query", expanded=False):
            st.info(response)

        query_result = run_query(response)
        if isinstance(query_result, pd.DataFrame):
            query_result_str = query_result.to_string(
                index=False)  # Convert DataFrame to string
        else:
            query_result_str = str(query_result)
        st.write("**GPT-4 Response:**")
        try:
            st.dataframe(query_result)
        except:
            st.write(query_result)

        st.session_state.messages_gpt.append(
            {"role": "assistant", "content": query_result})
        return query_result, query_result_str
    else:
        st.write("Please enter a query.")


def cortex_analyst_for_3rd_page(user_input):
    DATABASE = st.session_state['database']
    SCHEMA = st.session_state['schema']
    STAGE = st.session_state['stage']
    FILE = st.session_state['yaml_file']

    def send_message(prompt: str) -> dict:
        """Calls the REST API and returns the response."""
        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        }
        resp = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )
        if resp["status"] < 400:
            return json.loads(resp["content"])
        else:
            raise Exception(
                f"Failed request with status {resp['status']}: {resp}"
            )

    def process_message(prompt: str) -> None:
        """Processes a message and adds the response to the chat."""
        st.session_state.messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Generating response..."):
                response = send_message(prompt=prompt)
                content = response["message"]["content"]
                st.write(content)
                display_content(content=content)

        st.session_state.messages.append(
            {"role": "assistant", "content": content})

    st.subheader("Cortex Analyst")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            display_content(msg["content"], message_index=i)

    process_message(prompt=user_input)


def main():
    session = get_active_session()
    st.sidebar.title("Navigation")
    page_selection = st.sidebar.radio(
        "Go to", ["Cortex Analyst", "GPT-4 Query Interface", "Compare Models", "Reports"])

    if 'database' not in st.session_state:
        st.session_state['database'] = "CORTEX_ANALYST_DEMO"
    if 'schema' not in st.session_state:
        st.session_state['schema'] = "REVENUE_TIMESERIES"
    if 'stage' not in st.session_state:
        st.session_state['stage'] = "RAW_DATA"
    if 'yaml_file' not in st.session_state:
        st.session_state['yaml_file'] = f"demo_{st.session_state['database']}_{st.session_state['schema']}.yaml"
    if 'json_file' not in st.session_state:
        st.session_state['json_file'] = f"demo_{st.session_state['database']}_{st.session_state['schema']}.json"

    # User inputs for database, schema, and stage in the sidebar
    # st.sidebar.header("Configuration Settings")

    # User inputs for database, schema, and stage using dropdowns
    database_options = ["CORTEX_ANALYST_DEMO",
                        "ANOTHER_DATABASE"]  # Add more options as needed
    # Add more options as needed
    schema_options = ["REVENUE_TIMESERIES", "ANOTHER_SCHEMA"]
    # Add more options as needed
    stage_options = ["RAW_DATA", "ANOTHER_STAGE"]

    database_input = st.sidebar.selectbox(
        "Select Database Name:", options=database_options, index=0, key='database_input')
    schema_input = st.sidebar.selectbox(
        "Select Schema Name:", options=schema_options, index=0, key='schema_input')
    stage_input = st.sidebar.selectbox(
        "Select Stage Name:", options=stage_options, index=0, key='stage_input')

    st.session_state['database'] = database_input
    st.session_state['schema'] = schema_input
    st.session_state['stage'] = stage_input

    st.session_state['yaml_file'] = f"demo_{st.session_state['database']}_{st.session_state['schema']}.yaml"
    st.session_state['json_file'] = f"demo_{st.session_state['database']}_{st.session_state['schema']}.json"

    # Display current configurations
    with st.sidebar.expander("Current Configuration", expanded=False):
        st.markdown(f"**Database:** {st.session_state['database']}")
        st.markdown(f"**Schema:** {st.session_state['schema']}")
        st.markdown(f"**Stage:** {st.session_state['stage']}")
        st.markdown(f"**YAML File:** {st.session_state['yaml_file']}")
        st.markdown(f"**JSON File:** {st.session_state['json_file']}")

    def generate_yaml_json_files():
        # YAML structure initialization
        yaml_structure = {
            "name": "Revenue",
            "tables": []
        }

        # Get active Snowflake session
        session = get_active_session()

        # Fetch tables and views for the predefined schema
        tables_df = session.sql(
            f"SHOW TABLES IN {st.session_state['database']}.{st.session_state['schema']}").collect()
        views_df = session.sql(
            f"SHOW VIEWS IN {st.session_state['database']}.{st.session_state['schema']}").collect()

        # Combine tables and views
        tables_and_views = [row['name']
                            for row in tables_df] + [row['name'] for row in views_df]

        # Parse each table or view and add to the YAML structure
        for table_or_view in tables_and_views:
            # Get the definition of the table or view
            table_definition_df = session.sql(
                f"DESCRIBE TABLE {st.session_state['database']}.{st.session_state['schema']}.{table_or_view}").collect()

            columns = [row['name'] for row in table_definition_df]
            data_types = [row['type'] for row in table_definition_df]

            # Define the table entry in YAML structure
            table_entry = {
                "name": table_or_view,
                "description": f"Description of {table_or_view}",
                "base_table": {
                    "database": st.session_state['database'],
                    "schema": st.session_state['schema'],
                    "table": table_or_view
                },
                "dimensions": [],
                "time_dimensions": [],
                "measures": []
            }

            # Add columns as time dimensions and measures
            for column, data_type in zip(columns, data_types):
                data_type_simple = data_type.split('(')[0].upper()

                # Time dimensions
                if data_type_simple in ["DATE", "DATETIME", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"]:
                    time_dimension_entry = {
                        "name": column,
                        "expr": column,
                        "description": f"",
                        "unique": True,
                        "data_type": data_type_simple
                    }
                    table_entry["time_dimensions"].append(time_dimension_entry)

                # Measures
                elif data_type_simple in ["NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"]:
                    measure_entry = {
                        "name": column,
                        "expr": column,
                        "description": f"",
                        "data_type": data_type_simple,
                        "default_aggregation": "sum"  # Example default aggregation
                    }
                    table_entry["measures"].append(measure_entry)

                else:
                    dim_entry = {
                        "name": column,
                        "expr": column,
                        "description": f"",
                        "data_type": data_type_simple,
                        "sample_values": ["", ""]
                    }
                    table_entry["dimensions"].append(dim_entry)

            # Append table entry to YAML structure
            yaml_structure['tables'].append(table_entry)

        # Convert YAML structure to string
        yaml_str = yaml.dump(yaml_structure, sort_keys=False, indent=2)

        # Save the YAML content to a file
        yaml_bytes = io.BytesIO(yaml_str.encode('utf-8'))

        # Write the YAML content to a file with the specified name
        FILE = st.session_state['yaml_file']
        with open(FILE, 'wb') as f:
            f.write(yaml_bytes.getvalue())

        # Upload the YAML file to the Snowflake stage
        try:
            # Upload to Snowflake stage with the specified file name
            put_result = session.file.put(
                FILE, st.session_state['stage'], auto_compress=False, overwrite=True)

            if put_result[0].status == 'UPLOADED':
                st.sidebar.success(
                    "YAML file successfully generated and uploaded to stage.")
            else:
                st.error("Failed to upload YAML file to stage.")

        except Exception as e:
            st.error(f"Failed to upload YAML file to stage: {e}")

        # JSON file generation and upload
        json_data = json.dumps(yaml_structure, indent=2)
        json_bytes = io.BytesIO(json_data.encode('utf-8'))

        # Write the JSON content to a file with the specified name
        JSON_FILE = st.session_state['json_file']
        with open(JSON_FILE, 'wb') as f:
            f.write(json_bytes.getvalue())

        # Upload the JSON file to the Snowflake stage
        try:
            put_result_json = session.file.put(
                JSON_FILE, f"@{st.session_state['stage']}/{JSON_FILE}", auto_compress=False, overwrite=True)

            # Check if the upload was successful
            if put_result_json[0].status == 'UPLOADED':
                st.sidebar.success(
                    "JSON file successfully generated and uploaded to stage.")
            else:
                st.error("Failed to upload JSON file to stage.")

        except Exception as e:
            st.error(f"Error uploading JSON file to stage: {str(e)}")

    if st.sidebar.button("Run Function"):
        generate_yaml_json_files()

    def cortex_analyst_page():
        DATABASE = st.session_state['database']
        SCHEMA = st.session_state['schema']
        STAGE = st.session_state['stage']
        FILE = st.session_state['yaml_file']

        def send_message(prompt: str) -> dict:
            """Calls the REST API and returns the response."""
            request_body = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ],
                "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
            }
            resp = _snowflake.send_snow_api_request(
                "POST",
                f"/api/v2/cortex/analyst/message",
                {},
                {},
                request_body,
                {},
                30000,
            )
            if resp["status"] < 400:
                return json.loads(resp["content"])
            else:
                raise Exception(
                    f"Failed request with status {resp['status']}: {resp}"
                )

        def process_message(prompt: str) -> None:
            """Processes a message and adds the response to the chat."""
            st.session_state.messages.append(
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            )
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                with st.spinner("Generating response..."):
                    response = send_message(prompt=prompt)
                    content = response["message"]["content"]
                    with st.expander("Detailed Output Content", expanded=False):
                        st.markdown(content)
                    display_content(content=content)
            st.session_state.messages.append(
                {"role": "assistant", "content": content})

        st.title("Cortex Analyst")
        st.sidebar.title("Settings")

        if "messages" not in st.session_state:
            st.session_state.messages = []

        for i, msg in enumerate(st.session_state.messages):
            with st.chat_message(msg["role"]):
                display_content(msg["content"], message_index=i)

        if prompt := st.chat_input("Ask me anything about the data"):
            process_message(prompt=prompt)

    if page_selection == "Cortex Analyst":
        cortex_analyst_page()
    elif page_selection == "GPT-4 Query Interface":
        gpt4_query_interface_page()
    elif page_selection == "Compare Models":  # Add your new page name here
        new_page_function()
    elif page_selection == "Reports":  # Add your new page name here
        report_page_function()


# def fetch_table_ddl(session):
#     ddl_statements = []
#     schema_name =st.session_state['schema']
#     database_name = st.session_state['database']
#     # Query to get the list of tables
#     table_query = f"""
#     SELECT TABLE_NAME
#     FROM {database_name}.INFORMATION_SCHEMA.TABLES
#     WHERE TABLE_SCHEMA = '{schema_name}';
#     """

#     # Execute the query to get table names
#     tables_df = session.sql(table_query).collect()

#     # Fetch the DDL for each table
#     for table_row in tables_df:
#         table_name = table_row['TABLE_NAME']
#         ddl_query = f"SELECT GET_DDL('TABLE', '{database_name}.{schema_name}.{table_name}');"

#         # Execute the query to get the DDL statement
#         ddl_result = session.sql(ddl_query).collect()

#         # Extract the DDL string from the Row object
#         ddl_statements.append(ddl_result[0][0])

#     return ddl_statements

def fetch_table_ddl(database_name, schema_name, session):
    # session = get_active_session()
    ddl_statements = []

    # Query to get the list of tables
    table_query = f"""
    SELECT TABLE_NAME
    FROM {database_name}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema_name}';
    """

    # Execute the query to get table names
    tables_df = session.sql(table_query).collect()

    # Fetch the DDL for each table
    for table_row in tables_df:
        table_name = table_row['TABLE_NAME']
        ddl_query = f"SELECT GET_DDL('TABLE', '{database_name}.{schema_name}.{table_name}');"

        # Execute the query to get the DDL statement
        ddl_result = session.sql(ddl_query).collect()

        # Extract the DDL string from the Row object
        ddl_statements.append(ddl_result[0][0])

    return ddl_statements


def fetch_column_details(database_name, schema_name, table_name, session):
    # Query to get column details
    # session = get_active_session()
    describe_query = f"DESCRIBE TABLE {database_name}.{schema_name}.{table_name};"
    columns_df = session.sql(describe_query).collect()

    column_details = []
    for column in columns_df:
        column_name = column['name']

        # Query to count unique values
        unique_count_query = f"SELECT COUNT(DISTINCT {column_name}) AS UNIQUE_COUNT FROM {database_name}.{schema_name}.{table_name};"
        unique_count_result = session.sql(unique_count_query).collect()
        unique_count = unique_count_result[0]['UNIQUE_COUNT']

        # Query to get 5 random unique values
        random_values_query = f"""
        SELECT DISTINCT {column_name}
        FROM {database_name}.{schema_name}.{table_name}
        ORDER BY RANDOM()
        LIMIT 5;
        """
        random_values_result = session.sql(random_values_query).collect()
        random_values = [row[column_name] for row in random_values_result]

        # Store details
        column_details.append({
            'column_name': column_name,
            'unique_count': unique_count,
            'random_values': random_values
        })

    return column_details


def generate_metadata_string(session):
    schema_name = st.session_state['schema']
    database_name = st.session_state['database']
    # Fetch the DDL statements
    ddls = fetch_table_ddl(database_name, schema_name, session)

    # Initialize a list to hold the metadata details
    metadata_lines = []

    # Combine the DDL statements with additional column metadata
    metadata_lines.append("DDL Statements and Column Details:\n")
    for ddl in ddls:
        table_name = ddl.split('TABLE')[1].split(
            '(')[0].strip()  # Extract table name from DDL statement
        metadata_lines.append(f"\nTable: {table_name}\n")
        metadata_lines.append(f"{ddl}\n")

        # Fetch column details for the current table
        column_details = fetch_column_details(
            database_name, schema_name, table_name, session)

        # Add column metadata
        for column in column_details:
            metadata_lines.append(f"Column: {column['column_name']}")
            metadata_lines.append(
                f"  - Unique Count: {column['unique_count']}")
            metadata_lines.append(
                f"  - Random Unique Values: {column['random_values']}\n")

    # Join all lines into a single metadata string
    metadata_string = "\n".join(metadata_lines)
    return metadata_string


def remove_sql_markers(text):
    pattern = r"```sql(.*?)```"
    cleaned_text = re.sub(pattern, r"\1", text, flags=re.DOTALL)
    return cleaned_text.strip()

# Function for GPT-4 Query Interface Page


def gpt4_query_interface_page():

    st.title("GPT-4 Query Interface")
    session = get_active_session()

    if "json_data_str" not in st.session_state:
        temp = generate_metadata_string(session)
        st.session_state.json_data_str = temp.replace("'", "")
    json_data_str = st.session_state.json_data_str

    # print(metadata_string)
    # json_data_str_l = generate_metadata_string(session)
    # json_data_str = '\n'.join(metadata_string)
    with st.expander("Schema metadata", expanded=False):
        st.info(json_data_str)
    # st.write(json_data_str)

    def run_query(query):
        try:
            result = session.sql(query).collect()
            return result
        except Exception as e:
            return str(e)

    if "messages_gpt" not in st.session_state:
        st.session_state.messages_gpt = []

    # for i, msg in enumerate(st.session_state.messages_gpt):
    #     with st.chat_message(msg["role"]):
    #         display_content(msg["content"], message_index=i)

    user_input = st.chat_input("Ask anything:")

    if user_input:
        # Display the user's message in the chat message container
        with st.chat_message("user"):
            st.markdown(user_input)

        # Append the user message to the session state
        st.session_state.messages_gpt.append(
            {"role": "user", "content": user_input})

        # Query the Snowflake UDF
        with st.spinner("Generating response..."):
            result = session.sql(
                f"SELECT CHATGPT_4_md('{user_input}', '{json_data_str}')").collect()
            response = result[0][0]
            response = remove_sql_markers(response)
            response = response.replace('`', '')

        # Display the assistant's message in the chat message container
        with st.chat_message("assistant"):
            with st.expander("See GPT-4 Generated SQL Query", expanded=False):
                st.info(response)

            # Run the generated SQL query
            query_result = run_query(response)

            # Display the result
            if isinstance(query_result, str):
                st.markdown(f"**Error:** {query_result}")
            else:
                st.dataframe(query_result)

            # Append the assistant's response to the session state
            st.session_state.messages_gpt.append(
                {"role": "assistant", "content": str(query_result)})


def summarize_gpt(result):
    session = get_active_session()

    # Escape special characters in the result string
    safe_result = result.replace("'", "''")  # Escape single quotes

    # Use the escaped string in the SQL query
    sql_query = f"SELECT CHATGPT_4_summarize('{safe_result}')"

    try:
        # Run the query
        result = session.sql(sql_query).collect()
        response = result[0][0]
        return response
    except Exception as e:
        return str(e)


if __name__ == "__main__":
    main()
