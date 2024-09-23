import streamlit as st
from snowflake.snowpark.context import get_active_session
import _snowflake
import json
import yaml
import io
import re


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

def main():
    session = get_active_session()
    st.sidebar.title("Navigation")
    page_selection = st.sidebar.radio("Go to", ["Cortex Analyst", "GPT-4 Query Interface"])

    if 'database' not in st.session_state:
        st.session_state['database'] = "CORTEX_ANALYST_DEMO"
    if 'schema' not in st.session_state:
        st.session_state['schema'] = "PRODUCT_SALES"
    if 'stage' not in st.session_state:
        st.session_state['stage'] = "RAW_DATA"
    if 'yaml_file' not in st.session_state:
        st.session_state['yaml_file'] = f"demo_{st.session_state['database']}_{st.session_state['schema']}.yaml"
    if 'json_file' not in st.session_state:
        st.session_state['json_file'] = f"demo_{st.session_state['database']}_{st.session_state['schema']}.json"

    # User inputs for database, schema, and stage in the sidebar
    # st.sidebar.header("Configuration Settings")
    
    # User inputs for database, schema, and stage using dropdowns
    database_options = ["CORTEX_ANALYST_DEMO", "ANOTHER_DATABASE"]  # Add more options as needed
    schema_options = ["PRODUCT_SALES", "ANOTHER_SCHEMA"]       # Add more options as needed
    stage_options = ["RAW_DATA", "ANOTHER_STAGE"]                   # Add more options as needed
    
    database_input = st.sidebar.selectbox("Select Database Name:", options=database_options, index=0, key='database_input')
    schema_input = st.sidebar.selectbox("Select Schema Name:", options=schema_options, index=0, key='schema_input')
    stage_input = st.sidebar.selectbox("Select Stage Name:", options=stage_options, index=0, key='stage_input')

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
        tables_df = session.sql(f"SHOW TABLES IN {st.session_state['database']}.{st.session_state['schema']}").collect()
        views_df = session.sql(f"SHOW VIEWS IN {st.session_state['database']}.{st.session_state['schema']}").collect()

        # Combine tables and views
        tables_and_views = [row['name'] for row in tables_df] + [row['name'] for row in views_df]

        # Parse each table or view and add to the YAML structure
        for table_or_view in tables_and_views:
            # Get the definition of the table or view
            table_definition_df = session.sql(f"DESCRIBE TABLE {st.session_state['database']}.{st.session_state['schema']}.{table_or_view}").collect()

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
            put_result = session.file.put(FILE, st.session_state['stage'], auto_compress=False, overwrite=True)

            if put_result[0].status == 'UPLOADED':
                st.sidebar.success("YAML file successfully generated and uploaded to stage.")
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
            put_result_json = session.file.put(JSON_FILE, f"@{st.session_state['stage']}/{JSON_FILE}", auto_compress=False, overwrite=True)

            # Check if the upload was successful
            if put_result_json[0].status == 'UPLOADED':
                st.sidebar.success("JSON file successfully generated and uploaded to stage.")
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
                    st.write(content)
                    display_content(content=content)
            st.session_state.messages.append({"role": "assistant", "content": content})


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


def fetch_table_ddl(session):
    ddl_statements = []
    schema_name =st.session_state['schema']
    database_name = st.session_state['database']
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

def remove_sql_markers(text):
    pattern = r"```sql(.*?)```"
    cleaned_text = re.sub(pattern, r"\1", text, flags=re.DOTALL)
    return cleaned_text.strip()

# Function for GPT-4 Query Interface Page
def gpt4_query_interface_page():
    
    st.title("GPT-4 Query Interface")
    session = get_active_session()

    json_data_str_l = fetch_table_ddl(session)
    json_data_str = '\n'.join(json_data_str_l)
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
            
    user_input = st.text_input("Ask anything:")
 
    if st.button("Submit"):
        if user_input:
            # Query the Snowflake UDF
            st.session_state.messages_gpt.append({"role": "user", "content": user_input})
            result = session.sql(f"SELECT CHATGPT_4_md('{user_input}', '{json_data_str}')").collect()
            response = result[0][0]
            response = remove_sql_markers(response)
            response = response.replace('`','')
            with st.expander("See GPT-4 Generated SQL Query", expanded=False):
                st.info(response)
            query_result = run_query(response)
            st.write("**GPT-4 Response:**")
            try:
                st.dataframe(query_result)
            except:
                st.write(query_result)

            st.session_state.messages_gpt.append({"role": "assistant", "content": query_result})
            
        else:
            st.write("Please enter a query.")

            


if __name__ == "__main__":
    main()
