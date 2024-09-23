import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import io
import yaml

# Function to show the welcome page


def show_welcome_page():
    st.title("Cortex Analyst YAML Generator")
    st.markdown("Powered by Streamlit in Snowflake :snowflake:")

    st.markdown("""
### Description

The YAML File generator app simplifies creating YAML files for [Snowflake's Cortex Analyst](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/cortex-analyst-overview#overview). Users can select databases, schemas, and tables or views, and the tool auto-fills the YAML structure with relevant table or view info. The generated files can be downloaded or copied for use in creating semantic models for Cortex Analyst. _While it quickly populates most required fields, some additional details will still need to be added_.

#### Instructions

1. **Get Started Page:**
   - Enter Semantic Name: Input the name of your semantic model.
   - Enter Description: Provide a detailed description of the semantic model.
   - Click on "Save Semantic Model Info" to save the details and proceed to the Table Definition page.

2. **Table Definition Page:**
   - Select Database: Choose the database from the dropdown menu.
   - Select Schema: Select the schema associated with the chosen database.
   - Select Table or View: Pick the table or view you want to include in the YAML file. 
   - Click on "Add Table or View to YAML" to add the selected table or view to the YAML structure. The YAML display will be updated accordingly.
   - Download YAML: Once your tables or views are added, you can download the generated YAML file by clicking on the "Download YAML file" button.

3. **Reset Application:**
   - Navigate to the "Reset" page from the sidebar.
   - Click the "Reset" button to clear all saved data and reset the application to its initial state.

For further details, refer to the [Snowflake Documentation](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/semantic-model-spec#label-semantic-model-tips).

Feel free to navigate between pages using the sidebar navigation to complete your YAML file creation.
""")


st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTGtsjtT26xLbvGO_eRAcJJ2drgv6wC9S7REQ&s")


def show_get_started_page():
    st.title("Get Started")
    semantic_name = st.text_input("Enter Semantic Name")
    description = st.text_area("Enter Description of Semantic Model")

    if st.button("Save Semantic Model Info"):
        st.session_state['semantic_name'] = semantic_name
        st.session_state['description'] = description
        st.session_state['tables'] = []
        st.session_state['yaml_structure'] = {
            "name": semantic_name,
            "description": description,
            "tables": []
        }
        st.success(
            "Semantic Model info saved successfully! Click on Table Definition on the navigation menu to finish YAML file creation")
        st.experimental_set_query_params(page="Table Definition")

# Function to show the table definition page


def show_table_definition_page():
    session = get_active_session()

    # Show databases and create a select box for the database selection
    databases_df = session.sql("SHOW DATABASES").collect()
    databases = [row['name'] for row in databases_df]
    database_selector = st.selectbox("Select Database", databases)

    # Show schemas based on the selected database and create a select box for schema selection
    schemas_df = session.sql(
        f"SHOW SCHEMAS IN DATABASE {database_selector}").collect()
    schemas = [row['name'] for row in schemas_df]
    schema_selector = st.selectbox("Select Schema", schemas)

    # Show tables and views based on the selected schema and create a select box for table or view selection
    tables_df = session.sql(
        f"SHOW TABLES IN {database_selector}.{schema_selector}").collect()
    views_df = session.sql(
        f"SHOW VIEWS IN {database_selector}.{schema_selector}").collect()
    tables_and_views = [row['name']
                        for row in tables_df] + [row['name'] for row in views_df]
    table_or_view_selector = st.selectbox(
        "Select Table or View", tables_and_views)

    # Display the current YAML structure
    yaml_template = {
        "name": "<name>",
        "description": "<string>",
        "tables": [
            {
                "name": "<name>",
                "description": "<string>",
                "base_table": {
                    "database": "<database>",
                    "schema": "<schema>",
                    "table": "<base table name>"
                },
                "dimensions": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>",
                        "data_type": "<data type>",
                        "unique": False
                    }
                ],
                "time_dimensions": [
                    {
                        "name": "date",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "date",
                        "data_type": "date",
                        "unique": True
                    }
                ],
                "measures": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>",
                        "data_type": "<data type>",
                        "default_aggregation": "<aggregate function>"
                    }
                ],
                "filters": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>"
                    }
                ]
            }
        ]
    }

    if 'yaml_structure' not in st.session_state:
        st.session_state['yaml_structure'] = yaml_template

    # Add table or view to YAML structure
    if st.button("Add Table or View to YAML"):
        if len(st.session_state['tables']) < 3:
            st.session_state['tables'].append(table_or_view_selector)

            table_definition_df = session.sql(
                f"DESCRIBE TABLE {database_selector}.{schema_selector}.{table_or_view_selector}").collect()

            columns = [row['name'] for row in table_definition_df]
            data_types = [row['type'] for row in table_definition_df]

            # Add table or view definition to YAML structure
            table_entry = {
                "name": table_or_view_selector,
                "description": "",
                "base_table": {
                    "database": database_selector,
                    "schema": schema_selector,
                    "table": table_or_view_selector
                },
                "dimensions": [],
                "time_dimensions": [],
                "measures": [],
                "filters": [
                    {
                        "name": "<name>",
                        "synonyms": ["<array of strings>"],
                        "description": "<string>",
                        "expr": "<SQL expression>"
                    }
                ]
            }

            # Check for time dimensions, dimension columns, and measure columns
            for column, data_type in zip(columns, data_types):
                data_type_simple = data_type.split('(')[0].upper()
                if data_type_simple in ["DATE", "DATETIME", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"]:
                    time_dimension_entry = {
                        "name": column,
                        "expr": column,
                        "description": "<string>",
                        "unique": True,
                        "data_type": data_type_simple,
                        "synonyms": ["<array of strings>"]
                    }
                    table_entry["time_dimensions"].append(time_dimension_entry)
                if data_type_simple in ["VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "BINARY", "VARBINARY"]:
                    dimension_entry = {
                        "name": column,
                        "expr": column,
                        "description": "<string>",
                        "data_type": data_type_simple,
                        "unique": False,
                        "synonyms": ["<array of strings>"]
                    }
                    table_entry["dimensions"].append(dimension_entry)
                if data_type_simple in ["NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"]:
                    measure_entry = {
                        "name": column,
                        "expr": column,
                        "description": "<string>",
                        "data_type": data_type_simple,
                        "default_aggregation": "<aggregate function>",
                        "synonyms": ["<array of strings>"]
                    }
                    table_entry["measures"].append(measure_entry)

            st.session_state['yaml_structure']['tables'].append(table_entry)
            yaml_str = yaml.dump(
                st.session_state['yaml_structure'], sort_keys=False, indent=2)
            st.session_state['yaml_str'] = yaml_str  # Save to session state

    # Display the updated YAML structure
    st.code(st.session_state.get('yaml_str', yaml.dump(
        yaml_template, sort_keys=False, indent=2)), language='yaml')

    # Create a download button for the YAML file
    yaml_bytes = io.BytesIO(st.session_state.get(
        'yaml_str', '').encode('utf-8'))
    st.download_button(
        label="Download YAML file",
        data=yaml_bytes,
        file_name="semantic_model.yaml",
        mime="text/plain"
    )

# Function to reset


# Function to reset the app
def reset_app():
    st.title("Reset Application")
    st.warning(
        "Are you sure you want to reset the application? This will clear all saved data.")

    if st.button("Reset"):
        for key in st.session_state.keys():
            del st.session_state[key]
        st.experimental_set_query_params(page="Welcome")
        st.success("Application has been reset. Please go to the Welcome page.")

# Main function to control the app


def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Go to", ["Welcome", "Get Started", "Table Definition", "Reset"])

    if page == "Welcome":
        show_welcome_page()
    elif page == "Get Started":
        show_get_started_page()
    elif page == "Table Definition":
        show_table_definition_page()
    elif page == "Reset":
        reset_app()


# Auto-navigation based on session state
    if 'page' in st.session_state and st.session_state['page'] == "Table Definition":
        show_table_definition_page()


if __name__ == "__main__":
    main()
