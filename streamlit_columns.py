import streamlit as st
import json
import os
from typing import Dict, Any, List

# The name of the JSON file containing the lineage data
LINEAGE_FILE = "C:\\Users\\ADMIN\\Desktop\\AI\\hack\\modify_data.json"

# NOTE: generate_dot_lineage remains the same as in the previous, corrected version.
def generate_dot_lineage(data: Dict[str, Any], filter_column: str = None) -> str:
    """
    Converts the structured lineage data into a DOT language string for Graphviz,
    with an option to filter for a single target column.
    """
    dot = ['digraph G {']
    dot.append('  rankdir=LR;') # Left to Right flow
    dot.append('  splines=polyline;')
    dot.append('  node [shape=box, style="filled,rounded", color="#5E8C6A"];')
    dot.append('  edge [fontname="Helvetica", fontsize=10];')
    
    target_table = data['target_table']
    
    all_target_cols = sorted([k for k in data.keys() if k.startswith('col')])
    
    if filter_column and filter_column in all_target_cols:
        columns_to_process = [filter_column]
    else:
        columns_to_process = all_target_cols
    
    
    # --- 1. Collect all nodes (source/target columns) involved in the visualization ---
    
    involved_source_tables = set()
    involved_source_columns = set()
    
    for tgt_col in columns_to_process:
        for definition in data.get(tgt_col, []):
            for src_col_full in definition.get('source_columns', []):
                involved_source_columns.add(src_col_full)
                involved_source_tables.add(".".join(src_col_full.split('.')[:-1]))

    
    # --- 2. Define Clusters for Source Tables ---
    for table_name in involved_source_tables:
        table_alias = f"cluster_{table_name.replace('.', '_')}"
        
        dot.append(f'  subgraph {table_alias} {{')
        dot.append(f'    label = <<b>Source Table: {table_name}</b>>;')
        dot.append(f'    bgcolor="#F0FFF0";')
        
        dot.append(f'    "{table_name}" [shape=box, color="#387C44", fontcolor="#FFFFFF", style="filled,bold"];')
        
        for src_col_full in sorted([c for c in involved_source_columns if c.startswith(table_name)]):
            dot.append(f'    "{src_col_full}" [shape=ellipse, style="filled", fillcolor="#C1E1C1"];')
            
        dot.append('  }')
        dot.append('\n')

    # --- 3. Define Target Cluster ---
    dot.append(f'  subgraph cluster_tgt {{')
    dot.append(f'    label = <<b>Target Table: {target_table}</b>>;')
    dot.append(f'    bgcolor="#FFF0F0";')
    
    dot.append(f'    "{target_table}" [shape=box, color="#CC0000", fontcolor="#FFFFFF", style="filled,bold"];')

    for tgt_col in columns_to_process:
        target_col_full = f"{target_table}.{tgt_col}"
        dot.append(f'    "{target_col_full}" [shape=house, style="filled", fillcolor="#FFC1C1"];')

    dot.append('  }')
    dot.append('\n')
    
    # --- 4. Define Edges ---
    
    for tgt_col in columns_to_process:
        target_col_full = f"{target_table}.{tgt_col}"
        
        for definition in data.get(tgt_col, []):
            expression = ", ".join(definition.get('expressions', []))
            
            label = expression if expression else 'Raw Copy'
            
            for src_col_full in definition.get('source_columns', []):
                sanitized_label = label.replace('"', '\\"')
                
                dot.append(f'  "{src_col_full}" -> "{target_col_full}" [label="{sanitized_label}", arrowhead=normal];')

    dot.append('}')
    return "\n".join(dot)


def app():
    """Main Streamlit application function."""
    st.set_page_config(layout="wide")
    st.title("Filtered Data Lineage Visualization")
    
    # Initialize dot_string to avoid UnboundLocalError
    dot_string = "" 
    
    # Load data
    try:
        if not os.path.exists(LINEAGE_FILE):
             st.error(f"Error: Lineage file '{LINEAGE_FILE}' not found.")
             return
             
        with open(LINEAGE_FILE, 'r') as f:
            lineage_data = json.load(f)

    except Exception as e:
        st.error(f"Error loading JSON data: {e}")
        return

    # --- User Filtering Control ---
    
    all_target_cols = sorted([k for k in lineage_data.keys() if k.startswith('col')])
    filter_options = ["Show All Columns"] + all_target_cols
    
    selected_col = st.sidebar.selectbox(
        "**1. Select Target Column to Filter:**",
        options=filter_options,
        index=0
    )
    
    column_to_visualize = selected_col if selected_col != "Show All Columns" else None
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Target Table:** `{lineage_data.get('target_table')}`")
    st.sidebar.markdown(f"**Source Tables:** {', '.join(lineage_data.get('source_tables', []))}")

    # --- Display Title/Warning Logic ---
    
    if column_to_visualize:
        st.subheader(f"Showing Lineage for: `{lineage_data.get('target_table')}.{column_to_visualize}`")
    else:
        st.subheader(f"Showing Lineage for: `{lineage_data.get('target_table')}` (All Columns)")
        if len(all_target_cols) > 10:
             st.warning(f"Visualization includes {len(all_target_cols)} target columns. Select a single column from the filter on the sidebar to improve readability.")


    # Generate and display Graphviz chart
    try:
        # dot_string is successfully assigned here
        dot_string = generate_dot_lineage(lineage_data, column_to_visualize)
        st.graphviz_chart(dot_string)
        
    except Exception as e:
        st.error(f"Error generating graphviz chart: {e}")
        # dot_string is now guaranteed to exist (either the DOT code or an empty string)
        st.code(dot_string, language='dot')

    st.markdown("---")
    st.subheader("Transformation Expressions for Selected Column")
    
    if column_to_visualize:
        details = lineage_data.get(column_to_visualize, [])
        st.json(details)
    else:
        st.info("Select a single column from the filter on the sidebar to view its transformation details.")

if __name__ == '__main__':
    app()
