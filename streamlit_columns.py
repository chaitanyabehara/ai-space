import streamlit as st
import json
import os
from typing import Dict, Any, List

# The name of the JSON file containing the lineage data
LINEAGE_FILE = "C:\\Users\\ADMIN\\Desktop\\AI\\hack\\modify_data.json"

# --- Graph builders ---
def generate_table_lineage(data: Dict[str, Any]) -> str:
    """Table-level lineage: source tables -> target table."""
    dot = ['digraph G {']
    dot.append('  rankdir=LR;')
    dot.append('  node [shape=box, style="filled,rounded", color="#5E8C6A"];')
    dot.append('  edge [fontname="Helvetica", fontsize=10];')

    target_table = data.get('target_table', 'target')
    src_tables = data.get('source_tables', [])

    dot.append(f'  "{target_table}" [shape=box, color="#CC0000", fontcolor="#FFFFFF", style="filled,bold"];')
    for src in src_tables:
        dot.append(f'  "{src}" [shape=box, color="#387C44", fontcolor="#FFFFFF", style="filled,bold"];')
        dot.append(f'  "{src}" -> "{target_table}" [label="table", arrowhead=normal];')

    dot.append('}')
    return "\n".join(dot)


# Column-level graph
def generate_dot_lineage(data: Dict[str, Any], filter_column: str = None, use_intermediate: bool = False) -> str:
    """
    Converts the structured lineage data into a DOT language string for Graphviz,
    with an option to filter for a single target column.
    Supports both flat lineage and multi-hop lineage with intermediate tables.
    """
    dot = ['digraph G {']
    dot.append('  rankdir=LR;') # Left to Right flow
    dot.append('  splines=polyline;')
    dot.append('  node [shape=box, style="filled,rounded", color="#5E8C6A"];')
    dot.append('  edge [fontname="Helvetica", fontsize=10];')
    
    # Check if this is intermediate table format (with nodes and edges)
    if 'nodes' in data and 'edges' in data:
        return generate_dot_lineage_with_intermediates(data, filter_column)
    
    target_table = data['target_table']
    
    reserved_keys = {'target_table', 'source_tables'}
    all_target_cols = sorted([k for k in data.keys() if k not in reserved_keys])
    
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
    
    # --- 4. Define Edges with Intermediate Transformation Nodes ---
    # When multiple source columns share the same expression, create an intermediate node
    # to consolidate them into a single line to the target
    
    transformation_node_counter = 0
    
    for tgt_col in columns_to_process:
        target_col_full = f"{target_table}.{tgt_col}"
        
        for definition in data.get(tgt_col, []):
            expression = ", ".join(definition.get('expressions', []))
            label = expression if expression else 'Raw Copy'
            sanitized_label = label.replace('"', '\\"')
            
            source_columns = definition.get('source_columns', [])
            
            # If multiple source columns use the same expression, create intermediate node
            if len(source_columns) > 1:
                # Create a unique transformation node
                transformation_node_id = f"transform_{tgt_col}_{transformation_node_counter}"
                transformation_node_counter += 1
                
                # Add the transformation node (invisible, just for flow)
                dot.append(f'  "{transformation_node_id}" [shape=point, width=0.1, height=0.1, style=invis];')
                
                # All source columns point to this intermediate node
                for src_col_full in source_columns:
                    dot.append(f'  "{src_col_full}" -> "{transformation_node_id}" [arrowhead=none, style=solid];')
                
                # Intermediate node points to target with the expression label
                dot.append(f'  "{transformation_node_id}" -> "{target_col_full}" [label="{sanitized_label}", arrowhead=normal];')
            else:
                # Single source column: direct connection
                for src_col_full in source_columns:
                    dot.append(f'  "{src_col_full}" -> "{target_col_full}" [label="{sanitized_label}", arrowhead=normal];')

    dot.append('}')
    return "\n".join(dot)


def generate_dot_lineage_with_intermediates(data: Dict[str, Any], filter_column: str = None) -> str:
    """
    Generates lineage visualization for data with intermediate tables.
    Shows the complete path: source tables → intermediate tables → target table
    """
    dot = ['digraph G {']
    dot.append('  rankdir=LR;')
    dot.append('  splines=polyline;')
    dot.append('  node [shape=box, style="filled,rounded", color="#5E8C6A"];')
    dot.append('  edge [fontname="Helvetica", fontsize=10];')
    
    nodes = data.get('nodes', [])
    edges = data.get('edges', [])
    
    # Categorize nodes by type and parent
    tables = {}
    columns_by_table = {}
    
    for node in nodes:
        if node['type'] == 'table':
            tables[node['id']] = node
            columns_by_table[node['id']] = []
        elif node['type'] == 'column':
            parent_id = node.get('parent_id')
            if parent_id:
                if parent_id not in columns_by_table:
                    columns_by_table[parent_id] = []
                columns_by_table[parent_id].append(node)
    
    # If filter_column specified, find target table and trace lineage
    if filter_column:
        target_col_nodes = [n for n in nodes if n['type'] == 'column' and n['label'] == filter_column]
        if not target_col_nodes:
            # Fallback to showing all if column not found
            pass
        else:
            # Trace backwards from target column to find all related nodes and edges
            related_nodes = set()
            related_edges = []
            
            for col_node in target_col_nodes:
                # BFS to find all source nodes
                queue = [col_node['id']]
                visited = set()
                
                while queue:
                    current_id = queue.pop(0)
                    if current_id in visited:
                        continue
                    visited.add(current_id)
                    related_nodes.add(current_id)
                    
                    # Find incoming edges
                    for edge in edges:
                        if edge['target'] == current_id:
                            related_edges.append(edge)
                            queue.append(edge['source'])
            
            # Filter nodes and edges for visualization
            nodes = [n for n in nodes if n['id'] in related_nodes]
            edges = related_edges
    
    # Create clusters for tables
    for table_id, table_node in tables.items():
        if not filter_column or any(n['parent_id'] == table_id for n in nodes if n['type'] == 'column'):
            cols = [n for n in nodes if n['type'] == 'column' and n['parent_id'] == table_id]
            if cols or table_id in [n['id'] for n in nodes]:
                color = "#CC0000" if table_id.endswith('REPORT') or table_id == 'derived_df' else "#387C44"
                fontcolor = "#FFFFFF" if table_id.endswith('REPORT') or table_id == 'derived_df' else "#FFFFFF"
                bgcolor = "#FFF0F0" if table_id.endswith('REPORT') or table_id == 'derived_df' else "#F0FFF0"
                
                cluster_name = f"cluster_{table_id.replace('.', '_')}"
                dot.append(f'  subgraph {cluster_name} {{')
                dot.append(f'    label = <<b>{table_id}</b>>;')
                dot.append(f'    bgcolor="{bgcolor}";')
                dot.append(f'    "{table_id}" [shape=box, color="{color}", fontcolor="{fontcolor}", style="filled,bold"];')
                
                for col in cols:
                    col_shape = "house" if table_id.endswith('REPORT') or table_id == 'derived_df' else "ellipse"
                    col_color = "#FFC1C1" if table_id.endswith('REPORT') or table_id == 'derived_df' else "#C1E1C1"
                    dot.append(f'    "{col["id"]}" [shape={col_shape}, style="filled", fillcolor="{col_color}"];')
                
                dot.append('  }')
                dot.append('')
    
    # Add edges with transformation logic
    for edge in edges:
        if edge['flow_type'] == 'column_level':
            logic = edge.get('logic', 'Transform')
            sanitized_logic = logic.replace('"', '\\"')[:100]  # Truncate long logic
            dot.append(f'  "{edge["source"]}" -> "{edge["target"]}" [label="{sanitized_logic}", arrowhead=normal];')
        elif edge['flow_type'] == 'table_level':
            dot.append(f'  "{edge["source"]}" -> "{edge["target"]}" [label="table", style=dashed, arrowhead=normal];')
    
    dot.append('}')
    return "\n".join(dot)


def app():
    """Main Streamlit application function."""
    st.set_page_config(layout="wide")
    st.title("Filtered Data Lineage Visualization")
    
    # Initialize dot_string to avoid UnboundLocalError
    dot_string = "" 
    
    # File selection
    lineage_files = {
        "Direct Column Lineage": "C:\\Users\\ADMIN\\Desktop\\AI\\hack\\modify_data.json",
        "Multi-Hop Lineage (with Intermediates)": "C:\\Users\\ADMIN\\Desktop\\AI\\hack\\lineage_data.json"
    }
    
    selected_file_name = st.sidebar.radio("**Select Lineage Data:**", list(lineage_files.keys()), index=0)
    LINEAGE_FILE = lineage_files[selected_file_name]
    
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

    # --- View mode ---
    view_mode = st.sidebar.radio("**View mode**", ["Table lineage", "Column lineage"], index=0)

    # --- User Filtering Control (only for column view) ---
    column_to_visualize = None
    
    if view_mode == "Column lineage":
        if 'nodes' in lineage_data:
            # Multi-hop format: extract unique column labels
            column_nodes = [n for n in lineage_data.get('nodes', []) if n['type'] == 'column']
            all_target_cols = sorted(list(set([n['label'] for n in column_nodes])))
        else:
            # Direct format
            reserved_keys = {'target_table', 'source_tables'}
            all_target_cols = sorted([k for k in lineage_data.keys() if k not in reserved_keys])
        
        filter_options = ["Show All Columns"] + all_target_cols
        selected_col = st.sidebar.selectbox(
            "**1. Select Target Column to Filter:**",
            options=filter_options,
            index=0
        )
        column_to_visualize = selected_col if selected_col != "Show All Columns" else None

    st.sidebar.markdown("---")
    
    if 'nodes' in lineage_data:
        # Multi-hop format
        table_nodes = [n for n in lineage_data.get('nodes', []) if n['type'] == 'table']
        target_tables = [n['label'] for n in table_nodes if n['label'].endswith('REPORT') or 'derived' in n['label'].lower()]
        source_tables = [n['label'] for n in table_nodes if not (n['label'].endswith('REPORT') or 'derived' in n['label'].lower())]
        st.sidebar.markdown(f"**Target Table(s):** {', '.join(target_tables)}")
        st.sidebar.markdown(f"**Source Tables:** {', '.join(source_tables)}")
    else:
        # Direct format
        st.sidebar.markdown(f"**Target Table:** `{lineage_data.get('target_table')}`")
        st.sidebar.markdown(f"**Source Tables:** {', '.join(lineage_data.get('source_tables', []))}")

    # --- Display Title/Warning Logic ---
    
    if view_mode == "Table lineage":
        if 'nodes' in lineage_data:
            st.subheader("Table-Level Lineage (Multi-Hop)")
        else:
            st.subheader(f"Table Lineage: {', '.join(lineage_data.get('source_tables', []))}  →  {lineage_data.get('target_table')}")
    else:
        if column_to_visualize:
            st.subheader(f"Showing Lineage for: `{column_to_visualize}`")
        else:
            if 'nodes' in lineage_data:
                st.subheader("Showing All Column Lineage (Multi-Hop with Intermediates)")
            else:
                st.subheader(f"Showing Lineage for: `{lineage_data.get('target_table')}` (All Columns)")
                if len(all_target_cols) > 10:
                     st.warning(f"Visualization includes {len(all_target_cols)} target columns. Select a single column from the filter on the sidebar to improve readability.")


    # Generate and display Graphviz chart
    try:
        if view_mode == "Table lineage":
            if 'nodes' in lineage_data:
                # Multi-hop format
                dot_string = generate_dot_lineage_with_intermediates(lineage_data)
            else:
                # Direct format
                dot_string = generate_table_lineage(lineage_data)
            st.graphviz_chart(dot_string)
        else:
            if 'nodes' in lineage_data:
                # Multi-hop format with column filtering
                dot_string = generate_dot_lineage_with_intermediates(lineage_data, column_to_visualize)
            else:
                # Direct format
                dot_string = generate_dot_lineage(lineage_data, column_to_visualize)
            st.graphviz_chart(dot_string)
    except Exception as e:
        st.error(f"Error generating graphviz chart: {e}")
        st.code(dot_string, language='dot')

    if view_mode == "Column lineage":
        st.markdown("---")
        st.subheader("Transformation Expressions for Selected Column")
        if column_to_visualize:
            if 'nodes' in lineage_data:
                # Multi-hop format: find edges for this column
                edges = lineage_data.get('edges', [])
                col_edges = [e for e in edges if column_to_visualize in e.get('source', '') or column_to_visualize in e.get('target', '')]
                if col_edges:
                    st.json(col_edges)
                else:
                    st.info("No transformation details found for this column.")
            else:
                # Direct format
                details = lineage_data.get(column_to_visualize, [])
                # Remove duplicates while preserving order
                seen = set()
                unique_details = []
                for detail in details:
                    detail_json = json.dumps(detail, sort_keys=True)
                    if detail_json not in seen:
                        seen.add(detail_json)
                        unique_details.append(detail)
                st.json(unique_details)
        else:
            st.info("Select a single column from the filter on the sidebar to view its transformation details.")
    else:
        st.markdown("---")
        st.info("Switch to 'Column lineage' to see column-level expressions.")

if __name__ == '__main__':
    app()
