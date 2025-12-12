import json
from typing import Any, Dict, List, Set, Tuple, Optional

import sqlglot
from sqlglot import exp

# Example SQL - can be replaced with any Hive SQL
SQL_STATEMENTS = """
"""


def parse_statements(sql: str) -> List[exp.Expression]:
    """Parse SQL statements with Hive dialect"""
    try:
        return sqlglot.parse(sql, read="hive")
    except Exception as e:
        print(f"Warning: Failed to parse SQL: {e}")
        return []


def extract_target_and_select(statement: exp.Expression) -> Tuple[Optional[str], Optional[exp.Select]]:
    """Extract target table and SELECT expression from CREATE/INSERT statements"""
    target_table = None
    select_exp = None
    try:
        if isinstance(statement, exp.Create):
            target_table = statement.this.sql() if getattr(statement, "this", None) else None
            select_exp = getattr(statement, "expression", None)
        elif isinstance(statement, exp.Insert):
            target_table = statement.this.sql() if getattr(statement, "this", None) else None
            select_exp = getattr(statement, "expression", None)
        
        if isinstance(select_exp, exp.Select):
            return target_table, select_exp
    except Exception as e:
        print(f"Warning: Error extracting target/select: {e}")
    
    return target_table, None


def _clean_table_name(tbl_expr: exp.Expression) -> str:
    """Return fully qualified table name without alias (keeps db/schema if present)."""
    try:
        # tbl_expr.sql() retains catalog/schema; strip any trailing alias tokens
        raw = tbl_expr.sql()
    except Exception:
        raw = tbl_expr.sql() if hasattr(tbl_expr, "sql") else ""
    return raw.split()[0]


def extract_source_map(select_exp: exp.Select) -> Tuple[Dict[str, str], Dict[str, List[Tuple[str, str]]]]:
    """
    Build alias -> table name mapping and track subquery column aliases.
    Handles CTEs, subqueries, and regular table references.
    """
    src_map: Dict[str, str] = {}
    subquery_cols_map: Dict[str, List[Tuple[str, str]]] = {}
    
    try:
        # Handle CTEs (WITH clauses)
        with_clause = select_exp.args.get("with")
        if with_clause:
            expressions = with_clause.expressions if hasattr(with_clause, "expressions") else []
            for cte in expressions:
                cte_alias = cte.alias if hasattr(cte, "alias") else None
                if cte_alias:
                    inner_select = cte.this if isinstance(cte.this, exp.Select) else None
                    if inner_select:
                        inner_map, inner_sq = extract_source_map(inner_select)
                        src_map.update(inner_map)
                        subquery_cols_map.update(inner_sq)
        
        from_clause = select_exp.args.get("from_")
        if from_clause and getattr(from_clause, "this", None):
            tbl = from_clause.this
            if isinstance(tbl, exp.Subquery):
                # Dynamically get subquery alias
                sq_alias = getattr(tbl, "alias_or_name", None) or getattr(tbl, "alias", None)
                if not sq_alias:
                    sq_alias = f"subquery_{id(tbl)}"
                
                inner_select = tbl.this if isinstance(tbl.this, exp.Select) else None
                if inner_select:
                    col_pairs: List[Tuple[str, str]] = []
                    for proj in inner_select.expressions:
                        if isinstance(proj, exp.Star):
                            continue
                        src_col = proj.this.name if isinstance(proj.this, exp.Column) else proj.this.sql()
                        tgt_col = proj.alias_or_name or src_col
                        col_pairs.append((src_col, tgt_col))
                    if col_pairs:
                        subquery_cols_map[sq_alias] = col_pairs
                    inner_map, inner_sq = extract_source_map(inner_select)
                    src_map.update(inner_map)
                    subquery_cols_map.update(inner_sq)
            else:
                table_name_clean = _clean_table_name(tbl)
                alias = getattr(tbl, "alias_or_name", None) or getattr(tbl, "alias", None) or table_name_clean
                src_map[alias] = table_name_clean
        
        # Handle JOINs
        for join in select_exp.args.get("joins") or []:
            if getattr(join, "this", None):
                jt = join.this
                if isinstance(jt, exp.Subquery):
                    # Dynamically get subquery alias
                    sq_alias = getattr(jt, "alias_or_name", None) or getattr(jt, "alias", None)
                    if not sq_alias:
                        sq_alias = f"subquery_{id(jt)}"
                    
                    inner_select = jt.this if isinstance(jt.this, exp.Select) else None
                    if inner_select:
                        col_pairs: List[Tuple[str, str]] = []
                        for proj in inner_select.expressions:
                            if isinstance(proj, exp.Star):
                                continue
                            src_col = proj.this.name if isinstance(proj.this, exp.Column) else proj.this.sql()
                            tgt_col = proj.alias_or_name or src_col
                            col_pairs.append((src_col, tgt_col))
                        if col_pairs:
                            subquery_cols_map[sq_alias] = col_pairs
                        inner_map, inner_sq = extract_source_map(inner_select)
                        src_map.update(inner_map)
                        subquery_cols_map.update(inner_sq)
                else:
                    table_name_clean = _clean_table_name(jt)
                    jalias = getattr(jt, "alias_or_name", None) or getattr(jt, "alias", None) or table_name_clean
                    src_map[jalias] = table_name_clean
    except Exception as e:
        print(f"Warning: Error in extract_source_map: {e}")
    
    return src_map, subquery_cols_map


def get_column_parts(col_exp: exp.Expression) -> Tuple[Optional[str], Optional[str]]:
    """Extract table alias and column name from column expression"""
    try:
        if isinstance(col_exp, exp.Column):
            # Prefer direct properties; fall back to args
            table_alias = getattr(col_exp, "table", None)
            if not table_alias and col_exp.args.get("table") is not None:
                try:
                    table_alias = col_exp.args["table"].name
                except Exception:
                    table_alias = str(col_exp.args.get("table"))

            name = None
            try:
                name = getattr(col_exp.this, "name", None)
            except Exception:
                name = getattr(col_exp, "name", None)
            if not name:
                try:
                    name = col_exp.name
                except Exception:
                    name = None
            # Fallback: if name is still None, use the rendered SQL (e.g., for dotted or quoted columns)
            if name is None:
                try:
                    raw_sql = col_exp.sql()
                except Exception:
                    raw_sql = None
                if raw_sql:
                    if "." in raw_sql:
                        # e.g., raw.active -> alias raw, column active
                        name = raw_sql.split(".")[-1]
                        if table_alias is None:
                            table_alias = raw_sql.rsplit(".", 1)[0]
                    else:
                        name = raw_sql
            return table_alias, name
    except Exception:
        pass
    return None, None


def parse_simple_create_table_as_select(sql: str) -> Optional[Dict[str, Any]]:
    """
    Fallback parser for simple CREATE TABLE AS SELECT statements using regex.
    Returns source and target column mappings when sqlglot fails to extract columns properly.
    """
    import re
    
    sql = sql.strip()
    
    # Extract target table name
    target_match = re.search(r'create\s+table\s+(\w+)\s+as', sql, re.IGNORECASE)
    if not target_match:
        return None
    
    target_table = target_match.group(1)
    
    # Extract SELECT clause (columns)
    select_match = re.search(r'select\s+(.*?)\s+from', sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return None
    
    columns_str = select_match.group(1)
    # Simple split by comma (not perfect for complex expressions, but good for simple cases)
    columns = [col.strip() for col in columns_str.split(',')]
    
    # Extract source table
    from_match = re.search(r'from\s+([\w.]+)(?:\s+(\w+))?', sql, re.IGNORECASE)
    if not from_match:
        return None
    
    source_table = from_match.group(1)
    alias = from_match.group(2) if from_match.group(2) else None
    
    # Parse source and target columns
    source_columns = []
    target_columns = []
    
    for col in columns:
        # Handle alias.column or column
        if '.' in col:
            parts = col.split('.')
            source_col = parts[-1]  # Get column name after alias
        else:
            source_col = col
        
        # Check if there's a column alias (AS)
        if ' as ' in col.lower():
            col_parts = re.split(r'\s+as\s+', col, flags=re.IGNORECASE)
            source_col = col_parts[0].split('.')[-1] if '.' in col_parts[0] else col_parts[0]
            target_col = col_parts[1]
        else:
            target_col = source_col
        
        source_columns.append({
            'table': source_table,
            'column': source_col.strip()
        })
        
        target_columns.append({
            'table': target_table,
            'column': target_col.strip()
        })
    
    return {
        'source_table': source_table,
        'source_alias': alias,
        'target_table': target_table,
        'source_columns': source_columns,
        'target_columns': target_columns
    }


def is_valid_column_name(name: str) -> bool:
    """Check if a string is a valid column name (not a literal, keyword, or malformed)"""
    if not name or not isinstance(name, str):
        return False
    
    name = name.strip()
    
    # Filter out empty or very short names
    if len(name) == 0:
        return False
    
    # Filter out string literals (including escaped quotes)
    if name.startswith("'") or name.startswith('"') or "'" in name or '"' in name:
        return False
    
    # Filter out numbers
    if name.isdigit():
        return False
    
    # Filter out names with parentheses (function calls or incomplete expressions)
    if '(' in name or ')' in name:
        return False
    
    # Filter out names with SQL operators or special characters that indicate expressions
    invalid_chars = ['%', ',', ';', '<', '>', '=', '!', '?', '|', '&', '^', '~', '`']
    if any(char in name for char in invalid_chars):
        return False
    
    # Filter out multi-line strings or strings with newlines
    if '\n' in name or '\r' in name or '\t' in name:
        return False
    
    # Filter out SQL keywords and common functions
    sql_keywords = {
        'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'in', 'like',
        'is', 'null', 'select', 'from', 'where', 'group', 'by', 'order', 'having',
        'as', 'on', 'join', 'left', 'right', 'inner', 'outer', 'cross', 'union',
        'concat', 'replace', 'lower', 'upper', 'trim', 'cast', 'substring', 'coalesce',
        'validation', 'plan', 'mitigate', 'monitor', 'sustainability'
    }
    if name.lower() in sql_keywords:
        return False
    
    # Filter out malformed names with only special characters
    if all(c in "()[]{},.;:!@#$%^&*-+=<>?/\\|`~' \t\n\r" for c in name):
        return False
    
    # Must start with letter or underscore (valid identifier pattern)
    if not (name[0].isalpha() or name[0] == '_'):
        return False
    
    return True


def extract_columns_from_expression(expr_sql: str) -> List[Tuple[Optional[str], Optional[str]]]:
    """Parse expression and extract column references (alias, column_name)"""
    cols: List[Tuple[Optional[str], Optional[str]]] = []
    try:
        node = sqlglot.parse_one(expr_sql, read="hive")
        for c in node.find_all(exp.Column):
            ta, cn = get_column_parts(c)
            # Only add valid column names
            if cn and is_valid_column_name(cn):
                cols.append((ta, cn))
    except Exception:
        pass
    return cols


def build_table_defs(expressions: List[exp.Expression]) -> Dict[str, Dict[str, Any]]:
    """Build lineage definitions for all CREATE/INSERT statements"""
    table_defs: Dict[str, Dict[str, Any]] = {}

    def resolve_base_sources_local(tbl: str, seen: Optional[Set[str]] = None) -> Set[str]:
        """Recursively resolve intermediate tables to base physical tables"""
        if seen is None:
            seen = set()
        if tbl in seen:
            return set()
        seen.add(tbl)
        if tbl not in table_defs:
            return {tbl}
        declared = table_defs[tbl].get("source_tables", [])
        if not declared:
            return {tbl}
        bases: Set[str] = set()
        for s in declared:
            if s == tbl:
                bases.add(s)
            else:
                bases.update(resolve_base_sources_local(s, seen))
        return bases

    for stmt in expressions:
        try:
            if not isinstance(stmt, (exp.Create, exp.Insert)):
                continue
            
            target_table, select_exp = extract_target_and_select(stmt)
            if not target_table or not select_exp:
                continue

            source_map, subquery_cols_map = extract_source_map(select_exp)
            immediate_sources = list(source_map.values())

            column_lineage: Dict[str, List[Dict[str, Any]]] = {}
            
            for i, proj in enumerate(select_exp.expressions):
                if isinstance(proj, exp.Star):
                    column_lineage["*"] = [{
                        "source_expression": "*",
                        "source_tables": immediate_sources,
                        "origin": target_table,
                        "subquery_cols": dict(subquery_cols_map)
                    }]
                    break
                
                tgt_col = proj.alias_or_name or f"col_{i+1}"
                
                # Skip invalid column names
                if not is_valid_column_name(tgt_col):
                    continue
                
                if getattr(proj, "this", None) is None:
                    continue
                
                src_sql = proj.this.sql(pretty=True).strip()
                
                # Handle both Column and Identifier (Identifier is used when column has implicit alias)
                if isinstance(proj.this, (exp.Column, exp.Identifier)):
                    table_alias, col_name = get_column_parts(proj.this)

                    # If col_name came back dotted, split into alias and column
                    if col_name and "." in col_name and table_alias is None:
                        table_alias, col_name = col_name.rsplit(".", 1)

                    if table_alias:
                        # Primary: alias lookup in source_map
                        source_table_name = source_map.get(table_alias)
                        # Fallback: if alias not found, assume the alias is already the table name
                        if not source_table_name:
                            source_table_name = table_alias
                    else:
                        # No alias on column; if only one source table is present, use it
                        source_table_name = immediate_sources[0] if len(immediate_sources) == 1 else None
                    # If still missing, try any single source in source_map
                    if not source_table_name and len(source_map.values()) == 1:
                        source_table_name = list(source_map.values())[0]
                    # If still missing, and source_map not empty, take the first mapped table
                    if not source_table_name and source_map:
                        source_table_name = next(iter(source_map.values()))
                    # Last resort: if still missing, use any immediate source (even if multiple)
                    if not source_table_name and immediate_sources:
                        source_table_name = immediate_sources[0]

                    # If column name is still missing, fall back to rendered SQL of the projection
                    if not col_name:
                        try:
                            col_name = proj.this.sql()
                        except Exception:
                            col_name = None
                        if col_name and "." in col_name:
                            # normalize dotted fallback
                            col_name = col_name.split(".")[-1]
                    # If both table and column are still missing, skip entry
                    if not source_table_name and not col_name:
                        continue
                    column_lineage[tgt_col] = [{
                        "source_table": source_table_name,
                        "source_column": col_name,
                        "origin": target_table,
                        "subquery_cols": dict(subquery_cols_map)
                    }]
                else:
                    column_lineage[tgt_col] = [{
                        "source_expression": src_sql,
                        "origin_map": dict(source_map),
                        "origin": target_table,
                        "subquery_cols": dict(subquery_cols_map)
                    }]

            base_sources: Set[str] = set()
            for s in immediate_sources:
                if s in table_defs:
                    base_sources.update(resolve_base_sources_local(s))
                else:
                    base_sources.add(s)

            table_defs[target_table] = {
                "column_lineage": column_lineage,
                "immediate_source_tables": immediate_sources,
                "source_tables": list(base_sources),
                "source_map": source_map,
                "subquery_cols_map": subquery_cols_map,
            }
        except Exception as e:
            print(f"Warning: Error processing statement: {e}")
            continue

    return table_defs


def resolve_to_base_columns(table_defs: Dict[str, Dict[str, Any]],
                            table_name: str,
                            col_name: str,
                            seen: Optional[Set[Tuple[str, str]]] = None) -> List[Tuple[str, str]]:
    """Resolve column through intermediate tables to base physical columns"""
    if seen is None:
        seen = set()
    key = (table_name, col_name)
    if key in seen:
        return []
    seen.add(key)

    if table_name not in table_defs:
        return [(table_name, col_name)]

    try:
        tdef = table_defs[table_name]
        col_lineage = tdef.get("column_lineage", {})
        entries = col_lineage.get(col_name, [])

        result: List[Tuple[str, str]] = []

        for ent in entries:
            if ent.get("source_column") and ent.get("source_table"):
                st = ent["source_table"]
                sc = ent["source_column"]
                if st in table_defs:
                    result.extend(resolve_to_base_columns(table_defs, st, sc, seen))
                else:
                    result.append((st, sc))
            elif ent.get("source_expression"):
                expr = ent["source_expression"]
                origin_map = ent.get("origin_map") or tdef.get("source_map", {})
                refs = extract_columns_from_expression(expr)
                for alias, cname in refs:
                    if not cname:
                        continue
                    if alias:
                        mapped = origin_map.get(alias) or alias
                    else:
                        vals = list(origin_map.values())
                        mapped = vals[0] if len(vals) == 1 else None
                        # Fallback: if no origin_map, and only one immediate_source, use it
                        if not mapped and tdef.get("immediate_source_tables"):
                            im = tdef.get("immediate_source_tables")
                            mapped = im[0] if len(im) == 1 else None
                    if mapped:
                        if mapped in table_defs:
                            result.extend(resolve_to_base_columns(table_defs, mapped, cname, seen))
                        else:
                            result.append((mapped, cname))

        if not result:
            for candidate, cand_def in table_defs.items():
                if candidate == table_name:
                    continue
                cand_cols = cand_def.get("column_lineage", {})
                if col_name in cand_cols:
                    cand_bases = resolve_to_base_columns(table_defs, candidate, col_name, seen)
                    if cand_bases:
                        result.extend(cand_bases)
                        break

        seen_pairs = set()
        dedup: List[Tuple[str, str]] = []
        for r in result:
            if r not in seen_pairs:
                seen_pairs.add(r)
                dedup.append(r)
        return dedup
    except Exception as e:
        print(f"Warning: Error in resolve_to_base_columns: {e}")
        return [(table_name, col_name)]


def expand_select_star_for_target(table_defs: Dict[str, Dict[str, Any]], target: str, col_rename_map: Optional[Dict[str, str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Expand SELECT * statements to individual columns"""
    if col_rename_map is None:
        col_rename_map = {}
    
    try:
        final = table_defs.get(target, {})
        tgt_col_lineage = final.get("column_lineage", {}) or {}
        immediate = final.get("immediate_source_tables", []) or []
        base_sources = final.get("source_tables", []) or []
        subquery_cols_map = final.get("subquery_cols_map", {}) or {}

        for col, entries in tgt_col_lineage.items():
            if col != "*":
                for ent in entries:
                    if ent.get("source_column"):
                        src_col = ent["source_column"]
                        col_rename_map[src_col] = col
                    subq_cols = ent.get("subquery_cols", {})
                    for sq_alias, col_pairs in subq_cols.items():
                        for src, tgt in col_pairs:
                            if tgt == col:
                                col_rename_map[src] = col

        if "*" in tgt_col_lineage or not tgt_col_lineage:
            source_candidates = immediate or base_sources
            if not source_candidates:
                return {}
            
            expanded: Dict[str, List[Dict[str, Any]]] = {}
            
            for source_table in source_candidates:
                if source_table not in table_defs:
                    continue
                
                src_cols = table_defs[source_table].get("column_lineage", {})
                src_subquery_map = table_defs[source_table].get("subquery_cols_map", {})
                
                for col, entries in src_cols.items():
                    if col == "*":
                        local_rename: Dict[str, str] = {}
                        for sq_alias, col_pairs in src_subquery_map.items():
                            for src_col, tgt_col in col_pairs:
                                local_rename[src_col] = tgt_col
                        
                        nested_expanded = expand_select_star_for_target(table_defs, source_table, local_rename)
                        for nested_col, nested_entries in nested_expanded.items():
                            final_col = col_rename_map.get(nested_col, nested_col)
                            if final_col not in expanded:
                                expanded[final_col] = nested_entries
                        continue
                    
                    final_col = col_rename_map.get(col, col)
                    final_entries: List[Dict[str, Any]] = []
                    
                    for ent in entries:
                        if ent.get("source_column") and ent.get("source_table"):
                            st = ent["source_table"]
                            sc = ent["source_column"]
                            if st in table_defs:
                                bases = resolve_to_base_columns(table_defs, st, sc)
                                for bt, bc in bases:
                                    final_entries.append({"source_table": bt, "source_column": bc, "origin": source_table})
                            else:
                                final_entries.append({"source_table": st, "source_column": sc, "origin": source_table})
                        else:
                            ent_copy = dict(ent)
                            ent_copy.setdefault("origin_map", table_defs.get(source_table, {}).get("source_map", {}))
                            ent_copy.setdefault("origin", source_table)
                            final_entries.append(ent_copy)
                    
                    expanded[final_col] = final_entries
            
            return expanded
        
        # Filter out invalid column names before returning
        filtered_lineage = {k: v for k, v in tgt_col_lineage.items() if is_valid_column_name(k)}
        return filtered_lineage
    except Exception as e:
        print(f"Warning: Error in expand_select_star_for_target: {e}")
        return {}


def detect_schema(table_defs: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """Guess a schema by looking at the first table name that contains a dot."""
    candidates: List[str] = []
    for tbl, tdef in table_defs.items():
        candidates.append(tbl)
        candidates.extend(tdef.get("source_tables", []))
        candidates.extend(tdef.get("immediate_source_tables", []))
    for name in candidates:
        if name and "." in name:
            return name.split(".", 1)[0]
    return None


def build_final_lineage(table_defs: Dict[str, Dict[str, Any]], target: str = None, default_schema: Optional[str] = None) -> Dict[str, Any]:
    """Build final column lineage output"""
    if not table_defs:
        return {"error": "No table definitions found"}
    
    # Auto-detect target if not provided
    actual_target = target
    if not actual_target:
        actual_target = list(table_defs.keys())[-1]
    elif actual_target not in table_defs:
        for key in table_defs.keys():
            if key.endswith(f".{actual_target}") or key == actual_target:
                actual_target = key
                break
        if actual_target not in table_defs:
            actual_target = list(table_defs.keys())[-1]

    try:
        tgt_def = table_defs[actual_target]
        final_map = expand_select_star_for_target(table_defs, actual_target) or tgt_def.get("column_lineage", {})

        schema_prefix = default_schema or detect_schema(table_defs) or "default"

        def add_schema_prefix(table_name: str) -> str:
            """Add schema prefix if not present"""
            if not table_name or "." in table_name:
                return table_name
            return f"{schema_prefix}.{table_name}"

        output_cols: Dict[str, List[Dict[str, Any]]] = {}
        
        for tgt_col, src_list in final_map.items():
            # Skip invalid target column names
            if not is_valid_column_name(tgt_col):
                continue
            
            source_cols_set: Set[str] = set()
            expr_set: Set[str] = set()
            is_transformation = False

            for src in src_list:
                if src.get("source_expression"):
                    expr_sql = src["source_expression"]
                    is_bare_column = (expr_sql == "*" or 
                                    (expr_sql and expr_sql.replace(".", "").replace("_", "").isalnum() and 
                                     not any(c in expr_sql for c in ["(", ")", ",", " "])))
                    
                    if not is_bare_column:
                        is_transformation = True
                        expr_set.add(expr_sql)
                    
                    origin_map = src.get("origin_map") or tgt_def.get("source_map", {})
                    for alias, cname in extract_columns_from_expression(expr_sql):
                        if not cname:
                            continue
                        if alias:
                            mapped = origin_map.get(alias) or alias
                        else:
                            vals = list(origin_map.values())
                            mapped = vals[0] if len(vals) == 1 else None
                        if mapped:
                            bases = resolve_to_base_columns(table_defs, mapped, cname)
                            if not bases:
                                for candidate, td in table_defs.items():
                                    if cname in td.get("column_lineage", {}):
                                        bases = resolve_to_base_columns(table_defs, candidate, cname)
                                        if bases:
                                            break
                            for bt, bc in bases:
                                if bt and bc and str(bt).upper() != "N/A" and is_valid_column_name(bc):
                                    bt_qualified = add_schema_prefix(bt)
                                    source_cols_set.add(f"{bt_qualified}.{bc}")
                    continue

                st = src.get("source_table")
                sc = src.get("source_column")
                if st and sc:
                    if st in table_defs:
                        bases = resolve_to_base_columns(table_defs, st, sc)
                        if not bases:
                            for candidate, td in table_defs.items():
                                if sc in td.get("column_lineage", {}):
                                    bases = resolve_to_base_columns(table_defs, candidate, sc)
                                    if bases:
                                        break
                        for bt, bc in bases:
                            if bt and bc and str(bt).upper() != "N/A" and is_valid_column_name(bc):
                                bt_qualified = add_schema_prefix(bt)
                                source_cols_set.add(f"{bt_qualified}.{bc}")
                    else:
                        if str(st).upper() != "N/A" and is_valid_column_name(sc):
                            st_qualified = add_schema_prefix(st)
                            source_cols_set.add(f"{st_qualified}.{sc}")

            output_entry = {
                "target_column": tgt_col,
                "source_columns": sorted(source_cols_set),
                "expressions": sorted(expr_set) if is_transformation else [],
            }
            output_cols[tgt_col] = [output_entry]

        source_tables_qualified = [add_schema_prefix(t) for t in tgt_def.get("source_tables", [])]

        cleaned = {
            "target_table": actual_target,
            "source_tables": sorted(source_tables_qualified),
        }
        cleaned.update(output_cols)
        return cleaned
    except Exception as e:
        print(f"Error in build_final_lineage: {e}")
        return {"error": str(e)}


def generate_lineage(sql: str, target_table: str = None, default_schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Main function to generate column lineage from Hive SQL.
    
    Args:
        sql: Hive SQL statements (can be any valid Hive SQL)
        target_table: Name of the target table (if None, auto-detects last created table)
        default_schema: Default schema name to prepend to tables without schema
    
    Returns:
        Dictionary with column lineage information
    """
    expressions = parse_statements(sql)
    if not expressions:
        return {"error": "Failed to parse SQL statements"}
    
    table_defs = build_table_defs(expressions)
    if not table_defs:
        return {"error": "No table definitions found"}
    
    result = build_final_lineage(table_defs, target_table, default_schema)
    
    # Only use fallback parser for truly simple SQL (no CASE, no functions in SELECT)
    # Check if it's a simple CREATE TABLE AS SELECT
    is_simple_sql = ("case" not in sql.lower() and 
                     "concat" not in sql.lower() and
                     "replace" not in sql.lower() and
                     sql.count("create table") == 1)
    
    if is_simple_sql:
        # Check if source_columns are empty
        has_empty_sources = False
        for key, value in result.items():
            if key not in ["target_table", "source_tables", "error"]:
                if isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict) and "source_columns" in value[0]:
                        if not value[0]["source_columns"] or len(value[0]["source_columns"]) == 0:
                            has_empty_sources = True
                            break
        
        if has_empty_sources:
            print("Warning: Source columns empty, trying fallback parser...")
            fallback_result = parse_simple_create_table_as_select(sql)
            if fallback_result:
                # Build result from fallback parser
                enhanced_result = {
                    "target_table": fallback_result["target_table"],
                    "source_tables": [fallback_result["source_table"]]
                }
                
                # Map each target column to its source
                for src_col, tgt_col in zip(fallback_result["source_columns"], fallback_result["target_columns"]):
                    col_name = tgt_col["column"]
                    enhanced_result[col_name] = [{
                        "target_column": col_name,
                        "source_columns": [f"{src_col['table']}.{src_col['column']}"],
                        "expressions": []
                    }]
                
                return enhanced_result
    
    return result


def main():
    result = generate_lineage(SQL_STATEMENTS, default_schema=None)
    
    with open("C:\\Users\\ADMIN\\Desktop\\AI\\hack\\modify_data.json", "w") as f:
        json.dump(result, f, indent=4)
    
    print("Lineage data written to modify_data.json")


if __name__ == "__main__":
    main()
