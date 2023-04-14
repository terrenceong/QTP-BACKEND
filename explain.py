import psycopg2
import difflib
from configparser import ConfigParser
from typing import *


def getDBInfo(fileName = "db.ini"):
    configParser = ConfigParser()
    configParser.read(fileName)
    d = {}
    for val in configParser.items("postgresql"):
        key, value = val
        d[key] = value

    return d

conn = psycopg2.connect(**getDBInfo())
conn.autocommit = True
cur = conn.cursor()


def query_handler(query):
    jsonQuery = "EXPLAIN (FORMAT JSON) " + query
    try:
        cur.execute(jsonQuery)
        plan = cur.fetchall()
        return plan
    except Exception as err:
        print(err)
        return "Invalid Query"


def build_query_plan_tree(node_dict):
    children = []
    for child_node_dict in node_dict.get("Plans", []):
        children.append(build_query_plan_tree(child_node_dict))
    filters = ""
    for key in node_dict:
        if isinstance(node_dict[key], str) and "Filter" in key or "Cond" in key or "Sort" in key or "Key" in key:
            for i in node_dict[key]:
                filters += i
            filters = key + ": " + filters
    return {
        "description": node_dict.get("Node Type"),
        "cost": node_dict.get("Total Cost"),
        "filters": filters,
        "left_child": children[0] if len(children) > 0 else None,
        "right_child": children[1] if len(children) > 1 else None,
        "explanation": explain(node_dict)
    }

def explain(node_dict):
    node_type = node_dict.get("Node Type","")

    if node_type == "Aggregate":
        group_key = node_dict.get("Group Key", [])
        filter_cond = node_dict.get("Filter", "")
        if filter_cond:
            node_desc = f"Aggregate the rows by grouping them based on {', '.join(group_key)}, with filter condition: {filter_cond}"
        else:
            node_desc = f"Aggregate the rows by grouping them based on {', '.join(group_key)}."
            
    elif node_type == "Append":
        node_desc = "Append the results of all subplans into a single result set."
        
    elif node_type == "BitmapAnd":
        node_desc = "Perform a BitmapAnd operation on the given Bitmap Index Scan results."
        
    elif node_type == "Bitmap Heap Scan":
        table_name = node_dict.get("Relation Name", "the table")
        node_desc = f"Perform a Bitmap Heap Scan to efficiently access {table_name} using a bitmap index."
        
    elif node_type == "Bitmap Index Scan":
        index_name = node_dict.get("Index Name", "the index")
        node_desc = f"Scan {index_name} using a bitmap to find the relevant rows."
        
    elif node_type == "BitmapOr":
        node_desc = "Perform a BitmapOr operation on the given Bitmap Index Scan results."
        
    elif node_type == "Custom":
        custom_name = node_dict.get("Custom Name", "a custom operation")
        node_desc = f"Perform {custom_name}, which is not natively supported by PostgreSQL."
        
    elif node_type == "Foreign Scan":
        foreign_table = node_dict.get("Relation Name", "the foreign table")
        node_desc = f"Scan {foreign_table}."
        
    elif node_type == "Function Scan":
        function_name = node_dict.get("Function Name", "the function")
        node_desc = f"Scan {function_name} that returns a set of rows."
        
    elif node_type == "Gather":
        node_desc = "Gather the results of worker nodes into a single result set."
        workers_planned = node_dict.get("Workers Planned", 0)
        if workers_planned > 0:
            node_desc += f"Planned on {workers_planned} worker(s)."
    elif node_type == "Gather Merge":
        node_desc = "Gather the results of worker nodes and merge them into a single result set."
        workers_planned = node_dict.get("Workers Planned", 0)
        if workers_planned > 0:
            node_desc += f"Planned on {workers_planned} worker(s)."
            
    elif node_type == "Hash":
        node_desc = "Build a hash table from the input data for use in a Hash Join."
        
    elif node_type == "Hash Join":
        join_condition = node_dict.get("Join Filter", "")
        if(join_condition):
            node_desc = f"Perform a Hash Join using {join_condition} as the join condition."
        else:
            node_desc = "Perform a Hash Join"
            
    elif node_type == "Index Only Scan":
        index_name = node_dict.get("Index Name", "the index")
        node_desc = f"Perform an Index-Only Scan on {index_name} to find the relevant rows."
        
    elif node_type == "Index Scan":
        index_name = node_dict.get("Index Name", "the index")
        node_desc = f"Perform an Index Scan on {index_name} to find the relevant rows."
        
    elif node_type == "Limit":
        limit_count = node_dict.get("Limit Count", "a certain number of")
        node_desc = f"Limit the result set to {limit_count} rows."
        
    elif node_type == "LockRows":
        lock_mode = node_dict.get("Lock Mode", "a specific lock mode")
        node_desc = f"Lock the rows in the result set using {lock_mode}."
        
    elif node_type == "Materialize":
        node_desc = "Materialize the result set into a temporary storage."
        
    elif node_type == "Merge Join":
        join_condition = node_dict.get("Join Filter", "")
        if join_condition:
            node_desc = f"Perform a Merge Join using {join_condition} as the join condition."
        else:
            node_desc = "Perform a Merge Join"
            
    elif node_type == "Nested Loop":
        join_condition = node_dict.get("Join Filter", "")
        if join_condition:
            node_desc = f"Perform a Nested Loop Join using {join_condition} as the join condition."
        else:
            node_desc = "Perform a Nested Loop Join"
            
    elif node_type == "Project Set":
        function_name = node_dict.get("Function Name", "the function")
        node_desc = f"Perform a ProjectSet operation on the result of {function_name} returning a set of rows."
        
    elif node_type == "Recursive Union":
        node_desc = "Perform a Recursive Union operation to process recursive queries."
        
    elif node_type == "Result":
        filter_condition = node_dict.get("Filter", "")
        if filter_condition:
            node_desc = f"Perform a Result operation using {filter_condition} as the filter condition."
        else:
            node_desc = "Perform a Result operation using a filter condition"
        
    elif node_type == "Seq Scan":
        table_name = node_dict.get("Relation Name", "the table")
        filter_condition = node_dict.get("Filter", "a filter condition")
        node_desc = f"Perform a Sequential Scan on {table_name} using {filter_condition} as the filter condition."
        
    elif node_type == "Sort":
        sort_keys = node_dict.get("Sort Keys", [])
        node_desc = f"Sort the result set by {', '.join(sort_keys)}."
        
    elif node_type == "Subquery Scan":
        subquery_alias = node_dict.get("Alias", "a subquery")
        node_desc = f"Perform a Subquery Scan on {subquery_alias}."
        
    elif node_type == "Tid Scan":
        table_name = node_dict.get("Relation Name", "the table")
        node_desc = f"Perform a TID (tuple ID) Scan on {table_name} to retrieve specific rows."
        
    elif node_type == "Unique":
        unique_keys = node_dict.get("Unique Keys", [])
        if len(unique_keys) != 0:
            node_desc = f"Remove duplicate rows from the result set based on {', '.join(unique_keys)}."
        else:
            node_desc = "Remove duplicate rows from the result set"
        
    elif node_type == "Values Scan":
        values_list = node_dict.get("Values List", [])
        if len(values_list) != 0:
            node_desc = f"Scan a set of constant values: {', '.join(values_list)}."
        else:
            node_desc = "Scan a set of constant values"
        
    elif node_type == "WindowAgg":
        window_function = node_dict.get("Window Function", "a window function")
        node_desc = f"Perform a Window Aggregate operation using {window_function}."
        
    elif node_type == "CTE Scan":
        cte_name = node_dict.get("CTE Name", "")
        if cte_name:
            node_desc = f"Perform a CTE (Common Table Expression) Scan on {cte_name}."
        else:
            node_desc = "Perform a CTE (Common Table Expression Scan"
        
    elif node_type == "Group":
        group_keys = node_dict.get("Group Keys", [])
        if len(group_keys) != 0:
            node_desc = f"Group the result set by {', '.join(group_keys)}."
        else:
            node_desc = "Group the result set"
        
    elif node_type == "Modify Table":
        operation = node_dict.get("Operation", "an operation")
        table_name = node_dict.get("Relation Name", "the table")
        node_desc = f"Perform {operation} on {table_name}."
        
    elif node_type == "Sample Scan":
        table_name = node_dict.get("Relation Name", "the table")
        sample_method = node_dict.get("Sample Method", "a sample method")
        sample_percent = node_dict.get("Sample Percentage", "a certain percentage")
        node_desc = f"Perform a Sample Scan on {table_name} using {sample_method} and {sample_percent}."
        
    elif node_type == "SetOp":
        set_operation = node_dict.get("Set Operation", "")
        if(set_operation):
            node_desc = f"Perform a Set Operation ({set_operation}) on the input result sets."
        else:
            node_desc = "Perform an unknown Set Operation on the input result sets"
        
    elif node_type == "WorkTable Scan":
        table_name = node_dict.get("Relation Name", "the work table")
        node_desc = f"Perform a WorkTable Scan on {table_name}."
        
    elif node_type == "Incremental Sort":
        presorted_key = node_dict.get("Presorted Key", "None")
        sort_key = node_dict.get("Sort Key", "None")
        node_desc = f"Perform an Incremental Sort.Presorted Key: {presorted_key}, Sort Key: {sort_key}."

            
    elif node_type == "Parallel Append":
        node_desc = "Append the results of all subplans in parallel into a single result set."
        
    else:
        node_desc = f"Perform {node_type} operation"
    return node_desc

def query_difference(node_list, node_list_2):

    descriptions_1, explanations_1 = split_list(node_list)
    descriptions_2, explanations_2 = split_list(node_list_2)
    descriptions_1=formatting(descriptions_1)
    descriptions_2=formatting(descriptions_2)
    diff = []
    count_1 = 1
    count_2 = 1

    if explanations_1 != explanations_2:
        matcher = difflib.SequenceMatcher(None, explanations_1, explanations_2, False)

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            str1 = ' '.join(explanations_1[i1:i2])
            str2 = ' '.join(explanations_2[j1:j2])

            if tag == 'equal':
                for _ in str2.split('. '):
                    #diff.append((count_2, 'equal', str(line)+" "))
                    count_1 += 1
                    count_2 += 1
            elif tag == 'replace':
                for _ in str2.split('. '):
                    diff.append({"compare":str(count_1)+ '|replace|'+ descriptions_1[count_1-1]+" has been replaced by "+descriptions_2[count_2-1]})
                    count_1 += 1
                    count_2 += 1
            elif tag == 'delete':
                for _ in str1.split('. '):
                    diff.append({"compare":str(count_1)+ '|delete|'+ descriptions_1[count_1-1]+" has been removed from the query"})
                    count_1 += 1
            elif tag == 'insert':
                for _ in str2.split('. '):
                    diff.append({"compare":str(count_1)+'|insert|'+ descriptions_2[count_2-1]+" has been inserted into the query"})
                    count_1 += 1
                    count_2 += 1

    return diff

def split_list(list):
    descriptions = []
    explanations = []
    for row in list:
        descriptions.append(row[0])
        explanations.append(row[2])
    return descriptions, explanations

def formatting(sql_query):
    grouped_sql = []

    for line in sql_query:
        stripped_line = line.strip().replace(" ", "-")
        grouped_sql.append(stripped_line)

    return grouped_sql

def in_order_traversal(data, node_list):
    if data['left_child'] is not None:
        node_list = in_order_traversal(data['left_child'], node_list)
    if data['right_child'] is not None:
        node_list = in_order_traversal(data['right_child'], node_list)
    description = data['description']
    filter = data['filters']
    explanation = data['explanation']
    node_list.append((description, filter, explanation))
    return node_list
