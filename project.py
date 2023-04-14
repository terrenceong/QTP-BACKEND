from explain import *
from flask import Flask,request
from flask_cors import CORS

app = Flask("QepAPI")
CORS(app)
query_text = """
SELECT c_address, COUNT(*) FROM customer JOIN nation ON customer.c_nationkey = nation.n_nationkey WHERE nation.n_regionkey = 1 GROUP BY c_address;
"""

query_text_2 = """
SELECT c_address, COUNT(*) FROM customer JOIN nation ON customer.c_nationkey = nation.n_nationkey WHERE nation.n_regionkey = 1 OR nation.n_regionkey = 2 GROUP BY c_address;
"""

@app.route("/api/single", methods=['POST'],endpoint="singleQuery")
def singleQuery():
    try:
        data = request.get_json()
        plan = query_handler(data["query"])
        query_plan_dict = plan[0][0][0]
        query_plan_tree = build_query_plan_tree(query_plan_dict["Plan"])
        return query_plan_tree ,200
    except:
        return {"ErrorMessage":"Invalid SQL Query"},400
    
@app.route("/api/compare", methods=['POST'],endpoint="compareQuery")
def compareQuery():
    try:
        data = request.get_json()
        plan1 = query_handler(data["query1"])
        query_plan_dict1 = plan1[0][0][0]
        query_plan_tree1 = build_query_plan_tree(query_plan_dict1["Plan"])
        plan2 = query_handler(data["query2"])
        query_plan_dict2 = plan2[0][0][0]
        query_plan_tree2 = build_query_plan_tree(query_plan_dict2["Plan"])
        node_list = []
        node_list = in_order_traversal(query_plan_tree1, node_list)
        node_list_2 = []
        node_list_2 = in_order_traversal(query_plan_tree2, node_list_2)
        diff_explanation = query_difference(node_list, node_list_2)
        return [query_plan_tree1,query_plan_tree2,diff_explanation] ,200
    except:
        return {"ErrorMessage":"Invalid SQL Query"},400

if __name__ == "__main__":
    app.run(host='localhost',port=5000)