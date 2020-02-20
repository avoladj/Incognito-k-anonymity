import sqlite3
import re
import argparse
import json
import queue


def prepare_table_to_be_k_anonymized(cursor):
    attributes = list()
    path_to_datasets = "../datasets/"
    # get attributes from adult.names
    with open(path_to_datasets + "adult.names", "r") as adult_names:
        for line in adult_names:
            if not line.startswith("|") and re.search(".:.", line) is not None:
                split = line.split(":")
                name_and_type_of_attribute_to_append = split[0].strip().replace("-", "_")
                if split[1].strip() == "continuous.":
                    name_and_type_of_attribute_to_append += " REAL"
                else:
                    name_and_type_of_attribute_to_append += " TEXT"
                attributes.append(name_and_type_of_attribute_to_append)
    # insert records in adult.data in table AdultData
    with open(path_to_datasets + "adult.data", "r") as adult_data:
        table_name = "AdultData"
        cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + "(" + ','.join(attributes) + ")")
        connection.commit()

        for line in adult_data:
            """
            For each line I remove the 'classification' attribute (<>=50K), 
            every number will be converted to a float and
            replace - with _, otherwise sqlite3 bothers
            """
            values = line.rstrip(", <=50K\n").rstrip(", >50K\n").split(",")
            new_values = list()
            for value in values:
                value = value.strip()
                if value.isnumeric():
                    value = float(value)
                elif value.__contains__("-"):
                    value = value.replace("-", "_")
                new_values.append(value)

            # a line could be a "\n" => new_values ===== [''] => len(new_values) == 1
            if len(new_values) == 1:
                continue
            cursor.execute("INSERT INTO " + table_name + ' values ({})'.format(new_values)
                           .replace("[", "").replace("]", ""))
            connection.commit()


def get_quasi_identifiers():
    Q_temp = set()
    with open(args.quasi_identifiers, "r") as qi_filename:
        quasi_identifiers = qi_filename.readline().split(",")
        for qi in quasi_identifiers:
            Q_temp.add(qi.strip())
    return Q_temp


def get_dimension_tables():
    json_text = ""
    with open(args.dimension_tables, "r") as dimension_tables_filename:
        for line in dimension_tables_filename:
            json_text += line.strip()
    return json.loads(json_text)


def get_parent_index_C1(index, parent1_or_parent2):
    parent_index = index - parent1_or_parent2
    if parent_index < 0:
        parent_index = "null"
    return parent_index


def init_C1_and_E1():
    id = 1
    for dimension in dimension_tables:
        index = 0
        for node in dimension_tables[dimension]:
            # parenty = index - y
            parent1 = get_parent_index_C1(index, 1)
            parent2 = get_parent_index_C1(index, 2)
            tupla = (id, node, index, parent1, parent2)
            cursor.execute("INSERT INTO Ci values (?, ?, ?, ?, ?)", tupla)
            if index >= 1:
                cursor.execute("INSERT INTO Ei values (?, ?)", (id - 1, id))
            id += 1
            index += 1
    connection.commit()
    """
    cursor.execute("SELECT * FROM Ci")
    print(list(cursor))
    cursor.execute("SELECT * FROM Ei")
    print(list(cursor))
    """


def create_tables_Ci_Ei():
    # autoincrement id starts from 1 by default
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS Ci (ID INTEGER PRIMARY KEY, dim1 TEXT, index1 INT, parent1 INT, parent2 INT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS Ei (start INT, end INT)")
    connection.commit()


def get_height_of_node(node):
    # sum of the indexes in a row (node)
    i = 0
    height = 0
    length = len(node)
    while True:
        # 2+5*0, 2+5*1, 2+10*2, ...
        j = 2 + 5*i
        if j >= length:
            break
        height += node[j]
        i += 1
    return height


def frequency_set_of_T_wrt_attributes_of_node_using_T(Q):
    cursor.execute("SELECT COUNT(*) FROM AdultData GROUP BY " + ', '.join(Q))
    freq_set = list()
    for count in list(cursor):
        freq_set.append(count[0])
    return freq_set


def frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(frequency_set):
    # TODO
    return frequency_set


def mark_all_direct_generalizations_of_node(marked_nodes, node):
    cursor.execute("SELECT Ei.end FROM Ci, Ei WHERE ID = Ei.start and ID = " + str(node[0]))
    for node_to_mark in list(cursor):
        marked_nodes.add(node_to_mark[0])


def get_height_of_node_by_id(node_to_put):
    cursor.execute("SELECT * FROM Ci WHERE ID = " + str(node_to_put))
    return get_height_of_node(list(cursor)[0])


def insert_direct_generalization_of_node_in_queue(node, queue):
    cursor.execute("SELECT Ei.end FROM Ci, Ei WHERE ID = Ei.start and ID = " + str(node[0]))
    nodes_to_put = set(cursor)
    for node_to_put in nodes_to_put:
        # node_to_put == (ID,) -.-
        node_to_put = node_to_put[0]
        cursor.execute("SELECT * FROM Ci WHERE ID = " + str(node_to_put))
        node = (list(cursor)[0])
        queue.put_nowait((-get_height_of_node(node), node))


def graph_generation(Si, Ei, i):
    cursor.execute("ALTER TABLE Ci ADD COLUMN dim" + str(i) + " TEXT")
    cursor.execute("ALTER TABLE Ci ADD COLUMN index" + str(i) + " INT")
    connection.commit()
    # TODO
    cursor.execute("INSERT INTO Ei "
                   "WITH CandidatesEdges(start, end) AS ("
                   "SELECT p.ID, q.ID"
                   "FROM Ci as p,Ci as q,Ei as e,Ei as f"
                   "WHERE (e.start = p.parent1 and e.end = q.parent1"
                   "and f.start = p.parent2 and f.end = q.parent2)"
                   "or (e.start = p.parent1 and e.end = q.parent1"
                   "and p.parent2 = q.parent2)"
                   "or (e.start = p.parent2 and e.end = q.parent2"
                   "and p.parent1 = q.parent1)"
                   ")"
                   "SELECT D.start, D.end"
                   "FROM CandidateEdges as D"
                   "EXCEPT"
                   "SELECT D1.start, D2.end"
                   "FROM CandidateEdges as D1, CandidateEdges as D2"
                   "WHERE D1.end = D2.start")
    print(list(cursor))
    pass


def table_is_k_anonymous_wrt_attributes_of_node(frequency_set, k):
    return len(frequency_set) < int(k)


def basic_incognito_algorithm(cursor, priority_queue, Q, k):
    init_C1_and_E1()
    queue = priority_queue
    # marked_nodes = {(marked, node_ID)}
    marked_nodes = set()

    for i in range(1, len(Q)):
        cursor.execute("SELECT * FROM Ci")
        Si = set(cursor)

        # theese 3 lines for practicality
        Ci = set(Si)
        cursor.execute("SELECT * FROM Ei")
        Ei = set(cursor)

        # no edge directed to them ==== have no parent1 (and no parent2)
        cursor.execute("SELECT * FROM Ci WHERE parent1='null' ")
        roots = set(cursor)
        roots_in_queue = set()

        for node in roots:
            # height = 0 because these nodes are roots
            height = get_height_of_node(node)
            # -height because priority queue shows the lowest first. Syntax: (priority number, data)
            roots_in_queue.add((-height, node))

        for upgraded_node in roots_in_queue:
            queue.put_nowait(upgraded_node)

        while not queue.empty():
            upgraded_node = queue.get_nowait()
            # [1] => pick 'node' in (-height, node);
            node = upgraded_node[1]
            if node not in marked_nodes:
                if node in roots:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_T(Q)
                else:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(frequency_set)
                if table_is_k_anonymous_wrt_attributes_of_node(frequency_set, k):
                    mark_all_direct_generalizations_of_node(marked_nodes, node)
                else:
                    Si.remove(node)
                    insert_direct_generalization_of_node_in_queue(node, queue)
        # Ci, Ei =
        graph_generation(Si, Ei, i+1)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Insert path and filename of QI, "
                                                 "path and filename of dimension tables and"
                                                 "k of k-anonymization")
    parser.add_argument("--quasi_identifiers", "-Q", required=True, type=str)
    parser.add_argument("--dimension_tables", "-D", required=True, type=str)
    parser.add_argument("--k", "-k", required=True, type=str)
    args = parser.parse_args()

    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()

    prepare_table_to_be_k_anonymized(cursor)

    # Q is a set containing the quasi-identifiers. eg:
    # <class 'set'>: {'age', 'occupation'}
    Q = get_quasi_identifiers()

    """
     dimension_tables is a dictionary in which a single key is a specific QI and
     dimension_tables[QI] is the dimension table of QI. eg:
     <class 'dict'>: {'age': {'A0': [1, 2, 3], 'A1': [4, 5]}, 'occupation': {'O0': ['a', 'b', 'c'], 'O1': ['d', 'e'], 'O2': ['*']}}
    """
    dimension_tables = get_dimension_tables()

    k = args.k

    # the first domain generalization hierarchies are the simple A0->A1, O0->O1->O2 and, obviously, the first candidate
    # nodes Ci (i=1) are the "0" ones, that is Ci={A0, O0}. I have to create the Nodes and Edges tables

    create_tables_Ci_Ei()

    # I must pass the priorityQueue otherwise the body of the function can't see and instantiates a PriorityQueue -.-
    basic_incognito_algorithm(cursor, queue.PriorityQueue(), Q, k)

    connection.close()
