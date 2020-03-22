from pysqlite3 import dbapi2 as sqlite3
from os import path
from sympy import subsets
import argparse
import json
import queue

def prepare_table_to_be_k_anonymized(dataset, cursor):
    with open(dataset, "r") as dataset_table:
        table_name = path.basename(dataset).split(".")[0]
        print("Working on dataset " + table_name)

        # first line contains attribute names
        attribute_names = dataset_table.readline().split(",")
        for attr in attribute_names:
            table_attribute = attr.strip() + " TEXT"
            attributes.append(table_attribute.replace("-", "_"))
        cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + "(" + ','.join(attributes) + ")")
        print("Attributes found: " + str([attr.strip() for attr in attribute_names]))

        # insert records into the SQL table
        for line in dataset_table:
            values = line.split(",")
            new_values = list()
            for value in values:
                value = value.strip()
                if value.__contains__("-"):
                    value = value.replace("-", "_")
                new_values.append(value)

            # a line could be a "\n" => new_values ===== [''] => len(new_values) == 1
            if len(new_values) == 1:
                continue
            cursor.execute("INSERT INTO " + table_name + ' values ({})'.format(new_values)
                           .replace("[", "").replace("]", ""))

def get_dimension_tables():
    print("Getting dimension tables", end="")
    json_text = ""
    with open(args.dimension_tables, "r") as dimension_tables_filename:
        for line in dimension_tables_filename:
            json_text += line.strip()
    print("\t OK")
    return json.loads(json_text)


def create_dimension_tables(tables):
    for qi in tables:

        # create SQL table
        columns = list()
        for i in tables[qi]:
            columns.append("'" + i + "' TEXT")
        cursor.execute("CREATE TABLE IF NOT EXISTS " + qi + "_dim (" + ", ".join(columns) + ")")

        # insert values into the newly created table
        rows = list()
        for i in range(len(tables[qi]["1"])):
            row = "("
            for j in tables[qi]:
                row += "'" + str(tables[qi][j][i]) + "', "
            row = row[:-2] + ")"
            rows.append(row)
        cursor.execute("INSERT INTO " + qi + "_dim VALUES " + ", ".join(rows))


def get_parent_index_C1(index, parent1_or_parent2):
    parent_index = index - parent1_or_parent2
    if parent_index < 0:
        parent_index = "null"
    return parent_index


def init_C1_and_E1():
    print("Generating graph for 1 quasi-identifier", end="")
    id = 1
    for dimension in dimension_tables:
        index = 0
        for i in range(len(dimension_tables[dimension])):
            # parenty = index - y
            # parent2 is the parent of parent1
            parent1 = get_parent_index_C1(index, 1)
            parent2 = get_parent_index_C1(index, 2)
            tuple = (id, dimension, index, parent1, parent2)
            cursor.execute("INSERT INTO C1 values (?, ?, ?, ?, ?)", tuple)
            if index >= 1:
                cursor.execute("INSERT INTO E1 values (?, ?)", (id - 1, id))
            id += 1
            index += 1
    print("\t OK")


def create_tables_Ci_Ei():
    # Ci initally has only one pair dimx, indexx, which will increment if k-anonymity is not achieved
    # autoincrement id starts from 1 by default
    # parent1 == [3], parent2 == [4]
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS C1 (ID INTEGER PRIMARY KEY, dim1 TEXT, index1 INT, parent1 INT, parent2 INT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS E1 (start INT, end INT)")


def get_height_of_node(node):
    # sum of the indexes in a row (node)
    i = 0
    height = 0
    length = len(node)
    while True:
        # 2, 6, 8, ...
        if i == 0:
            j = 2
        else:
            j = 6 + 2*(i-1)
        if j >= length:
            break
        if node[j] != 'null':
            height += node[j]
        i += 1
    return height


def get_dimensions_of_node(node):
    dimensions_temp = set()
    i = 0
    length = len(node)
    while True:
        # 1, 5, 7, 9, ...
        if i == 0:
            j = 1
        else:
            j = 5 + 2*(i-1)
        if j >= length:
            break
        if node[j] != 'null':
            dimensions_temp.add(node[j])
        i += 1
    return dimensions_temp


def frequency_set_of_T_wrt_attributes_of_node_using_T(node):
    attributes = get_dimensions_of_node(node)
    try:
        attributes.remove("null")
    except:
        pass
    dims_and_indexes_s_node = get_dims_and_indexes_of_node(node)
    group_by_attributes = set(attributes)
    dimension_table_names = list()
    where_items = list()
    for i in range(len(dims_and_indexes_s_node)):
        if dims_and_indexes_s_node[i][0] == "null" or dims_and_indexes_s_node[i][1] == "null":
            continue
        column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str = prepare_query_parameters(
            attributes, dims_and_indexes_s_node, group_by_attributes, i)

        where_item = "" + dataset + "." + column_name + " = " + dimension_with_previous_generalization_level

        dimension_table_names.append(dimension_table)
        where_items.append(where_item)

    cursor.execute("SELECT COUNT(*) FROM " + dataset + ", " + ', '.join(dimension_table_names) +
                   " WHERE " + 'and '.join(where_items) + " GROUP BY " + ', '.join(group_by_attributes))
    freq_set = list()
    for count in list(cursor):
        freq_set.append(count[0])
    return freq_set


def get_dims_and_indexes_of_node(node):
    list_temp = list()
    i = 0
    length = len(node)
    while True:
        # dims = 1, 5, 7, ...
        # indexes = 2, 6, 8, ... = dims + 1
        if i == 0:
            j = 1
        else:
            j = 5 + 2*(i-1)
        if j >= length or j+1 >= length:
            break
        list_temp.append((node[j], node[j+1]))
        i += 1
    return list_temp


def frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(node, i):
    i_str = str(i)
    cursor.execute("SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE E" + i_str + ".start = C" + i_str +
                   ".ID and E" + i_str + ".end = " + str(node[0]))
    dims_and_indexes_s_node = get_dims_and_indexes_of_node(node)

    attributes = get_dimensions_of_node(node)
    try:
        while True:
            attributes.remove("null")
    except:
        pass
    cursor.execute("CREATE TEMPORARY TABLE TempTable (count INT, " + ', '.join(attributes) + ")")

    select_items = list()
    where_items = list()
    group_by_attributes = set(attributes)
    dimension_table_names = list()

    for i in range(len(dims_and_indexes_s_node)):

        if dims_and_indexes_s_node[i][0] == "null" or dims_and_indexes_s_node[i][1] == "null":
            continue
        column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str = prepare_query_parameters(
            attributes, dims_and_indexes_s_node, group_by_attributes, i)

        select_item = dimension_table + ".\"" + generalization_level_str + "\" AS " + column_name
        where_item = "" + dataset + "." + column_name + " = " + dimension_with_previous_generalization_level

        select_items.append(select_item)
        where_items.append(where_item)
        dimension_table_names.append(dimension_table)

    cursor.execute("INSERT INTO TempTable"
                   " SELECT COUNT(*) AS count, " + ', '.join(select_items) +
                   " FROM " + dataset + ", " + ', '.join(dimension_table_names) +
                   " WHERE " + 'AND '.join(where_items) +
                   " GROUP BY " + ', '.join(group_by_attributes))

    cursor.execute("SELECT SUM(count) FROM TempTable GROUP BY " + ', '.join(attributes))
    results = list(cursor)
    freq_set = list()
    for result in results:
        freq_set.append(result[0])

    cursor.execute("DROP TABLE TempTable")

    return freq_set


def prepare_query_parameters(attributes, dims_and_indexes_s_node, group_by_attributes, i):
    column_name = dims_and_indexes_s_node[i][0]
    generalization_level = dims_and_indexes_s_node[i][1]
    generalization_level_str = str(generalization_level)
    previous_generalization_level_str = "0"
    dimension_table = column_name + "_dim"
    dimension_with_previous_generalization_level = dimension_table + ".\"" + previous_generalization_level_str + "\""
    if column_name in attributes:
        group_by_attributes.remove(column_name)
        group_by_attributes.add(dimension_table + ".\"" + generalization_level_str + "\"")
    return column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str


def mark_all_direct_generalizations_of_node(marked_nodes, node, i):
    i_str = str(i)
    marked_nodes.add(node[0])
    cursor.execute("SELECT E" + i_str + ".end FROM C" + i_str + ", E" + i_str + " WHERE ID = E" + i_str +
                   ".start and ID = " + str(node[0]))
    for node_to_mark in list(cursor):
        marked_nodes.add(node_to_mark[0])


def insert_direct_generalization_of_node_in_queue(node, queue, i, Si):
    i_str = str(i)
    cursor.execute("SELECT E" + i_str + ".end FROM C" + i_str + ", E" + i_str + " WHERE ID = E" + i_str +
                   ".start and ID = " + str(node[0]))
    nodes_to_put = set(cursor)

    Si_indices = set()
    for node in Si:
        Si_indices.add(node[0])

    for node_to_put in nodes_to_put:
        # node_to_put == (ID,) -.-
        if node_to_put[0] not in Si_indices:
            continue
        node_to_put = node_to_put[0]
        cursor.execute("SELECT * FROM C" + i_str + " WHERE ID = " + str(node_to_put))
        node = (list(cursor)[0])
        queue.put_nowait((-get_height_of_node(node), node))


def graph_generation(Si, i):
    i_here = i+1
    i_str = str(i)
    ipp_str = str(i+1)
    if i < len(Q):
        print("Generating graphs for " + ipp_str + " quasi-identifiers", end="")
    # to create Si i need all column names of Ci
    # PRAGMA returns infos like (0, 'ID', 'INTEGER', 0, None, 1), (1, 'dim1', 'TEXT', 0, None, 0), ...
    cursor.execute("PRAGMA table_info(C" + i_str + ")")
    column_infos = list()
    column_infos_from_db = list(cursor)
    for column in column_infos_from_db:
        if column[1] == "ID":
            column_infos.append("ID INTEGER PRIMARY KEY")
        else:
            column_infos.append(str(column[1]) + " " + str(column[2]))
    cursor.execute("CREATE TABLE IF NOT EXISTS S" + i_str + " (" + ', '.join(column_infos) + ")")
    question_marks = ""
    for j in range(0, len(column_infos_from_db) - 1):
        question_marks += " ?,"
    question_marks += " ? "

    cursor.executemany("INSERT INTO S" + i_str + " values (" + question_marks + ")", Si)

    cursor.execute("SELECT * FROM S" + i_str)
    Si_new = set(cursor)

    # in the last iteration are useless the phases because after graph_generation only Si (Sn) is taken
    # into account
    if i == len(Q):
        return
    i_here_str = str(i_here)
    cursor.execute("CREATE TABLE IF NOT EXISTS C" + ipp_str + " (" + ', '.join(column_infos) + ")")
    cursor.execute("ALTER TABLE C" + ipp_str + " ADD COLUMN dim" + i_here_str + " TEXT")
    cursor.execute("ALTER TABLE C" + ipp_str + " ADD COLUMN index" + i_here_str + " INT")
    # UPDATE Ci SET dim2 = 'null', index2 = 'null' WHERE Ci.index2 is null
    cursor.execute("UPDATE C" + ipp_str + " SET dim" + i_here_str + " = 'null', index" + i_here_str +
                   "= 'null' WHERE index" + i_here_str + " is null")
    select_str = ""
    select_str_except = ""
    where_str = ""
    for j in range(2, i_here):
        j_str = str(j)
        if j == i_here-1:
            select_str += ", p.dim" + j_str + ", p.index" + j_str + ", q.dim" + j_str + ", q.index" + j_str
            select_str_except += ", q.dim" + j_str + ", q.index" + j_str + ", p.dim" + j_str + ", p.index" + j_str
            where_str += " and p.dim" + j_str + "<q.dim" + j_str
        else:
            select_str += ", p.dim" + j_str + ", p.index" + j_str
            select_str_except += ", q.dim" + j_str + ", q.index" + j_str
            where_str += " and p.dim" + j_str + "=q.dim" + j_str + " and p.index" + j_str + "=q.index" + j_str

    # join phase. Ci == Ci+1
    if i > 1:
        cursor.execute("INSERT INTO C" + ipp_str +
                        " SELECT null, p.dim1, p.index1, p.ID, q.ID" + select_str +
                        " FROM S" + i_str + " p, S" + i_str + " q WHERE p.dim1 = q.dim1 and p.index1 = q.index1 " + where_str)

    else:
        cursor.execute("INSERT INTO C" + ipp_str + " SELECT null, p.dim1, p.index1, p.ID, q.ID, q.dim1, q.index1"
                       " FROM S" + i_str + " p, S" + i_str + " q WHERE p.dim1<q.dim1")

    cursor.execute("SELECT * FROM C" + ipp_str)
    Ci_new = set(cursor)

    # prune phase
    # all subsets of Si == dims_and_indexes of all nodes in Si
    # forall node in Ci+1 I will remove that nodes which contains a subset of dims_and_indexes
    # which is not in all_subsets_of_Si

    all_subsets_of_Si = set()
    for node in Si_new:
        all_subsets_of_Si.add(tuple(get_dims_and_indexes_of_node(node)))
    for c in Ci_new:
        for s in subsets(get_dims_and_indexes_of_node(c), i):
            if s not in all_subsets_of_Si:
                node_id = str(c[0])
                cursor.execute("DELETE FROM C" + ipp_str + " WHERE C" + ipp_str + ".ID = " + node_id)
                #cursor.execute("DELETE FROM E" + i_str + " WHERE E" + i_str + ".start = " + node_id +
                #               " OR E" + i_str + ".end = " + node_id)

    # edge generation
    cursor.execute("CREATE TABLE IF NOT EXISTS E" + ipp_str + " (start INT, end INT)")
    cursor.execute("INSERT INTO E" + ipp_str + " "
                   "WITH CandidatesEdges(start, end) AS ("
                   "SELECT p.ID, q.ID "
                   "FROM C" + ipp_str + " p,C" + ipp_str + " q,E" + i_str + " e,E" + i_str + " f "
                   "WHERE (e.start = p.parent1 AND e.end = q.parent1 "
                   "AND f.start = p.parent2 AND f.end = q.parent2) "
                   "OR (e.start = p.parent1 AND e.end = q.parent1 "
                   "AND p.parent2 = q.parent2) "
                   "OR (e.start = p.parent2 AND e.end = q.parent2 "
                   "AND p.parent1 = q.parent1) "
                   ") "
                   "SELECT D.start, D.end "
                   "FROM CandidatesEdges D "
                   "EXCEPT "
                   "SELECT D1.start, D2.end "
                   "FROM CandidatesEdges D1, CandidatesEdges D2 "
                   "WHERE D1.end = D2.start")
    print("\t OK")


def table_is_k_anonymous_wrt_attributes_of_node(frequency_set, k):
    """
    Relation T is said to satisfy the k-anonymity property (or to be k-anonymous) with respect to attribute set A if
    every count in the frequency set of T with respect to A is greater than or equal to k
    """
    if len(frequency_set) == 0:
        return False
    for count in frequency_set:
        if type(count) == tuple:
            count = count[0]
        if count < k:
            return False
    return True


def basic_incognito_algorithm(priority_queue, Q, k):
    init_C1_and_E1()
    queue = priority_queue
    # marked_nodes = {(marked, node_ID)}
    marked_nodes = set()

    for i in range(1, len(Q) + 1):
        i_str = str(i)
        cursor.execute("SELECT * FROM C" + i_str + "")
        Si = set(cursor)

        # no edge directed to a node => root
        cursor.execute("SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" + i_str + ".ID = E" + i_str + ".start "
                       "EXCEPT "
                       "SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" + i_str + ".ID = E" + i_str + ".end ")
        roots = set(cursor)
        roots_in_queue = set()

        for node in roots:
            height = get_height_of_node(node)
            # -height because priority queue shows the lowest first. Syntax: (priority number, data)
            roots_in_queue.add((-height, node))

        for upgraded_node in roots_in_queue:
            queue.put_nowait(upgraded_node)

        while not queue.empty():
            upgraded_node = queue.get_nowait()
            # [1] => pick 'node' in (-height, node);
            node = upgraded_node[1]
            if node[0] not in marked_nodes:
                if node in roots:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_T(node)
                else:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(node, i)
                if table_is_k_anonymous_wrt_attributes_of_node(frequency_set, k):
                    mark_all_direct_generalizations_of_node(marked_nodes, node, i)
                else:
                    Si.remove(node)
                    insert_direct_generalization_of_node_in_queue(node, queue, i, Si)
                    cursor.execute("DELETE FROM C" + str(i) + " WHERE ID = " + str(node[0]))

        graph_generation(Si, i)
        marked_nodes = set()


def projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn):
    # get node with lowest height, as it should be the least "generalized" one that makes the table k-anonymous
    lowest_node = min(Sn, key = lambda t: t[0])
    height = get_height_of_node(lowest_node)
    for node in Sn:
        temp_height = get_height_of_node(node)
        if temp_height < height:
            height = temp_height
            lowest_node = node

    print("Chosen anonymization levels: ", end="")

    # get QI names and their indexes (i.e. their generalization level)
    qis = list()
    qi_indexes = list()
    for i in range(len(lowest_node)):
        if lowest_node[i] in Q:
            qis.append(lowest_node[i])
            qi_indexes.append(lowest_node[i+1])
            print(str(lowest_node[i]) + "(" + str(lowest_node[i+1]) + ") ", end="")
    print("")

    # get all table attributes with generalized QI's in place of the original ones
    gen_attr = attributes
    for i in range(len(gen_attr)):
        gen_attr[i] = gen_attr[i].split()[0]
        if gen_attr[i] in qis:
            gen_attr[i] = qis[qis.index(gen_attr[i])] + "_dim.'" + str(qi_indexes[qis.index(gen_attr[i])]) + "'"

    # get dimension tables names
    dim_tables = list()
    for qi in qis:
        dim_tables.append(qi + "_dim")

    # get pairings for the SQL JOIN
    pairs = list()
    for x, y in zip(qis, dim_tables):
        pairs.append(x + "=" + y + ".'0'")

    cursor.execute("SELECT " + ', '.join(gen_attr) + " FROM " + dataset + ", " + ', '.join(dim_tables) +
          " WHERE " + 'AND '.join(pairs))

    print("Writing k-anonymous table to anonymous_table.csv", end="")
    with open("anonymous_table.csv", "w") as anonymous_table:
        for row in list(cursor):
            anonymous_table.writelines(','.join(str(x) for x in row) + "\n")
        anonymous_table.close()
    print("\t OK")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Insert path and filename of QI, "
                                                 "path and filename of dimension tables and"
                                                 "k of k-anonymization")
    parser.add_argument("--dataset", "-d", required=True, type=str)
    parser.add_argument("--dimension-tables", "-D", required=True, type=str)
    parser.add_argument("--k", "-k", required=True, type=str)
    args = parser.parse_args()

    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = OFF")
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE")

    # all attributes of the table
    attributes = list()

    dataset = args.dataset

    prepare_table_to_be_k_anonymized(dataset, cursor)

    dataset = path.basename(dataset).split(".")[0]

    """
     dimension_tables is a dictionary in which a single key is a specific QI (except the first that indicates the type) and
     dimension_tables[QI] is the dimension table of QI. eg:
     <class 'dict'>: {'age': {'0': [1, 2, 3], '1': [4, 5]}, 'occupation': {'0': ['a', 'b', 'c'], '1': ['d', 'e'], '2': ['*']}}
    
     Q is a set containing the quasi-identifiers. eg:
     <class 'set'>: {'age', 'occupation'}
    """
    dimension_tables = get_dimension_tables()
    Q = set(dimension_tables.keys())

    # create dimension SQL tables
    create_dimension_tables(dimension_tables)

    k = int(args.k)

    cursor.execute("SELECT * FROM " + dataset)
    if k > len(list(cursor)) or k <= 0:
        print("ERROR: k value is invalid")
        exit(0)
    # the first domain generalization hierarchies are the simple A0->A1, O0->O1->O2 and, obviously, the first candidate
    # nodes Ci (i=1) are the "0" ones, that is Ci={A0, O0}. I have to create the Nodes and Edges tables
    create_tables_Ci_Ei()

    # I must pass the priorityQueue otherwise the body of the function can't see and instantiates a PriorityQueue -.-
    basic_incognito_algorithm(queue.PriorityQueue(), Q, k)

    cursor.execute("SELECT * FROM S" + str(len(Q)))
    Sn = list(cursor)

    projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn)

    print("DONE")

    connection.close()
