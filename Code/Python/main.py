from pysqlite3 import dbapi2 as sqlite3
import re
import argparse
import json
import queue

from sympy import subsets


def prepare_table_to_be_k_anonymized(cursor):
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


def create_dimension_tables(tables):
    for qi in tables:

        # create SQL table
        columns = list()
        for i in tables[qi]:
            if i == "type":
                continue
            columns.append("'" + i + "' " + tables[qi]["type"])
        cursor.execute("CREATE TABLE IF NOT EXISTS " + qi + "_dim (" + ", ".join(columns) + ")")
        connection.commit()

        # insert values into the newly created table
        rows = list()
        if tables[qi]["type"] == "text":
            for i in range(len(tables[qi]["1"])):
                row = "("
                for j in tables[qi]:
                    if j == "type":
                        continue
                    row += "'" + str(tables[qi][j][i]) + "', "
                row = row[:-2] + ")"
                rows.append(row)
            cursor.execute("INSERT INTO " + qi + "_dim VALUES " + ", ".join(rows))
            connection.commit()
        else:
            for i in range(len(tables[qi]["1"])):
                row = "("
                for j in tables[qi]:
                    if j == "type":
                        continue
                    row += str(tables[qi][j][i]) + ", "
                row = row[:-2] + ")"
                rows.append(row)
            cursor.execute("INSERT INTO " + qi + "_dim VALUES " + ", ".join(rows))
            connection.commit()


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
            if str(node) == 'type':
                continue
            # parenty = index - y
            # parent2 is the parent of parent1
            parent1 = get_parent_index_C1(index, 1)
            parent2 = get_parent_index_C1(index, 2)
            tuple = (id, dimension, index, parent1, parent2)
            cursor.execute("INSERT INTO Ci values (?, ?, ?, ?, ?)", tuple)
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
    # Ci initally has only one pair dimx, indexx, which will increment if k-anonymity is not achieved
    # autoincrement id starts from 1 by default
    # parent1 == [3], parent2 == [4]
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


def frequency_set_of_T_wrt_attributes_of_node_using_T(node, Q):
    attributes = get_dimensions_of_node(node)
    #print("SELECT COUNT(*), " + ', '.join(attributes) + " FROM AdultData GROUP BY " + ', '.join(attributes))
    cursor.execute("SELECT COUNT(*), " + ', '.join(attributes) + " FROM AdultData GROUP BY " + ', '.join(attributes))
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


def frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(Ei, map_frequency_set, node, Q):

    cursor.execute("SELECT Ci.* FROM Ci, Ei WHERE Ei.start = Ci.ID and Ei.end = " + str(node[0]))
    parent_nodes = list(cursor)
    dims_and_indexes_s_node = get_dims_and_indexes_of_node(node)

    changed_qis = list()

    for parent_node in parent_nodes:
        dims_and_indexes_s_parent_node = get_dims_and_indexes_of_node(parent_node)

        for i in range(len(dims_and_indexes_s_node)):
            if dims_and_indexes_s_node[i] > dims_and_indexes_s_parent_node[i]:
                changed_qis.append(dims_and_indexes_s_node[i])

    attributes = get_dimensions_of_node(node)
    cursor.execute("CREATE TEMPORARY TABLE TempTable (count INT, " + ', '.join(attributes) + ")")
    connection.commit()

    # SELECT COUNT(*), age FROM AdultData GROUP BY age
    # prendere la colonna 'age'
    # generalizzo ogni valore rispetto 'age' changed_qis[i][1]
    # creo JoinedTable con 'age1' con colonna generalizzata
    # SELECT SUM(COUNT) FROM JoinedTable GROUP BY age1

    select_items = list()
    where_items = list()
    group_by_attributes = set(attributes)
    dimension_table_names = list()

    for i in range(len(changed_qis)):

        column_name = changed_qis[i][0]
        generalization_level = changed_qis[i][1]
        generalization_level_str = str(generalization_level)
        #previous_generalization_level_str = str(generalization_level - 1)
        # TODO: temporary
        previous_generalization_level_str = "0"

        dimension_table = column_name + "_dim"
        dimension_with_previous_generalization_level = dimension_table + ".\"" + previous_generalization_level_str + "\""

        if column_name in attributes:
            group_by_attributes.remove(column_name)
            group_by_attributes.add(dimension_table + ".\"" + generalization_level_str + "\"")

        select_item = dimension_table + ".\"" + generalization_level_str + "\" AS " + column_name
        where_item = "AdultData." + column_name + " = " + dimension_with_previous_generalization_level

        select_items.append(select_item)
        where_items.append(where_item)
        dimension_table_names.append(dimension_table)

    cursor.execute("INSERT INTO TempTable "
                   "SELECT COUNT(*) as count, " + ', '.join(select_items) +
                   " FROM AdultData, " + ', '.join(dimension_table_names) +
                   " WHERE " + 'and '.join(where_items) +
                   " GROUP BY " + ', '.join(group_by_attributes))

    # cursor.execute("SELECT * FROM TempTable")
    # print(cursor.fetchall())

    cursor.execute("SELECT SUM(count) FROM TempTable GROUP BY " + ', '.join(attributes))
    results = list(cursor)
    freq_set = list()
    for result in results:
        freq_set.append(result[0])

    cursor.execute("DROP TABLE TempTable")
    connection.commit()

    return freq_set


def mark_all_direct_generalizations_of_node(marked_nodes, node):
    marked_nodes.add(node[0])
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


def all_subsets(c, i):
    my_set = set()
    # extract only the ids
    p = 0
    length = len(c)
    while True:
        # 0+7*0, 0+7*1, 0+7*2, ...
        j = 7*p
        if j >= length:
            break
        my_set.add(c[j])
        p += 1
    return subsets(my_set, i)



def graph_generation(Ci, Si, Ei, i):
    i_here = i+1
    # to create Si i need all columnnames of Ci
    # PRAGMA returns infos like (0, 'ID', 'INTEGER', 0, None, 1), (1, 'dim1', 'TEXT', 0, None, 0), ...
    cursor.execute("PRAGMA table_info(Ci)")
    column_infos = list()
    column_infos_from_db = list(cursor)
    for column in column_infos_from_db:
        column_infos.append(str(column[1]) + " " + str(column[2]))
    cursor.execute("CREATE TABLE IF NOT EXISTS Si (" + ', '.join(column_infos) + ")")
    connection.commit()
    question_marks = ""
    for j in range(0, len(column_infos_from_db) - 1):
        question_marks += " ?,"
    question_marks += " ? "
    cursor.executemany("INSERT INTO Si values (" + question_marks + ")", Si)

    """
    serve per console sqlite database pycharm 
    mia_zia = list()
    for tupla in Si:
        mia_zia.append("(" + str(tupla[0]) + " ," + str(tupla[1]) + " ," + str(tupla[2]) + " ," +
                       str(tupla[3]) + " ," + str(tupla[4]) + ")")

    print("INSERT INTO Si values " + ", ".join(mia_zia))
    """

    cursor.execute("SELECT * FROM Si")
    Si_new = set(cursor)

    i_here_str = str(i_here)
    cursor.execute("ALTER TABLE Ci ADD COLUMN dim" + i_here_str + " TEXT")
    cursor.execute("ALTER TABLE Ci ADD COLUMN index" + i_here_str + " INT")
    # UPDATE Ci SET dim2 = 'null', index2 = 'null' WHERE Ci.index2 is null
    cursor.execute("UPDATE Ci SET dim" + i_here_str + " = 'null', index" + i_here_str +
                   "= 'null' WHERE index" + i_here_str + " is null")
    connection.commit()

    help_me = ""
    help_me_now = ""
    # j starts from 1, but here the indexes of 'dim' and 'index' are 2 => j = 2, indexes = 3 ...
    # if i > 1 otherwise I have p.dim1 = q.dim1 and p.dim1 < q.dim1 ._.
    i_is_greater_than_1 = i > 1
    for j in range(1, i_here):
        indexes = j + 1
        if indexes == i_here:
            str_j = str(j)
            help_me += ", q.dim" + str_j + ", q.index" + str_j
            if i_is_greater_than_1:
                help_me_now += "and p.dim" + str_j + " < q.dim" + str_j + " "
        else:
            str_j = str(indexes)
            help_me += ", p.dim" + str_j + ", p.index" + str_j
            if i_is_greater_than_1:
                help_me_now += "and p.dim" + str_j + " = q.dim" + str_j \
                               + " and p.index" + str_j + " = q.index" + str_j + " "

    # join phase. Ci == Ci+1
    if i_is_greater_than_1:
        cursor.execute("INSERT INTO Ci "
                   "SELECT null, p.dim1, p.index1, p.ID, q.ID " + help_me + " "
                   "FROM Si p, Si q "
                   "WHERE p.dim1 = q.dim1 and p.index1 = q.index1 " + help_me_now)
    else:
        cursor.execute("INSERT INTO Ci "
                   "SELECT null, p.dim1, p.index1, p.ID, q.ID " + help_me + " "
                   "FROM Si p, Si q "
                   "WHERE p.index1 = q.index1 and p.dim1 < q.dim1")

    cursor.execute("SELECT * FROM Ci")
    Ci_new = set(cursor)

    Ci_map = dict()
    for c in Ci:
        keys = list()
        t = 0
        length = len(c)
        while True:
            r = 7 * t
            if r >= length:
                break
            keys.append(c[r])
            t += 1
        Ci_map[tuple(keys)] = c

    # prune phase
    for c in Ci_new:
        for s in all_subsets(c, i):
            if s in Ci_map.keys() and Ci_map[s] not in Si_new:
                node_id = str(c[0])
                cursor.execute("DELETE FROM Ci WHERE Ci.ID = " + node_id)
                cursor.execute("DELETE FROM Ei WHERE Ei.start = " + node_id)

    cursor.execute("SELECT * FROM Ci")
    #print(list(cursor))

    # edge generation
    cursor.execute("INSERT INTO Ei "
                   "WITH CandidatesEdges(start, end) AS ("
                   "SELECT p.ID, q.ID "
                   "FROM Ci p,Ci q,Ei e,Ei f "
                   "WHERE (e.start = p.parent1 and e.end = q.parent1 "
                   "and f.start = p.parent2 and f.end = q.parent2) "
                   "or (e.start = p.parent1 and e.end = q.parent1 "
                   "and p.parent2 = q.parent2) "
                   "or (e.start = p.parent2 and e.end = q.parent2 "
                   "and p.parent1 = q.parent1) "
                   ") "
                   "SELECT D.start, D.end "
                   "FROM CandidatesEdges D "
                   "EXCEPT "
                   "SELECT D1.start, D2.end "
                   "FROM CandidatesEdges D1, CandidatesEdges D2 "
                   "WHERE D1.end = D2.start")

    """
    cursor.execute("SELECT * FROM Ei")
    print(list(cursor))

    cursor.execute("SELECT * FROM Si")
    print(list(cursor))
    """
    if i != len(Q):
        cursor.execute("DROP TABLE Si")
        connection.commit()


def table_is_k_anonymous_wrt_attributes_of_node(frequency_set, k):
    """
    Relation T is said to satisfy the k-anonymity property (or to be k-anonymous) with respect to attribute set A if
    every count in the frequency set of T with respect to A is greater than or equal to k
    """
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
    map_node_frequency_set = dict()

    for i in range(1, len(Q) + 1):
        cursor.execute("SELECT * FROM Ci")
        Si = set(cursor)

        # these next 3 lines are for practicality
        Ci = set(Si)
        cursor.execute("SELECT * FROM Ei")
        Ei = set(cursor)

        # no edge directed to a node => root
        cursor.execute("SELECT Ci.* FROM Ci, Ei WHERE Ci.ID = Ei.start "
                       "EXCEPT "
                       "SELECT Ci.* FROM Ci, Ei WHERE Ci.ID = Ei.end ")
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
            if node[0] not in marked_nodes:
                if node in roots:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_T(node, Q)
                    map_node_frequency_set[node] = frequency_set
                    print("Freq_set root for " + str(node) + ": " + str(frequency_set))
                else:
                    frequency_set = frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(
                        Ei, map_node_frequency_set, node, Q)
                    print("Freq_set for " + str(node) + ": " + str(frequency_set))
                    map_node_frequency_set[node] = frequency_set
                if table_is_k_anonymous_wrt_attributes_of_node(frequency_set, k):
                    print("NODE " + str(node) + " IS ANONYMOUS")
                    mark_all_direct_generalizations_of_node(marked_nodes, node)
                else:
                    Si.remove(node)
                    insert_direct_generalization_of_node_in_queue(node, queue)

        graph_generation(Ci, Si, Ei, i)


def projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn):
    # get node with lowest ID, as it should be the least "generalized" one that makes the table k-anonymous
    # TODO: is it really the least generalized one?
    lowest_node = min(Sn, key = lambda t: t[0])
    #lowest_node = list(Sn)[0]
    print("Lowest node: " + str(lowest_node))

    # get QI names and their indexes (i.e. their generalization level)
    qis = list()
    qi_indexes = list()
    for i in range(len(lowest_node)):
        if lowest_node[i] in Q:
            qis.append(lowest_node[i])
            qi_indexes.append(lowest_node[i+1])
    #print("QIs: " + str(qis))
    #print("QI_indexes: " + str(qi_indexes))

    # get all table attributes with generalized QI's in place of the original ones
    gen_attr = attributes
    #print("Gen_attr before: " + str(gen_attr))
    for i in range(len(gen_attr)):
        gen_attr[i] = gen_attr[i].split()[0]
        if gen_attr[i] in qis:
            gen_attr[i] = qis[qis.index(gen_attr[i])] + "_dim.'" + str(qi_indexes[qis.index(gen_attr[i])]) + "'"
    #print("Gen_attr after: " + str(gen_attr))

    # get dimension tables names
    dim_tables = list()
    for qi in qis:
        dim_tables.append(qi + "_dim")

    # get pairings for the SQL JOIN
    pairs = list()
    for x, y in zip(qis, dim_tables):
        pairs.append(x + "=" + y + ".'0'")

    print("SELECT " + ', '.join(gen_attr) + " FROM AdultData, " + ', '.join(dim_tables) +
          " WHERE " + 'AND '.join(pairs))
    #cursor.execute("SELECT " + ', '.join(gen_attr) + " FROM AdultData, " + ', '.join(dim_tables) +
    #      " WHERE " + 'AND '.join(pairs))



class Node:

    id = 0
    dims_and_indexes = dict()
    parent1 = 0
    parent2 = 0

    frequency_set = set()
    height = 0

    next_node_ids = list()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Insert path and filename of QI, "
                                                 "path and filename of dimension tables and"
                                                 "k of k-anonymization")
    parser.add_argument("--quasi_identifiers", "-Q", required=True, type=str)
    parser.add_argument("--dimension_tables", "-D", required=True, type=str)
    parser.add_argument("--k", "-k", required=True, type=str)
    args = parser.parse_args()

    connection = sqlite3.connect(":memory:")
    #connection = sqlite3.connect("identifier.sqlite")
    cursor = connection.cursor()

    # all attributes of the table
    attributes = list()

    prepare_table_to_be_k_anonymized(cursor)

    # Q is a set containing the quasi-identifiers. eg:
    # <class 'set'>: {'age', 'occupation'}
    Q = get_quasi_identifiers()

    """
     dimension_tables is a dictionary in which a single key is a specific QI (except the first that indicates the type) and
     dimension_tables[QI] is the dimension table of QI. eg:
     <class 'dict'>: {'age': {'0': [1, 2, 3], '1': [4, 5]}, 'occupation': {'0': ['a', 'b', 'c'], '1': ['d', 'e'], '2': ['*']}}
    """
    dimension_tables = get_dimension_tables()

    # create dimension SQL tables
    create_dimension_tables(dimension_tables)

    k = int(args.k)

    # the first domain generalization hierarchies are the simple A0->A1, O0->O1->O2 and, obviously, the first candidate
    # nodes Ci (i=1) are the "0" ones, that is Ci={A0, O0}. I have to create the Nodes and Edges tables

    create_tables_Ci_Ei()

    # I must pass the priorityQueue otherwise the body of the function can't see and instantiates a PriorityQueue -.-
    basic_incognito_algorithm(queue.PriorityQueue(), Q, k)

    cursor.execute("SELECT * FROM Si")
    Sn = list(cursor)
    print("Sn: " + str(Sn))

    projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn)

    connection.close()

