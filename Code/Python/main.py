import sqlite3
import re
import argparse
import json


def prepare_table_to_be_k_anonymized():
    cursor = connection.cursor()

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


parser = argparse.ArgumentParser(description="Insert path and filename of QI and "
                                             "path and filename of dimension tables")
parser.add_argument("--quasi_identifiers", "-Q", required=True, type=str)
parser.add_argument("--dimension_tables", "-D", required=True, type=str)
args = parser.parse_args()

connection = sqlite3.connect(":memory:")

prepare_table_to_be_k_anonymized()

# Q is a set containing the quasi-identifiers. eg:
# <class 'set'>: {'age', 'occupation'}
Q = get_quasi_identifiers()

"""
 dimension_tables is a dictionary in which a single key is a specific QI and
 dimension_tables[QI] is the dimension table of QI. eg:
 <class 'dict'>: {'age': {'A0': [1, 2, 3], 'A1': [4, 5]}, 'occupation': {'O0': ['a', 'b', 'c'], 'O1': ['d', 'e'], 'O2': ['*']}}
"""
dimension_tables = get_dimension_tables()

connection.close()
