import sqlite3
import re

connection = sqlite3.connect(":memory:")
cursor = connection.cursor()

attributes = list()
path_to_datasets = "../datasets/"

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

print(', '.join(attributes))

with open(path_to_datasets + "adult.data", "r") as adult_data:
    table_name = "AdultData"
    cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + "(" + ','.join(attributes) + ")")
    connection.commit()

    for line in adult_data:
        values = line.rstrip(", <=50K\n").rstrip(", >50K\n").split(",")
        new_values = list()
        for value in values:
            value = value.strip()
            if value.isnumeric():
                value = float(value)
            elif value.__contains__("-"):
                value = value.replace("-", "_")
            new_values.append(value)

        if len(new_values) == 1:
            continue
        cursor.execute("INSERT INTO " + table_name + ' values ({})'.format(new_values)
                       .replace("[", "").replace("]", ""))
        connection.commit()

connection.close()
