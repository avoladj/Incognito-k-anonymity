# Incognito
This is a rather straightforward implementation of the Incognito algorithm for
achieving k-anonymity published by Kristen LeFevre, David J. DeWitt and 
Raghu Ramakrishnan.

## How to use
Run the following command

    python main.py -d path/to/dataset.csv -D path/to/dimension_tables.json -k k_value -t t_value

Where: 
- path/to/dataset.csv is the path to the CSV file containing the dataset you wish
to make k-anonymous
- path/to/dimension_tables.json is the path to the JSON file containing the dimension
tables, i.e. the tables containing the values of each quasi-identifier and their
increasing generalizations (see Notes for more details)
- k_value is the desired value for k-anonymity
- t_value is the desired value for the outlier threshold (rows with a certain
combination of quasi-identifiers will be deleted if they are present in an amount
lower than t_value). To disable it set it to 0.

This script outputs a file named anonymous_table.csv containing the resulting
k-anonymous table.


## Notes
### Dataset
The dataset is supposed to be a CSV file whose first row contains the table's
attributes, and the following ones the table's tuples with the values in the same
order as the attributes' names specified in the firs row

### Dimension tables
The dimension tables file must be provided in JSON, and have the following shape:

    {
        "QI_name_1": {
            "0": [..., ..., ...],
            "1": [..., ..., ...],
            ...
        },
        ...
        "QI_name_n": {
            "0": [..., ..., ...],
            "1": [..., ..., ...],
            ...
        }
    }
    
Where "0" indicates the non-anonymous values and "1", "2", etc. their increasingly
deeper anonymization levels. Each level must have the same number of values, and the
single elements in each level must correspond (i.e. the i-th element in "0" will be
generalized into the i-th element in "1", then into the i-th element in "2", etc.)
Multiple-word QI names and values must be provided using underscores ( _ ) instead
of hyphens ( - ), even if the input database uses hyphens.