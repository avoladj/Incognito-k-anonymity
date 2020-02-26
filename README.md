# data-merlo

## Notes
The QI file must be in CSV and have the following structure:

    QI-name1, QI-name2, ...


The dimension tables file must be provided in JSON, and have the following shape:

    {
        "QI-name1": {
            "type": "...",
            "0": [..., ..., ...],
            "1": [..., ..., ...],
            ...
        }
        "QI-name2": {
            "type": "...",
            "0": [..., ..., ...],
            "1": [..., ..., ...],
            ...
        }
        ...
    }
Where "type" can either be "real" or "text", according to the QI's type.

## TODO
- Make the script "table-agnostic" (i.e. remove AdultData & co. from the code)
- Make sure that all entries in the table are treated as strings (thus removing the need to specify the type)