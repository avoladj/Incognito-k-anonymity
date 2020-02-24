# data-merlo

##NOTES
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
Where "type" can either be "int" or "text", according to the QI's type.