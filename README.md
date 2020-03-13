# data-merlo

## Notes

The dimension tables file must be provided in JSON, and have the following shape:

    {
        "QI-name1": {
            "0": [..., ..., ...],
            "1": [..., ..., ...],
            ...
        }
        "QI-name2": {
            "0": [..., ..., ...],
            "1": [..., ..., ...],
            ...
        }
        ...
    }

## TODO
- Make the script "table-agnostic" (i.e. remove AdultData & co. from the code)