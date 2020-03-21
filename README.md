# data-merlo

## Notes

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
Multiple-word QI names and values must be provided using underscores ( _ ) instead
of hyphens ( - ), even if the input database uses hyphens.

## TODO
- Improve how the script gets input table (no XYZ.data file, XYZ.csv would be better)
- Improve how the script gets attributes names (no XYZ.names file)
- Insert written feedback
- Check if DB improvements actually work / are useless
- Improve dimension table values