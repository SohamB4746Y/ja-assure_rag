def validate_sum_assured(sum_assured: dict) -> bool:
    """
    Ensures only ONE sum insured type is present per record.
    """
    keys = [
        "maximum_stock_in_premises_label",
        "maximum_stock_foreign_currency_in_premise_label",
        "value_of_cash_in_premise_label"
    ]

    present = [k for k in keys if k in sum_assured and sum_assured[k] not in ["", None]]

    return len(present) == 1
