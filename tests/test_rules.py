from lakehouse.silver import rules


def test_reject_reasons_are_unique():
    reasons = [rule.reason for rule in rules.REJECT_RULES]
    assert len(reasons) == len(set(reasons))


def test_dedup_key_uses_silver_column_names():
    silver_names = {silver for silver, _ in rules.COLUMNS.values()}
    assert set(rules.DEDUP_KEY) <= silver_names


def test_every_silver_column_is_named_once():
    silver_names = [silver for silver, _ in rules.COLUMNS.values()]
    assert len(silver_names) == len(set(silver_names))


def test_columns_with_a_range_rule_also_have_a_null_rule():
    # In Spark SQL a comparison with null is not true, so null values need their own check.
    conditions = {rule.reason: rule.condition for rule in rules.REJECT_RULES}
    assert "IS NULL" in conditions["invalid_amount"]
    assert "IS NULL" in conditions["non_positive_distance"]
    assert "IS NULL" in conditions["missing_timestamp"]
