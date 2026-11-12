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


def test_thresholds_appear_in_rule_conditions():
    conditions = " ".join(rule.condition for rule in rules.REJECT_RULES)
    assert str(rules.MAX_TRIP_MILES) in conditions
    assert str(rules.MAX_FARE_DOLLARS) in conditions
    assert str(rules.MAX_TRIP_HOURS * 3600) in conditions
