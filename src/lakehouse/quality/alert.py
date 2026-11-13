"""The alert text for a batch that failed the quality gate."""

from lakehouse.quality.gate import GateResult


def format_alert(result: GateResult, quarantine_table: str) -> str:
    summary = (
        f"QUALITY ALERT: batch {result.source_month} failed {len(result.failed_checks)} "
        f"of {len(result.checks)} checks ({result.row_count:,} rows)."
    )
    lines = [summary]
    lines += [f"  FAILED {check.name}: {check.detail}" for check in result.failed_checks]
    lines.append(
        f"The batch was not published. Its rows are held in {quarantine_table} for review."
    )
    return "\n".join(lines)
