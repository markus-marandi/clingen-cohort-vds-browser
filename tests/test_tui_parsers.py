from tui.parsers import parse_line


def test_bracket_progress() -> None:
    update = parse_line("[  1,234/5,000] ok sample.gvcf.gz")
    assert update is not None
    assert update.kind == "progress"
    assert update.current == 1234
    assert update.total == 5000


def test_completed_progress() -> None:
    update = parse_line("Completed:        1234/5000 (24.7%)")
    assert update is not None
    assert update.current == 1234
    assert update.total == 5000


def test_indexed_progress() -> None:
    update = parse_line("  indexed 5,000/150,000 (250 docs/s)")
    assert update is not None
    assert update.current == 5000
    assert update.total == 150000


def test_stage_success_and_error() -> None:
    assert parse_line("STARTING: VDS Combination").label == "VDS Combination"
    assert parse_line("PIPELINE COMPLETED SUCCESSFULLY").kind == "success"
    assert parse_line("Traceback (most recent call last):").kind == "error"
