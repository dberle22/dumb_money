import importlib

cli_main_module = importlib.import_module("dumb_money.cli.main")


def test_stage_all_command_dispatches_transform_builds(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        cli_main_module,
        "stage_prices",
        lambda **kwargs: calls.append(("prices", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_fundamentals",
        lambda **kwargs: calls.append(("fundamentals", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_benchmark_sets",
        lambda **kwargs: calls.append(("benchmark_sets", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_security_master",
        lambda **kwargs: calls.append(("security_master", kwargs)),
    )

    exit_code = cli_main_module.main(
        [
            "stage",
            "all",
            "--price-paths",
            "raw_prices.csv",
            "--fundamental-paths",
            "raw_fundamentals.csv",
            "--benchmark-definition-paths",
            "benchmark_defs.csv",
            "--set-id",
            "core_us",
        ]
    )

    assert exit_code == 0
    assert calls == [
        ("prices", {"input_paths": ["raw_prices.csv"]}),
        ("fundamentals", {"input_paths": ["raw_fundamentals.csv"]}),
        ("benchmark_sets", {"input_paths": ["benchmark_defs.csv"], "set_id": "core_us"}),
        (
            "security_master",
            {
                "fundamentals_paths": ["raw_fundamentals.csv"],
                "benchmark_definition_paths": ["benchmark_defs.csv"],
            },
        ),
    ]
