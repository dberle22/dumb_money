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
        "stage_benchmark_definition_refresh",
        lambda **kwargs: calls.append(("benchmark_definitions", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_benchmark_memberships",
        lambda **kwargs: calls.append(("benchmark_memberships", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_benchmark_mappings",
        lambda **kwargs: calls.append(("benchmark_mappings", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_benchmark_membership_coverage",
        lambda **kwargs: calls.append(("benchmark_membership_coverage", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_benchmark_sets",
        lambda **kwargs: calls.append(("benchmark_sets", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_peer_sets",
        lambda **kwargs: calls.append(("peer_sets", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_listed_security_seed",
        lambda **kwargs: calls.append(("listed_security_seed", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_security_master_overrides",
        lambda **kwargs: calls.append(("security_master_overrides", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_security_master",
        lambda **kwargs: calls.append(("security_master", kwargs)),
    )
    monkeypatch.setattr(
        cli_main_module,
        "stage_security_ingestion_status",
        lambda **kwargs: calls.append(("security_ingestion_status", kwargs)),
    )

    exit_code = cli_main_module.main(
        [
            "stage",
            "all",
            "--price-paths",
            "raw_prices.csv",
            "--fundamental-paths",
            "raw_fundamentals.csv",
            "--nasdaq-listed-paths",
            "nasdaqlisted.txt",
            "--other-listed-paths",
            "otherlisted.txt",
            "--benchmark-definition-paths",
            "benchmark_defs.csv",
            "--benchmark-mapping-path",
            "benchmark_mapping.csv",
            "--custom-benchmark-membership-path",
            "custom_benchmark_memberships.csv",
            "--override-paths",
            "security_master_overrides.csv",
            "--set-id",
            "core_us",
        ]
    )

    assert exit_code == 0
    assert calls == [
        ("prices", {"input_paths": ["raw_prices.csv"]}),
        ("fundamentals", {"input_paths": ["raw_fundamentals.csv"]}),
        (
            "listed_security_seed",
            {"nasdaq_listed_paths": ["nasdaqlisted.txt"], "other_listed_paths": ["otherlisted.txt"]},
        ),
        (
            "security_master_overrides",
            {"input_paths": ["security_master_overrides.csv"]},
        ),
        (
            "benchmark_definitions",
            {
                "mapping_path": "benchmark_mapping.csv",
                "custom_membership_path": "custom_benchmark_memberships.csv",
            },
        ),
        (
            "benchmark_memberships",
            {
                "mapping_path": "benchmark_mapping.csv",
                "custom_membership_path": "custom_benchmark_memberships.csv",
            },
        ),
        ("benchmark_sets", {"input_paths": ["benchmark_defs.csv"], "set_id": "core_us"}),
        ("peer_sets", {}),
        (
            "security_master",
            {
                "listed_security_paths": None,
                "fundamentals_paths": ["raw_fundamentals.csv"],
                "benchmark_definition_paths": ["benchmark_defs.csv"],
                "override_paths": ["security_master_overrides.csv"],
            },
        ),
        ("benchmark_mappings", {"mapping_path": "benchmark_mapping.csv"}),
        ("security_ingestion_status", {}),
        (
            "benchmark_membership_coverage",
            {
                "mapping_path": "benchmark_mapping.csv",
                "custom_membership_path": "custom_benchmark_memberships.csv",
            },
        ),
    ]
