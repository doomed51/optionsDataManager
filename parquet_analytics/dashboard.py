"""Presentation helpers shared by the read-only Parquet dashboard."""

from __future__ import annotations


ZERO_CONTRACT_COLOR = "#f97316"
MINIMUM_CONTRACT_COLOR = "#ffffff"
MAXIMUM_CONTRACT_COLOR = "#08306b"


def delta_heatmap_colorscale(max_contract_count: int | float) -> list[tuple[float, str]]:
    """Return a scale with an exact visual boundary between zero and one.

    Plotly color-scale positions are normalized to the configured z range. By
    fixing that range to start at zero, the first non-zero count occurs at
    `1 / max_contract_count` and can begin the white-to-blue gradient.
    """
    maximum = max(1.0, float(max_contract_count))
    first_contract_position = 1.0 / maximum
    if maximum == 1.0:
        return [
            (0.0, ZERO_CONTRACT_COLOR),
            (1.0, ZERO_CONTRACT_COLOR),
            (1.0, MINIMUM_CONTRACT_COLOR),
        ]
    return [
        (0.0, ZERO_CONTRACT_COLOR),
        (first_contract_position, ZERO_CONTRACT_COLOR),
        (first_contract_position, MINIMUM_CONTRACT_COLOR),
        (1.0, MAXIMUM_CONTRACT_COLOR),
    ]
