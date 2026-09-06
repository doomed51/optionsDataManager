"""Interactive Streamlit dashboard for Parquet continuity and quality review."""

from __future__ import annotations

from time import perf_counter

import pandas as pd
import plotly.express as px
import streamlit as st

from parquet_analytics.inspection import (
    InspectionFilters,
    ParquetInspectionError,
    ParquetInspectionService,
)


st.set_page_config(page_title="Options Parquet Inspector", page_icon="🔎", layout="wide")
st.title("Options Parquet Inspector")
st.caption("Read-only continuity, coverage, ATM-price, and data-quality inspection")


@st.cache_resource
def load_service() -> ParquetInspectionService:
    return ParquetInspectionService.from_env()


@st.cache_data(show_spinner=False)
def load_selection(root: str, signature: tuple[int, int], symbol: str, interval: str):
    del signature
    return ParquetInspectionService(root).selection(symbol, interval)


@st.cache_data(show_spinner=False)
def load_date_bounds(
    root: str, signature: tuple[int, int], symbol: str, interval: str, dte: int
):
    del signature
    return ParquetInspectionService(root).date_bounds(symbol, interval, dte)


@st.cache_data(show_spinner="Querying Parquet data…")
def load_views(root: str, signature: tuple[int, int], filters: InspectionFilters):
    del signature
    service = ParquetInspectionService(root)
    return {
        "summary": service.summary(filters),
        "strikes": service.daily_strikes(filters),
        "deltas": service.delta_coverage(filters),
        "atm": service.atm_series(filters),
        "gaps": service.atm_gaps(filters),
        "exception_counts": service.exception_counts(filters),
        "exceptions": service.exceptions(filters),
    }


def right_name(value: str) -> str:
    return {"C": "Calls", "P": "Puts"}.get(value, value)


def csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def render_delta_heatmap(frame: pd.DataFrame, right: str) -> None:
    subset = frame[frame["right"] == right].copy()
    st.subheader(f"{right_name(right)} delta coverage")
    if subset.empty:
        st.info("No valid delta observations for this side.")
        return
    subset["delta_bucket"] = subset["bucket_start"].map(
        lambda value: f"{float(value):.1f}–{float(value) + 0.1:.1f}"
    )
    pivot = subset.pivot(
        index="delta_bucket", columns="trade_date", values="contract_count"
    ).fillna(0)
    ordered = [f"{value / 10:.1f}–{value / 10 + 0.1:.1f}" for value in range(10)]
    pivot = pivot.reindex(ordered, fill_value=0)
    figure = px.imshow(
        pivot, aspect="auto", color_continuous_scale="Blues",
        labels={"x": "Trading date", "y": "Absolute delta", "color": "Contracts"},
    )
    figure.update_layout(margin=dict(l=10, r=10, t=20, b=10), height=360)
    st.plotly_chart(figure, width="stretch", key=f"delta_heatmap_{right}")


try:
    service = load_service()
    signature = service.signature()
    symbols = service.symbols()
    if not symbols:
        st.warning("No published symbol partitions were found in PARQUET_ROOT.")
        st.stop()

    with st.sidebar:
        st.header("Dataset filters")
        symbol = st.selectbox("Symbol", symbols)
        intervals = service.intervals(symbol)
        if not intervals:
            st.warning("No intervals are available for this symbol.")
            st.stop()
        interval = st.selectbox("Interval", intervals)
        selection = load_selection(str(service.root), signature, symbol, interval)
        if not selection.dtes:
            st.warning("No dated rows with a DTE are available for this selection.")
            st.stop()
        dte = st.selectbox("Exact DTE", selection.dtes)
        first_date, last_date = load_date_bounds(
            str(service.root), signature, symbol, interval, int(dte)
        )
        if first_date is None or last_date is None:
            st.warning("No dated rows are available for this exact DTE.")
            st.stop()
        selected_range = st.date_input(
            "Trading-date range", value=(first_date, last_date),
            min_value=first_date, max_value=last_date,
            key=f"date_range_{symbol}_{interval}_{dte}",
        )
        if not isinstance(selected_range, (tuple, list)) or len(selected_range) != 2:
            st.info("Select both a start and end date.")
            st.stop()
        start_date, end_date = selected_range

    filters = InspectionFilters(symbol, interval, int(dte), start_date, end_date)
    loading_bar = st.progress(0, text="Preparing Parquet query…")
    query_started = perf_counter()
    loading_bar.progress(15, text="Loading filtered Parquet data…")
    views = load_views(str(service.root), signature, filters)
    elapsed_seconds = perf_counter() - query_started
    loading_bar.progress(100, text=f"Parquet query complete in {elapsed_seconds:.2f} seconds")
    st.caption(
        f"Dataset location: `{service.root}` · Query elapsed time: {elapsed_seconds:.2f} seconds"
    )
    summary = views["summary"]

    metrics = st.columns(5)
    metrics[0].metric("Days available", f"{int(summary['available_days']):,}")
    metrics[1].metric("Expected NYSE sessions", f"{summary['expected_sessions']:,}")
    metrics[2].metric("Missing sessions", f"{summary['missing_sessions']:,}")
    metrics[3].metric("Session coverage", f"{summary['coverage_percent']:.1f}%")
    metrics[4].metric("Rows", f"{int(summary['row_count']):,}")
    if summary["first_date"] is not None:
        st.caption(
            f"Observed {summary['first_date']} through {summary['last_date']} · "
            f"Exact DTE {dte} · Dates grouped in America/New_York"
        )

    strike_tab, delta_tab, atm_tab, quality_tab = st.tabs(
        ["Strike coverage", "Delta coverage", "ATM prices & gaps", "Quality exceptions"]
    )

    with strike_tab:
        strikes = views["strikes"].copy()
        if strikes.empty:
            st.info("No strikes match the selected filters.")
        else:
            strikes["side"] = strikes["right"].map(right_name)
            figure = px.line(
                strikes, x="trade_date", y="strike_count", color="side", markers=True,
                labels={"trade_date": "Trading date", "strike_count": "Distinct strikes", "side": "Side"},
            )
            figure.update_layout(hovermode="x unified", height=480)
            st.plotly_chart(figure, width="stretch", key="strike_coverage")
        missing = pd.DataFrame({"missing_session": summary["missing_dates"]})
        if missing.empty:
            st.success("No NYSE sessions are missing in the selected date range.")
        else:
            st.warning(f"{len(missing)} expected NYSE session(s) have no matching rows.")
            st.dataframe(missing, width="stretch", hide_index=True)
            st.download_button(
                "Download missing sessions CSV", csv_bytes(missing),
                file_name=f"{symbol}_{interval}_{dte}dte_missing_sessions.csv", mime="text/csv",
            )

    with delta_tab:
        left, right = st.columns(2)
        with left:
            render_delta_heatmap(views["deltas"], "C")
        with right:
            render_delta_heatmap(views["deltas"], "P")
        st.caption(
            "Cells count unique expiry/strike contracts after taking the daily median "
            "absolute delta. Invalid deltas appear in Quality exceptions."
        )

    with atm_tab:
        atm = views["atm"].copy()
        if atm.empty:
            st.info("No rolling ATM candidates match the selected filters.")
        else:
            atm["side"] = atm["right"].map(right_name)
            figure = px.line(
                atm, x="timestamp", y="mid_price", color="side",
                custom_data=["expiry", "strike", "underlying_price", "bid", "ask", "dte"],
                labels={"timestamp": "UTC timestamp", "mid_price": "ATM midpoint", "side": "Side"},
            )
            figure.update_traces(hovertemplate=(
                "%{x}<br>Mid: %{y}<br>Expiry: %{customdata[0]}<br>"
                "Strike: %{customdata[1]}<br>Underlying: %{customdata[2]}<br>"
                "Bid / ask: %{customdata[3]} / %{customdata[4]}<br>DTE: %{customdata[5]}"
                "<extra>%{fullData.name}</extra>"
            ))
            figure.update_layout(hovermode="x unified", height=520)
            st.plotly_chart(figure, width="stretch", key="atm_midprices")
        gaps = views["gaps"]
        if gaps.empty:
            st.success("No interior ATM midpoint gaps exceed 1.5× the selected interval.")
        else:
            st.warning(f"Found {len(gaps)} interior ATM midpoint gap(s).")
            st.dataframe(gaps, width="stretch", hide_index=True)
            st.download_button(
                "Download ATM gaps CSV", csv_bytes(gaps),
                file_name=f"{symbol}_{interval}_{dte}dte_atm_gaps.csv", mime="text/csv",
            )

    with quality_tab:
        counts = views["exception_counts"]
        exceptions = views["exceptions"]
        if counts.empty:
            st.success("No configured row-level quality exceptions were found.")
        else:
            st.dataframe(counts, width="stretch", hide_index=True)
            st.caption("Filter and sort the detailed records without changing the overall counts above.")
            rule_options = sorted(exceptions["rule"].dropna().unique().tolist())
            right_options = sorted(exceptions["right"].dropna().unique().tolist())
            expiry_options = sorted(exceptions["expiry"].dropna().unique().tolist())
            filters_row = st.columns(4)
            selected_rules = filters_row[0].multiselect("Rule", rule_options)
            selected_rights = filters_row[1].multiselect("Right", right_options)
            selected_expiries = filters_row[2].multiselect("Expiry", expiry_options)
            sort_columns = [
                column for column in (
                    "timestamp", "rule", "right", "expiry", "strike", "dte",
                    "underlying_price", "bid", "ask", "delta",
                ) if column in exceptions.columns
            ]
            sort_by = filters_row[3].selectbox("Sort by", sort_columns, index=0)

            filtered_exceptions = exceptions.copy()
            if selected_rules:
                filtered_exceptions = filtered_exceptions[
                    filtered_exceptions["rule"].isin(selected_rules)
                ]
            if selected_rights:
                filtered_exceptions = filtered_exceptions[
                    filtered_exceptions["right"].isin(selected_rights)
                ]
            if selected_expiries:
                filtered_exceptions = filtered_exceptions[
                    filtered_exceptions["expiry"].isin(selected_expiries)
                ]

            valid_strikes = exceptions["strike"].dropna()
            if not valid_strikes.empty:
                strike_min = float(valid_strikes.min())
                strike_max = float(valid_strikes.max())
                selected_strikes = st.slider(
                    "Strike range", strike_min, strike_max, (strike_min, strike_max)
                )
                filtered_exceptions = filtered_exceptions[
                    filtered_exceptions["strike"].between(*selected_strikes)
                ]

            descending = st.checkbox("Sort descending", value=False)
            filtered_exceptions = filtered_exceptions.sort_values(
                sort_by, ascending=not descending, na_position="last"
            )
            st.dataframe(
                filtered_exceptions, width="stretch", hide_index=True, height=480
            )
            st.download_button(
                "Download filtered row exceptions CSV", csv_bytes(filtered_exceptions),
                file_name=f"{symbol}_{interval}_{dte}dte_quality_exceptions.csv", mime="text/csv",
            )

except (ParquetInspectionError, ValueError) as exc:
    st.error(str(exc))
    st.stop()
