import os
from typing import Dict, Optional

import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable


# -----------------------------------------------------------------------------
# App Configuration
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="AWS Glue Delta Lake Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

alt.data_transformers.disable_max_rows()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def get_secret(key: str, default=None):
    """Safely read a top-level Streamlit secret."""
    try:
        return st.secrets[key]
    except Exception:
        return default


def get_nested_secret(section: str, key: str, default=None):
    """Safely read a nested Streamlit secret."""
    try:
        return st.secrets[section][key]
    except Exception:
        return default


def get_aws_storage_options() -> Dict[str, str]:
    """
    Storage options for delta-rs when reading private S3 Delta tables.

    These values should be configured in Streamlit Cloud secrets.
    """
    storage_options = {}

    aws_access_key_id = get_nested_secret("aws", "AWS_ACCESS_KEY_ID")
    aws_secret_access_key = get_nested_secret("aws", "AWS_SECRET_ACCESS_KEY")
    aws_session_token = get_nested_secret("aws", "AWS_SESSION_TOKEN")
    aws_region = get_nested_secret("aws", "AWS_REGION", "us-east-1")

    if aws_access_key_id:
        storage_options["AWS_ACCESS_KEY_ID"] = aws_access_key_id

    if aws_secret_access_key:
        storage_options["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    if aws_session_token:
        storage_options["AWS_SESSION_TOKEN"] = aws_session_token

    if aws_region:
        storage_options["AWS_REGION"] = aws_region

    return storage_options


def get_table_path(table_name: str) -> Optional[str]:
    """
    Resolve a curated table path.

    Preferred:
      [paths]
      customer_ltv = "s3://bucket/curated/customer_ltv/"

    Fallback:
      [app]
      bucket_name = "my-bucket"
    """
    explicit_path = get_nested_secret("paths", table_name)
    if explicit_path:
        return explicit_path

    bucket_name = get_nested_secret("app", "bucket_name")
    if bucket_name:
        return f"s3://{bucket_name}/curated/{table_name}/"

    return None


@st.cache_data(ttl=3600, show_spinner=False)
def read_curated_delta_table(table_name: str) -> pd.DataFrame:
    """
    Read a curated Delta table from S3 using delta-rs.

    Returns an empty DataFrame instead of crashing if configuration is missing.
    """
    table_path = get_table_path(table_name)

    if not table_path:
        return pd.DataFrame()

    storage_options = get_aws_storage_options()

    try:
        delta_table = DeltaTable(table_path, storage_options=storage_options)
        return delta_table.to_pandas()
    except Exception as exc:
        st.error(f"Unable to read `{table_name}` from `{table_path}`.")
        st.exception(exc)
        return pd.DataFrame()


def format_currency(value) -> str:
    if pd.isna(value):
        return "$0.00"
    return f"${value:,.2f}"


def format_number(value) -> str:
    if pd.isna(value):
        return "0"
    return f"{value:,.0f}"


def format_percent(value) -> str:
    if pd.isna(value):
        return "0.0%"
    return f"{value:.1f}%"


def require_columns(df: pd.DataFrame, required_columns, table_name: str) -> bool:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        st.warning(f"`{table_name}` is missing required columns: {missing}")
        return False
    return True


def empty_state(table_name: str):
    st.info(
        f"No data loaded for `{table_name}` yet. Confirm your Streamlit secrets, "
        "AWS credentials, and curated Delta table S3 path."
    )


# -----------------------------------------------------------------------------
# Load Data
# -----------------------------------------------------------------------------

customer_ltv_df = read_curated_delta_table("customer_ltv")
customer_segmentation_df = read_curated_delta_table("customer_segmentation")
churn_risk_df = read_curated_delta_table("churn_risk")
location_performance_df = read_curated_delta_table("location_performance")
loyalty_impact_df = read_curated_delta_table("loyalty_impact")
order_timing_analysis_df = read_curated_delta_table("order_timing_analysis")


# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------

st.sidebar.title("Dashboard Controls")

if st.sidebar.button("Refresh cached data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption(
    "Data source: curated Delta tables in S3. "
    "Configure paths and AWS credentials in Streamlit Cloud secrets."
)


# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------

st.title("AWS Glue + Delta Lake Restaurant Analytics")
st.caption(
    "Streamlit dashboard for curated Delta Lake tables built with AWS Glue, "
    "S3, and a medallion-style data lake."
)


# -----------------------------------------------------------------------------
# Tabs
# -----------------------------------------------------------------------------

tabs = st.tabs(
    [
        "Executive Overview",
        "Customer LTV",
        "Customer Segmentation",
        "Churn Risk",
        "Location Performance",
        "Loyalty Impact",
        "Order Timing Analysis",
    ]
)


# -----------------------------------------------------------------------------
# Executive Overview
# -----------------------------------------------------------------------------

with tabs[0]:
    st.header("Executive Overview")

    metric_cols = st.columns(4)

    total_customers = (
        customer_ltv_df["user_id"].nunique()
        if not customer_ltv_df.empty and "user_id" in customer_ltv_df.columns
        else 0
    )

    total_ltv = (
        customer_ltv_df["ltv"].sum()
        if not customer_ltv_df.empty and "ltv" in customer_ltv_df.columns
        else 0
    )

    at_risk_count = (
        churn_risk_df[churn_risk_df["at_risk"] == True]["user_id"].nunique()
        if not churn_risk_df.empty and {"at_risk", "user_id"}.issubset(churn_risk_df.columns)
        else 0
    )

    total_locations = (
        location_performance_df["restaurant_id"].nunique()
        if not location_performance_df.empty and "restaurant_id" in location_performance_df.columns
        else 0
    )

    metric_cols[0].metric("Customers", format_number(total_customers))
    metric_cols[1].metric("Total Customer LTV", format_currency(total_ltv))
    metric_cols[2].metric("At-Risk Customers", format_number(at_risk_count))
    metric_cols[3].metric("Restaurant Locations", format_number(total_locations))

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Customer Segment Mix")
        if not customer_segmentation_df.empty and require_columns(
            customer_segmentation_df,
            ["segment", "user_id"],
            "customer_segmentation",
        ):
            segment_counts = (
                customer_segmentation_df
                .groupby("segment", as_index=False)["user_id"]
                .nunique()
                .rename(columns={"user_id": "customer_count"})
            )

            chart = (
                alt.Chart(segment_counts)
                .mark_bar()
                .encode(
                    x=alt.X("segment:N", title="Segment", sort="-y"),
                    y=alt.Y("customer_count:Q", title="Customers"),
                    tooltip=["segment", "customer_count"],
                )
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            empty_state("customer_segmentation")

    with right_col:
        st.subheader("Top Locations by Revenue")
        if not location_performance_df.empty and require_columns(
            location_performance_df,
            ["restaurant_id", "total_revenue"],
            "location_performance",
        ):
            top_locations = (
                location_performance_df
                .copy()
                .sort_values("total_revenue", ascending=False)
                .head(10)
            )

            top_locations["total_revenue"] = (
                top_locations["total_revenue"]
                .astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .astype(float)
            )

            chart = (
                alt.Chart(top_locations)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "total_revenue:Q",
                        title="Total Revenue",
                        axis=alt.Axis(format="$,.2f")
                    ),
                    y=alt.Y(
                        "restaurant_id:N",
                        title="Restaurant",
                        sort=top_locations["restaurant_id"].tolist()
                    ),
                    tooltip=[
                        alt.Tooltip("restaurant_id:N", title="Restaurant"),
                        alt.Tooltip(
                            "total_revenue:Q",
                            title="Total Revenue",
                            format="$,.2f"
                        ),
                    ],
                )
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            empty_state("location_performance")


# -----------------------------------------------------------------------------
# Customer LTV
# -----------------------------------------------------------------------------

with tabs[1]:
    st.header("Customer LTV")

    if customer_ltv_df.empty:
        empty_state("customer_ltv")
    elif require_columns(
        customer_ltv_df,
        ["user_id", "ltv", "ltv_category", "num_orders", "avg_order_value"],
        "customer_ltv",
    ):
        categories = sorted(customer_ltv_df["ltv_category"].dropna().unique().tolist())
        selected_categories = st.multiselect(
            "Filter by LTV category",
            categories,
            default=categories,
        )

        filtered_ltv = customer_ltv_df[
            customer_ltv_df["ltv_category"].isin(selected_categories)
        ].copy()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Customers", format_number(filtered_ltv["user_id"].nunique()))
        col2.metric("Total LTV", format_currency(filtered_ltv["ltv"].sum()))
        col3.metric("Average LTV", format_currency(filtered_ltv["ltv"].mean()))
        col4.metric("Average Order Value", format_currency(filtered_ltv["avg_order_value"].mean()))

        st.subheader("LTV Category Distribution")

        category_summary = (
            filtered_ltv
            .groupby("ltv_category", as_index=False)
            .agg(
                customer_count=("user_id", "nunique"),
                avg_ltv=("ltv", "mean"),
                avg_order_value=("avg_order_value", "mean"),
            )
        )

        chart = (
            alt.Chart(category_summary)
            .mark_bar()
            .encode(
                x=alt.X("ltv_category:N", title="LTV Category", sort=["High", "Medium", "Low"]),
                y=alt.Y("customer_count:Q", title="Customers"),
                tooltip=[
                    "ltv_category",
                    "customer_count",
                    alt.Tooltip("avg_ltv:Q", format=",.2f"),
                    alt.Tooltip("avg_order_value:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Top Customers by LTV")
        top_customers = (
            filtered_ltv
            .sort_values("ltv", ascending=False)
            .head(25)
        )
        st.dataframe(top_customers, use_container_width=True, hide_index=True)


# -----------------------------------------------------------------------------
# Customer Segmentation
# -----------------------------------------------------------------------------

with tabs[2]:
    st.header("Customer Segmentation")

    if customer_segmentation_df.empty:
        empty_state("customer_segmentation")
    elif require_columns(
        customer_segmentation_df,
        [
            "user_id",
            "days_since_last_order",
            "num_orders_last_3_months",
            "total_spend_last_3_months",
            "segment",
        ],
        "customer_segmentation",
    ):
        segment_options = sorted(customer_segmentation_df["segment"].dropna().unique().tolist())
        selected_segments = st.multiselect(
            "Filter by segment",
            segment_options,
            default=segment_options,
        )

        filtered_segments = customer_segmentation_df[
            customer_segmentation_df["segment"].isin(selected_segments)
        ].copy()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Customers", format_number(filtered_segments["user_id"].nunique()))
        col2.metric(
            "Avg Days Since Last Order",
            f"{filtered_segments['days_since_last_order'].mean():,.1f}",
        )
        col3.metric(
            "Avg Orders Last 3 Months",
            f"{filtered_segments['num_orders_last_3_months'].mean():,.1f}",
        )
        col4.metric(
            "Avg Spend Last 3 Months",
            format_currency(filtered_segments["total_spend_last_3_months"].mean()),
        )

        st.subheader("Customers by Segment")

        segment_summary = (
            filtered_segments
            .groupby("segment", as_index=False)
            .agg(
                customer_count=("user_id", "nunique"),
                avg_days_since_last_order=("days_since_last_order", "mean"),
                avg_orders_last_3_months=("num_orders_last_3_months", "mean"),
                avg_spend_last_3_months=("total_spend_last_3_months", "mean"),
            )
        )

        chart = (
            alt.Chart(segment_summary)
            .mark_bar()
            .encode(
                x=alt.X("segment:N", title="Segment", sort="-y"),
                y=alt.Y("customer_count:Q", title="Customers"),
                tooltip=[
                    "segment",
                    "customer_count",
                    alt.Tooltip("avg_days_since_last_order:Q", format=",.1f"),
                    alt.Tooltip("avg_orders_last_3_months:Q", format=",.1f"),
                    alt.Tooltip("avg_spend_last_3_months:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Frequency vs Spend by Segment")

        scatter = (
            alt.Chart(filtered_segments)
            .mark_circle(size=70, opacity=0.65)
            .encode(
                x=alt.X("num_orders_last_3_months:Q", title="Orders Last 3 Months"),
                y=alt.Y("total_spend_last_3_months:Q", title="Spend Last 3 Months"),
                color=alt.Color("segment:N", title="Segment"),
                tooltip=[
                    "user_id",
                    "segment",
                    "days_since_last_order",
                    "num_orders_last_3_months",
                    alt.Tooltip("total_spend_last_3_months:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(scatter, use_container_width=True)

        st.dataframe(filtered_segments, use_container_width=True, hide_index=True)


# -----------------------------------------------------------------------------
# Churn Risk
# -----------------------------------------------------------------------------

with tabs[3]:
    st.header("Churn Risk")

    if churn_risk_df.empty:
        empty_state("churn_risk")
    elif require_columns(
        churn_risk_df,
        [
            "user_id",
            "days_since_last_order",
            "avg_days_between_orders",
            "percent_change_in_spend",
            "at_risk",
        ],
        "churn_risk",
    ):
        at_risk_only = st.toggle("Show at-risk customers only", value=False)

        filtered_churn = churn_risk_df.copy()
        if at_risk_only:
            filtered_churn = filtered_churn[filtered_churn["at_risk"] == True]

        total_customers = churn_risk_df["user_id"].nunique()
        at_risk_customers = churn_risk_df[churn_risk_df["at_risk"] == True]["user_id"].nunique()
        at_risk_rate = (at_risk_customers / total_customers * 100) if total_customers else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Customers", format_number(total_customers))
        col2.metric("At-Risk Customers", format_number(at_risk_customers))
        col3.metric("At-Risk Rate", format_percent(at_risk_rate))
        col4.metric(
            "Avg Days Since Last Order",
            f"{churn_risk_df['days_since_last_order'].mean():,.1f}",
        )

        st.subheader("At-Risk Breakdown")

        risk_summary = (
            churn_risk_df
            .groupby("at_risk", as_index=False)
            .agg(customer_count=("user_id", "nunique"))
        )
        risk_summary["risk_status"] = risk_summary["at_risk"].map({True: "At Risk", False: "Not At Risk"})

        chart = (
            alt.Chart(risk_summary)
            .mark_bar()
            .encode(
                x=alt.X("risk_status:N", title="Risk Status"),
                y=alt.Y("customer_count:Q", title="Customers"),
                tooltip=["risk_status", "customer_count"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Days Since Last Order Distribution")

        histogram = (
            alt.Chart(filtered_churn)
            .mark_bar()
            .encode(
                x=alt.X("days_since_last_order:Q", bin=True, title="Days Since Last Order"),
                y=alt.Y("count():Q", title="Customers"),
                tooltip=[alt.Tooltip("count():Q", title="Customers")],
            )
        )
        st.altair_chart(histogram, use_container_width=True)

        st.dataframe(
            filtered_churn.sort_values("days_since_last_order", ascending=False),
            use_container_width=True,
            hide_index=True,
        )


# -----------------------------------------------------------------------------
# Location Performance
# -----------------------------------------------------------------------------

with tabs[4]:
    st.header("Location Performance")

    if location_performance_df.empty:
        empty_state("location_performance")
    elif require_columns(
        location_performance_df,
        [
            "restaurant_id",
            "total_revenue",
            "avg_order_value",
            "avg_orders_per_day",
            "avg_orders_per_week",
            "revenue_rank",
        ],
        "location_performance",
    ):
        max_rank = int(location_performance_df["revenue_rank"].max())
        selected_rank_range = st.slider(
            "Revenue rank range",
            min_value=1,
            max_value=max_rank,
            value=(1, min(20, max_rank)),
        )

        filtered_locations = location_performance_df[
            (location_performance_df["revenue_rank"] >= selected_rank_range[0])
            & (location_performance_df["revenue_rank"] <= selected_rank_range[1])
        ].copy()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Locations", format_number(location_performance_df["restaurant_id"].nunique()))
        col2.metric("Total Revenue", format_currency(location_performance_df["total_revenue"].sum()))
        col3.metric("Avg Order Value", format_currency(location_performance_df["avg_order_value"].mean()))
        col4.metric("Avg Orders / Week", f"{location_performance_df['avg_orders_per_week'].mean():,.2f}")

        st.subheader("Revenue by Location")

        chart = (
            alt.Chart(filtered_locations.sort_values("revenue_rank"))
            .mark_bar()
            .encode(
                x=alt.X("total_revenue:Q", title="Total Revenue"),
                y=alt.Y("restaurant_id:N", title="Restaurant", sort="-x"),
                tooltip=[
                    "restaurant_id",
                    "revenue_rank",
                    alt.Tooltip("total_revenue:Q", format=",.2f"),
                    alt.Tooltip("avg_order_value:Q", format=",.2f"),
                    alt.Tooltip("avg_orders_per_week:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Revenue Rank Table")
        st.dataframe(
            filtered_locations.sort_values("revenue_rank"),
            use_container_width=True,
            hide_index=True,
        )


# -----------------------------------------------------------------------------
# Loyalty Impact
# -----------------------------------------------------------------------------

with tabs[5]:
    st.header("Loyalty Impact")

    if loyalty_impact_df.empty:
        empty_state("loyalty_impact")
    elif require_columns(
        loyalty_impact_df,
        [
            "is_loyalty",
            "customer_count",
            "total_orders",
            "total_revenue",
            "avg_ltv",
            "avg_order_value",
            "avg_orders_per_customer",
            "revenue_per_customer",
        ],
        "loyalty_impact",
    ):
        display_df = loyalty_impact_df.copy()
        display_df["loyalty_status"] = display_df["is_loyalty"].map(
            {True: "Loyalty", False: "Non-Loyalty"}
        )

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Revenue by Loyalty Status")
            chart = (
                alt.Chart(display_df)
                .mark_bar()
                .encode(
                    x=alt.X("loyalty_status:N", title="Loyalty Status"),
                    y=alt.Y("total_revenue:Q", title="Total Revenue"),
                    tooltip=[
                        "loyalty_status",
                        alt.Tooltip("total_revenue:Q", format=",.2f"),
                        "customer_count",
                        "total_orders",
                    ],
                )
            )
            st.altair_chart(chart, use_container_width=True)

        with col2:
            st.subheader("Revenue per Customer")
            chart = (
                alt.Chart(display_df)
                .mark_bar()
                .encode(
                    x=alt.X("loyalty_status:N", title="Loyalty Status"),
                    y=alt.Y("revenue_per_customer:Q", title="Revenue per Customer"),
                    tooltip=[
                        "loyalty_status",
                        alt.Tooltip("revenue_per_customer:Q", format=",.2f"),
                        alt.Tooltip("avg_ltv:Q", format=",.2f"),
                        alt.Tooltip("avg_order_value:Q", format=",.2f"),
                    ],
                )
            )
            st.altair_chart(chart, use_container_width=True)

        st.subheader("Loyalty Impact Metrics")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.caption(
            "Note: This comparison shows correlation between loyalty status and order behavior. "
            "It does not prove that the loyalty program caused the difference."
        )


# -----------------------------------------------------------------------------
# Order Timing Analysis
# -----------------------------------------------------------------------------

with tabs[6]:
    st.header("Order Timing Analysis")

    if order_timing_analysis_df.empty:
        empty_state("order_timing_analysis")
    elif require_columns(
        order_timing_analysis_df,
        [
            "order_date",
            "order_year",
            "order_month",
            "day_of_week",
            "month",
            "is_weekend",
            "is_holiday",
            "holiday_name",
            "daypart",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "unique_customers",
        ],
        "order_timing_analysis",
    ):
        timing_df = order_timing_analysis_df.copy()
        timing_df["order_date"] = pd.to_datetime(timing_df["order_date"])

        daypart_order = ["Morning", "Lunch", "Afternoon", "Dinner", "Late Night"]

        available_dayparts = [
            value for value in daypart_order
            if value in timing_df["daypart"].dropna().unique().tolist()
        ]

        selected_dayparts = st.multiselect(
            "Filter by daypart",
            available_dayparts,
            default=available_dayparts,
        )

        filtered_timing = timing_df[timing_df["daypart"].isin(selected_dayparts)].copy()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Orders", format_number(filtered_timing["total_orders"].sum()))
        col2.metric("Total Revenue", format_currency(filtered_timing["total_revenue"].sum()))
        col3.metric("Avg Order Value", format_currency(filtered_timing["avg_order_value"].mean()))
        col4.metric("Unique Customers", format_number(filtered_timing["unique_customers"].sum()))

        st.subheader("Revenue by Daypart")

        daypart_summary = (
            filtered_timing
            .groupby("daypart", as_index=False)
            .agg(
                total_orders=("total_orders", "sum"),
                total_revenue=("total_revenue", "sum"),
                avg_order_value=("avg_order_value", "mean"),
                unique_customers=("unique_customers", "sum"),
            )
        )

        chart = (
            alt.Chart(daypart_summary)
            .mark_bar()
            .encode(
                x=alt.X("daypart:N", title="Daypart", sort=daypart_order),
                y=alt.Y("total_revenue:Q", title="Total Revenue"),
                tooltip=[
                    "daypart",
                    "total_orders",
                    alt.Tooltip("total_revenue:Q", format=",.2f"),
                    alt.Tooltip("avg_order_value:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Holiday vs Non-Holiday Performance")

        holiday_summary = (
            filtered_timing
            .groupby("is_holiday", as_index=False)
            .agg(
                total_orders=("total_orders", "sum"),
                total_revenue=("total_revenue", "sum"),
                avg_order_value=("avg_order_value", "mean"),
            )
        )
        holiday_summary["holiday_status"] = holiday_summary["is_holiday"].map(
            {True: "Holiday", False: "Non-Holiday"}
        )

        chart = (
            alt.Chart(holiday_summary)
            .mark_bar()
            .encode(
                x=alt.X("holiday_status:N", title="Holiday Status"),
                y=alt.Y("total_revenue:Q", title="Total Revenue"),
                tooltip=[
                    "holiday_status",
                    "total_orders",
                    alt.Tooltip("total_revenue:Q", format=",.2f"),
                    alt.Tooltip("avg_order_value:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Daily Revenue Trend")

        daily_summary = (
            filtered_timing
            .groupby("order_date", as_index=False)
            .agg(total_revenue=("total_revenue", "sum"))
        )

        line_chart = (
            alt.Chart(daily_summary)
            .mark_line()
            .encode(
                x=alt.X("order_date:T", title="Order Date"),
                y=alt.Y("total_revenue:Q", title="Total Revenue"),
                tooltip=[
                    alt.Tooltip("order_date:T", title="Order Date"),
                    alt.Tooltip("total_revenue:Q", format=",.2f"),
                ],
            )
        )
        st.altair_chart(line_chart, use_container_width=True)

        st.subheader("Detailed Timing Table")
        st.dataframe(
            filtered_timing.sort_values(["order_date", "daypart"]),
            use_container_width=True,
            hide_index=True,
        )
