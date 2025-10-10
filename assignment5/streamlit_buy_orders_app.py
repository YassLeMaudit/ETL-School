from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st
from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session

TABLE_NAME = "INGEST1.INGEST1.CLIENT_BUY_ORDERS"

st.set_page_config(page_title="Client Buy Orders Explorer", layout="wide")


@st.cache_resource(show_spinner=False)
def get_session():
    return get_active_session()


@st.cache_resource(show_spinner=False)
def get_base_table():
    return get_session().table(TABLE_NAME)


@st.cache_resource(show_spinner=False)
def get_flattened_table():
    df = get_base_table()
    addr = F.col("ADDRESS")
    emergency = F.col("EMERGENCY_CONTACT")
    return df.select(
        "TXID",
        "RFID",
        "CAR_MODEL",
        "BRAND",
        "ENGINE",
        "HORSEPOWER",
        "PURCHASE_TIME",
        "DAYS",
        "NAME",
        "PHONE",
        "EMAIL",
        addr["street_address"].alias("STREET_ADDRESS"),
        addr["city"].alias("CITY"),
        addr["state"].alias("STATE"),
        addr["postalcode"].alias("POSTALCODE"),
        emergency["name"].alias("EMERGENCY_NAME"),
        emergency["phone"].alias("EMERGENCY_PHONE"),
    )


@st.cache_data(show_spinner=False, ttl=300)
def get_distinct(col: str, limit: int = 200):
    pdf = (
        get_flattened_table()
        .select(F.col(col))
        .distinct()
        .order_by(F.col(col))
        .limit(limit)
        .to_pandas()
    )
    return [value for value in pdf[col].tolist() if value is not None]


@st.cache_data(show_spinner=False, ttl=300)
def get_minmax(col: str) -> Tuple[Optional[Any], Optional[Any]]:
    row = (
        get_flattened_table()
        .agg(F.min(F.col(col)).alias("MINV"), F.max(F.col(col)).alias("MAXV"))
        .collect()[0]
    )
    return row["MINV"], row["MAXV"]


def apply_filters(df, filters: Dict[str, Any]):
    if filters.get("BRAND"):
        df = df.filter(F.col("BRAND").isin(filters["BRAND"]))
    if filters.get("CAR_MODEL"):
        df = df.filter(F.col("CAR_MODEL").isin(filters["CAR_MODEL"]))
    if filters.get("ENGINE"):
        df = df.filter(F.col("ENGINE").isin(filters["ENGINE"]))
    if filters.get("CITY"):
        df = df.filter(F.col("CITY").isin(filters["CITY"]))
    if filters.get("STATE"):
        df = df.filter(F.col("STATE").isin(filters["STATE"]))

    if filters.get("HORSEPOWER"):
        low, high = filters["HORSEPOWER"]
        df = df.filter((F.col("HORSEPOWER") >= low) & (F.col("HORSEPOWER") <= high))
    if filters.get("DAYS"):
        low, high = filters["DAYS"]
        df = df.filter((F.col("DAYS") >= low) & (F.col("DAYS") <= high))

    if filters.get("PURCHASE_TIME"):
        start_date, end_date = filters["PURCHASE_TIME"]
        if start_date and end_date:
            df = df.filter(
                (F.col("PURCHASE_TIME") >= F.to_timestamp(F.lit(str(start_date))))
                & (
                    F.col("PURCHASE_TIME")
                    < F.to_timestamp(F.lit(str(end_date))) + F.expr("INTERVAL '1 DAY'")
                )
            )

    if filters.get("QUERY"):
        query = filters["QUERY"].strip()
        if query:
            like_value = f"%{query}%"
            df = df.filter(
                F.col("NAME").ilike(like_value)
                | F.col("EMAIL").ilike(like_value)
                | F.col("PHONE").ilike(like_value)
                | F.col("RFID").ilike(like_value)
            )
    return df


@st.cache_data(show_spinner=False, ttl=300)
def to_pandas_limited(_df, limit_rows: int = 10000):
    return _df.limit(limit_rows).to_pandas()


def format_metric(value: Optional[Any], decimals: int = 1) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def render_monitoring_page(filtered_df):
    st.subheader("Operational Monitoring")

    total_orders = filtered_df.count()
    avg_hp_row = filtered_df.agg(F.avg("HORSEPOWER").alias("AVG_HP")).collect()[0]
    avg_days_row = filtered_df.agg(F.avg("DAYS").alias("AVG_DAYS")).collect()[0]
    unique_customers = filtered_df.select(F.col("EMAIL")).distinct().count()

    metrics = st.columns(4)
    metrics[0].metric("Orders", f"{total_orders:,}")
    metrics[1].metric("Average horsepower", format_metric(avg_hp_row["AVG_HP"]))
    metrics[2].metric("Average contract days", format_metric(avg_days_row["AVG_DAYS"]))
    metrics[3].metric("Unique customers", f"{unique_customers:,}")

    st.markdown("### Quick breakdowns")
    col1, col2 = st.columns(2)
    with col1:
        top_n_brands = st.slider(
            "Brands to display",
            min_value=5,
            max_value=30,
            value=10,
            step=1,
            key="monitoring_brands_top_n",
        )
        brand_pdf = (
            filtered_df.group_by("BRAND")
            .agg(
                F.count(F.lit(1)).alias("ORDERS"),
                F.avg("HORSEPOWER").alias("AVG_HP"),
            )
            .order_by(F.col("ORDERS").desc())
            .limit(top_n_brands)
            .to_pandas()
        )
        if not brand_pdf.empty:
            brand_pdf = brand_pdf.rename(
                columns={"BRAND": "Brand", "ORDERS": "Orders", "AVG_HP": "Avg horsepower"}
            )
            st.bar_chart(brand_pdf.set_index("Brand")["Orders"])
            st.dataframe(brand_pdf, use_container_width=True)
        else:
            st.info("No brands to display with current filters.")

    with col2:
        top_n_models = st.slider(
            "Models to display",
            min_value=5,
            max_value=30,
            value=10,
            step=1,
            key="monitoring_models_top_n",
        )
        model_pdf = (
            filtered_df.group_by("CAR_MODEL")
            .agg(
                F.count(F.lit(1)).alias("ORDERS"),
                F.avg("HORSEPOWER").alias("AVG_HP"),
            )
            .order_by(F.col("ORDERS").desc())
            .limit(top_n_models)
            .to_pandas()
        )
        if not model_pdf.empty:
            model_pdf = model_pdf.rename(
                columns={"CAR_MODEL": "Model", "ORDERS": "Orders", "AVG_HP": "Avg horsepower"}
            )
            st.bar_chart(model_pdf.set_index("Model")["Orders"])
            st.dataframe(model_pdf, use_container_width=True)
        else:
            st.info("No models to display with current filters.")


def render_business_insights_page(filtered_df):
    st.subheader("Business Insights")

    segment_options = {
        "Brand": "BRAND",
        "Model": "CAR_MODEL",
        "State": "STATE",
        "City": "CITY",
        "Engine": "ENGINE",
    }
    segment_label = st.selectbox(
        "Segment customers by",
        list(segment_options.keys()),
        key="bi_segment",
    )
    top_n = st.slider(
        "Top groups",
        min_value=5,
        max_value=30,
        value=10,
        step=1,
        key="bi_top_groups",
    )
    segment_col = segment_options[segment_label]

    segment_pdf = (
        filtered_df.group_by(segment_col)
        .agg(
            F.count(F.lit(1)).alias("ORDERS"),
            F.avg("HORSEPOWER").alias("AVG_HP"),
            F.avg("DAYS").alias("AVG_DAYS"),
        )
        .order_by(F.col("ORDERS").desc())
        .limit(top_n)
        .to_pandas()
    )

    if not segment_pdf.empty:
        rename_map = {
            segment_col: segment_label,
            "ORDERS": "Orders",
            "AVG_HP": "Avg horsepower",
            "AVG_DAYS": "Avg days",
        }
        segment_pdf = segment_pdf.rename(columns=rename_map)
        st.markdown("### Top performers")
        st.bar_chart(segment_pdf.set_index(segment_label)["Orders"])
        st.dataframe(segment_pdf, use_container_width=True)
    else:
        st.info("No segment data available with current filters.")

    st.markdown("### Regional footprint")
    state_pdf = (
        filtered_df.filter(F.col("STATE").is_not_null())
        .group_by("STATE")
        .agg(F.count(F.lit(1)).alias("ORDERS"))
        .order_by(F.col("ORDERS").desc())
        .limit(25)
        .to_pandas()
    )
    if not state_pdf.empty:
        state_pdf = state_pdf.rename(columns={"STATE": "State", "ORDERS": "Orders"})
        st.bar_chart(state_pdf.set_index("State")["Orders"])
        st.dataframe(state_pdf, use_container_width=True)
    else:
        st.info("No states available for the selected filters.")

    st.markdown("### Engine preferences")
    engine_pdf = (
        filtered_df.group_by("ENGINE")
        .agg(
            F.count(F.lit(1)).alias("ORDERS"),
            F.avg("HORSEPOWER").alias("AVG_HP"),
        )
        .order_by(F.col("ORDERS").desc())
        .limit(25)
        .to_pandas()
    )
    if not engine_pdf.empty:
        engine_pdf = engine_pdf.rename(
            columns={"ENGINE": "Engine", "ORDERS": "Orders", "AVG_HP": "Avg horsepower"}
        )
        st.bar_chart(engine_pdf.set_index("Engine")["Orders"])
        st.dataframe(engine_pdf, use_container_width=True)
    else:
        st.info("Engine insights unavailable for the current selection.")


def render_data_preview(filtered_df):
    st.markdown("### Data preview and export")
    all_columns = [
        "TXID",
        "RFID",
        "BRAND",
        "CAR_MODEL",
        "ENGINE",
        "HORSEPOWER",
        "PURCHASE_TIME",
        "DAYS",
        "NAME",
        "PHONE",
        "EMAIL",
        "STREET_ADDRESS",
        "CITY",
        "STATE",
        "POSTALCODE",
        "EMERGENCY_NAME",
        "EMERGENCY_PHONE",
    ]
    default_columns = [
        "TXID",
        "BRAND",
        "CAR_MODEL",
        "ENGINE",
        "HORSEPOWER",
        "PURCHASE_TIME",
        "DAYS",
        "CITY",
        "STATE",
    ]
    selected_columns = st.multiselect(
        "Columns to display",
        options=all_columns,
        default=default_columns,
        key="preview_columns",
    )
    limit_rows = st.slider(
        "Rows to display",
        min_value=50,
        max_value=5000,
        value=500,
        step=50,
        key="preview_limit",
    )

    table_df = filtered_df.select([F.col(col) for col in selected_columns]) if selected_columns else filtered_df
    preview_pdf = to_pandas_limited(table_df, limit_rows)

    if preview_pdf.empty:
        st.info("No rows match the current filters. Adjust the filters to see data.")
    else:
        st.dataframe(preview_pdf, use_container_width=True)
        csv_bytes = preview_pdf.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name="client_buy_orders.csv",
            mime="text/csv",
        )


def main():
    st.title("Client Buy Orders Explorer")
    st.caption(f"Snowflake source: `{TABLE_NAME}`")

    try:
        base_table = get_flattened_table()
    except Exception as exc:  # pragma: no cover - handled in the UI
        st.error(
            "Unable to read the source table. Verify role, database, and permissions before retrying.\n"
            f"Details: {exc}"
        )
        st.stop()

    filters: Dict[str, Any] = {}

    with st.sidebar:
        st.header("Navigation")
        selected_page = st.radio(
            "Select a page",
            ["Monitoring", "Business Insights"],
            key="nav_page",
        )

        st.header("Filters")
        filters["BRAND"] = st.multiselect("Brand", get_distinct("BRAND"), key="filter_brand")
        filters["CAR_MODEL"] = st.multiselect("Model", get_distinct("CAR_MODEL"), key="filter_model")
        filters["ENGINE"] = st.multiselect("Engine", get_distinct("ENGINE"), key="filter_engine")
        filters["CITY"] = st.multiselect("City", get_distinct("CITY"), key="filter_city")
        filters["STATE"] = st.multiselect("State", get_distinct("STATE"), key="filter_state")

        hp_min, hp_max = get_minmax("HORSEPOWER")
        if hp_min is not None and hp_max is not None:
            hp_min = float(hp_min)
            hp_max = float(hp_max)
            filters["HORSEPOWER"] = st.slider(
                "Horsepower",
                min_value=hp_min,
                max_value=hp_max,
                value=(hp_min, hp_max),
                step=1.0,
                key="filter_hp",
            )

        days_min, days_max = get_minmax("DAYS")
        if days_min is not None and days_max is not None:
            days_min = int(days_min)
            days_max = int(days_max)
            filters["DAYS"] = st.slider(
                "Contract days",
                min_value=days_min,
                max_value=days_max,
                value=(days_min, days_max),
                step=1,
                key="filter_days",
            )

        purchase_min, purchase_max = get_minmax("PURCHASE_TIME")
        if purchase_min and purchase_max:
            min_date = pd.to_datetime(purchase_min).date()
            max_date = pd.to_datetime(purchase_max).date()
            filters["PURCHASE_TIME"] = st.date_input(
                "Purchase period",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key="filter_purchase",
            )
        else:
            filters["PURCHASE_TIME"] = None

        filters["QUERY"] = st.text_input(
            "Search (name, email, phone, RFID)",
            placeholder="Type to search...",
            key="filter_query",
        )

    filtered_table = apply_filters(base_table, filters)

    if selected_page == "Monitoring":
        render_monitoring_page(filtered_table)
    else:
        render_business_insights_page(filtered_table)

    st.divider()
    render_data_preview(filtered_table)

    st.caption("Note: Snowpark objects are cached with cache_resource; serializable data uses cache_data.")


if __name__ == "__main__":
    main()

