
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, when


# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="DeliverIQ | Delivery Intelligence",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

BASE_DIR = Path(__file__).parent
HADOOP_OUTPUT = BASE_DIR / "mapreduce_output.txt"

BLUE = "#4263EB"
PURPLE = "#7C3AED"
GREEN = "#059669"
RED = "#DC4C64"


# =====================================================
# COMPLETE UI STYLING
# =====================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: #F5F7FC;
}

/* Reduce top whitespace */
[data-testid="stHeader"] {
    background: transparent !important;
    height: 1.5rem !important;
}

.block-container {
    padding-top: 0.45rem !important;
    padding-bottom: 2rem !important;
    max-width: 1600px;
}

/* Sidebar */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div:first-child {
    background: #101B35 !important;
}

[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
    color: #BAC6DF !important;
}

[data-testid="stSidebar"] label {
    color: #C5D0E6 !important;
    font-size: 0.86rem !important;
    font-weight: 500 !important;
}

/* Soft dark dropdowns */
[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background: #202E4C !important;
    border: 1px solid #354565 !important;
    border-radius: 10px !important;
    min-height: 42px;
}

[data-testid="stSidebar"] [data-baseweb="select"] * {
    color: #E8EEFF !important;
}

[data-testid="stSidebar"] [data-baseweb="popover"],
[data-testid="stSidebar"] [data-baseweb="menu"] {
    background: #202E4C !important;
}

[data-testid="stSidebar"] [data-baseweb="menu"] * {
    color: #E8EEFF !important;
}

[data-testid="stSidebar"] input {
    background: #202E4C !important;
    color: #FFFFFF !important;
    border-color: #354565 !important;
    border-radius: 10px !important;
}

[data-testid="stSidebar"] [data-testid="stDateInput"] input {
    background: #202E4C !important;
    color: #FFFFFF !important;
}

/* Brand */
.brand {
    display: flex;
    align-items: center;
    gap: 12px;
    margin: 8px 0 30px;
}

.brand-icon {
    width: 46px;
    height: 46px;
    border-radius: 14px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 24px;
    background: linear-gradient(135deg,#4263EB,#8B5CF6);
    box-shadow: 0 6px 20px rgba(66,99,235,.3);
}

.brand-name {
    font-size: 19px;
    font-weight: 800;
    color: #FFFFFF;
}

.brand-caption {
    font-size: 10px;
    color: #9EACCA;
    margin-top: 3px;
    letter-spacing: .7px;
}

.sidebar-section {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.6px;
    color: #8395B8;
    font-weight: 700;
    margin: 22px 0 12px;
}

/* Main heading */
.page-title {
    font-size: 31px;
    font-weight: 800;
    color: #17233F;
    letter-spacing: -1px;
    line-height: 1.2;
}

.page-subtitle {
    font-size: 13px;
    color: #7D8AA2;
    margin: 7px 0 22px;
}

/* KPI cards */
.kpi-card {
    background: #FFFFFF;
    padding: 19px;
    border: 1px solid #E8ECF4;
    border-radius: 17px;
    box-shadow: 0 5px 18px rgba(28,43,75,.04);
    min-height: 140px;
}

.kpi-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.kpi-label {
    color: #7D8AA2;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .3px;
}

.kpi-icon {
    width: 37px;
    height: 37px;
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 18px;
}

.kpi-value {
    font-size: 30px;
    font-weight: 800;
    letter-spacing: -1px;
    color: #17233F;
    margin-top: 12px;
}

.kpi-note {
    color: #8995A9;
    font-size: 11px;
    margin-top: 7px;
}

/* Section headings and panels */
.section-title {
    font-size: 19px;
    font-weight: 750;
    color: #17233F;
    letter-spacing: -.4px;
}

.section-caption {
    color: #8995A9;
    font-size: 12px;
    margin: 4px 0 13px;
}

div[data-testid="stDataFrame"] {
    border: 1px solid #E8ECF4;
    border-radius: 12px;
    overflow: hidden;
}

div.stButton > button,
div[data-testid="stDownloadButton"] button {
    border-radius: 10px;
    font-weight: 600;
}

hr {
    border-color: #E8ECF4;
}

.footer {
    color: #9AA5B7;
    font-size: 11px;
    text-align: center;
    padding: 18px 0 4px;
}
</style>
""", unsafe_allow_html=True)


# =====================================================
# APACHE SPARK
# =====================================================
@st.cache_resource
def start_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("DeliveryIntelligence")
        .getOrCreate()
    )

spark = start_spark()


# =====================================================
# SAMPLE DATA
# =====================================================
data = {
    "order_id": list(range(101, 111)),
    "city": [
        "Mysuru", "Bengaluru", "Mysuru", "Mandya",
        "Bengaluru", "Mandya", "Mysuru", "Bengaluru",
        "Mandya", "Mysuru"
    ],
    "shipping_method": [
        "Standard", "Express", "Standard", "Express",
        "Standard", "Standard", "Express", "Standard",
        "Express", "Standard"
    ],
    "expected_date": [
        "2026-10-20", "2026-10-20", "2026-10-21",
        "2026-10-21", "2026-10-22", "2026-10-22",
        "2026-10-23", "2026-10-23", "2026-10-24",
        "2026-10-24"
    ],
    "actual_date": [
        "2026-10-20", "2026-10-22", "2026-10-23",
        "2026-10-21", "2026-10-24", "2026-10-25",
        "2026-10-22", "2026-10-23", "2026-10-27",
        "2026-10-26"
    ]
}


@st.cache_data
def process_orders(raw_data):
    pdf = pd.DataFrame(raw_data)
    sdf = spark.createDataFrame(pdf)

    sdf = (
        sdf
        .withColumn("expected_date", to_date("expected_date"))
        .withColumn("actual_date", to_date("actual_date"))
        .withColumn(
            "delay_days",
            datediff(col("actual_date"), col("expected_date"))
        )
        .withColumn(
            "status",
            when(col("delay_days") > 0, "Delayed")
            .otherwise("On Time")
        )
    )

    result = sdf.toPandas()
    result["expected_date"] = pd.to_datetime(result["expected_date"])
    result["actual_date"] = pd.to_datetime(result["actual_date"])
    return result


result = process_orders(data)


# =====================================================
# SIDEBAR
# =====================================================
with st.sidebar:
    st.markdown("""
    <div class="brand">
        <div class="brand-icon">📦</div>
        <div>
            <div class="brand-name">DeliverIQ</div>
            <div class="brand-caption">LOGISTICS INTELLIGENCE</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(
        '<div class="sidebar-section">Workspace</div>',
        unsafe_allow_html=True
    )
    st.markdown("📊 **Delivery Overview**")
    st.caption("Order performance and delivery analytics")

    st.markdown(
        '<div class="sidebar-section">Filters</div>',
        unsafe_allow_html=True
    )

    cities = ["All Cities"] + sorted(result["city"].unique().tolist())
    selected_city = st.selectbox("📍 City", cities)

    selected_status = st.selectbox(
        "🚚 Delivery status",
        ["All Statuses", "Delayed", "On Time"]
    )

    shipping_options = (
        ["All Methods"]
        + sorted(result["shipping_method"].unique().tolist())
    )
    selected_shipping = st.selectbox("📦 Shipping method", shipping_options)

    date_min = result["expected_date"].min().date()
    date_max = result["expected_date"].max().date()

    selected_dates = st.date_input(
        "📅 Expected delivery date",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max
    )

    st.markdown("---")
    st.markdown(
        '<div class="sidebar-section">Processing engines</div>',
        unsafe_allow_html=True
    )
    st.markdown("⚡ **Apache Spark**")
    st.caption("Local data processing")

    st.markdown("🗺️ **Hadoop MapReduce**")
    st.caption("Mapper · Shuffle & Sort · Reducer")

    st.markdown("---")

    if st.button("↻ Refresh dashboard", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption("Fictional sample dataset")


# =====================================================
# FILTER DATA
# =====================================================
filtered = result.copy()

if selected_city != "All Cities":
    filtered = filtered[filtered["city"] == selected_city]

if selected_status != "All Statuses":
    filtered = filtered[filtered["status"] == selected_status]

if selected_shipping != "All Methods":
    filtered = filtered[
        filtered["shipping_method"] == selected_shipping
    ]

if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
    filtered = filtered[
        filtered["expected_date"].dt.date.between(start_date, end_date)
    ]


# =====================================================
# HEADER — NO DEMO BADGE
# =====================================================
header_left, header_right = st.columns([3, 1], gap="small")

with header_left:
    st.markdown(
        '<div class="page-title">Delivery Intelligence</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<div class="page-subtitle">'
        'Monitor delivery performance, identify delays and compare shipping.'
        '</div>',
        unsafe_allow_html=True
    )

with header_right:
    st.markdown(
        "<div style='text-align:right;color:#8290A8;"
        "font-size:11px;padding-top:8px'>"
        + datetime.now().strftime("%d %b %Y · %H:%M:%S")
        + "</div>",
        unsafe_allow_html=True
    )
    st.markdown(
        "<div style='text-align:right;color:#9AA5B7;font-size:10px'>"
        "Last refreshed</div>",
        unsafe_allow_html=True
    )


# =====================================================
# KPI CARDS
# =====================================================
total = len(filtered)
delayed = int((filtered["status"] == "Delayed").sum())
on_time = int((filtered["status"] == "On Time").sum())
delay_rate = delayed / total * 100 if total else 0
on_time_rate = on_time / total * 100 if total else 0


def kpi_card(label, value, icon, color, background, note):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-top">
            <div class="kpi-label">{label}</div>
            <div class="kpi-icon" style="background:{background};color:{color}">
                {icon}
            </div>
        </div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-note">{note}</div>
    </div>
    """, unsafe_allow_html=True)


k1, k2, k3, k4 = st.columns(4, gap="medium")

with k1:
    kpi_card("TOTAL ORDERS", total, "📦", BLUE, "#EAF0FF",
             "Orders in current selection")
with k2:
    kpi_card("DELAYED ORDERS", delayed, "⚠", RED, "#FFF0F1",
             f"{delay_rate:.1f}% of selected orders")
with k3:
    kpi_card("DELIVERED ON TIME", on_time, "✓", GREEN, "#E6F8EF",
             f"{on_time_rate:.1f}% of selected orders")
with k4:
    kpi_card("DELAY RATE", f"{delay_rate:.1f}%", "◷", PURPLE, "#F1EAFE",
             "Lower is better")


# =====================================================
# CHARTS
# =====================================================
st.write("")
st.markdown(
    '<div class="section-title">Delivery Performance</div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="section-caption">'
    'Explore delays by city and compare shipping methods.'
    '</div>',
    unsafe_allow_html=True
)

chart_layout = dict(
    template="plotly_white",
    paper_bgcolor="white",
    plot_bgcolor="white",
    font=dict(family="Inter, sans-serif", color="#526079", size=12),
    margin=dict(l=10, r=25, t=25, b=10),
    showlegend=False
)

left, right = st.columns(2, gap="large")

with left:
    with st.container(border=True):
        st.markdown("**Delayed orders by city**")
        st.caption("Orders delivered after their expected date")

        city_chart = (
            filtered.assign(
                delayed_flag=(filtered["status"] == "Delayed").astype(int)
            )
            .groupby("city", as_index=False)
            .agg(
                Delayed=("delayed_flag", "sum"),
                Total=("order_id", "count")
            )
            .sort_values("Delayed", ascending=True)
        )

        if not city_chart.empty:
            fig1 = px.bar(
                city_chart,
                x="Delayed",
                y="city",
                orientation="h",
                text="Delayed",
                color_discrete_sequence=[BLUE],
                labels={"Delayed": "Delayed orders", "city": "City"}
            )
            fig1.update_traces(
                textposition="outside",
                cliponaxis=False,
                marker_line_width=0
            )
            fig1.update_layout(**chart_layout, height=300)
            fig1.update_xaxes(dtick=1, rangemode="tozero")
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No orders match the selected filters.")

with right:
    with st.container(border=True):
        st.markdown("**Shipping method performance**")
        st.caption("Percentage of orders delivered late")

        ship_chart = (
            filtered.assign(
                delayed_flag=(filtered["status"] == "Delayed").astype(int)
            )
            .groupby("shipping_method", as_index=False)
            .agg(
                Total=("order_id", "count"),
                Delayed=("delayed_flag", "sum")
            )
        )

        if not ship_chart.empty:
            ship_chart["Delay Rate (%)"] = (
                ship_chart["Delayed"] / ship_chart["Total"] * 100
            )

            fig2 = px.bar(
                ship_chart,
                x="Delay Rate (%)",
                y="shipping_method",
                orientation="h",
                text=ship_chart["Delay Rate (%)"].map(lambda x: f"{x:.1f}%"),
                color_discrete_sequence=[PURPLE],
                labels={
                    "Delay Rate (%)": "Delay rate (%)",
                    "shipping_method": "Shipping method"
                }
            )
            fig2.update_traces(
                textposition="outside",
                cliponaxis=False,
                marker_line_width=0
            )
            fig2.update_layout(**chart_layout, height=300)
            fig2.update_xaxes(range=[0, 110], ticksuffix="%")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No orders match the selected filters.")


# =====================================================
# STATUS AND CITY SUMMARY
# =====================================================
st.write("")
a, b = st.columns(2, gap="large")

with a:
    with st.container(border=True):
        st.markdown("**Delivery status distribution**")

        if total:
            status_chart = pd.DataFrame({
                "Status": ["Delayed", "On Time"],
                "Orders": [delayed, on_time]
            })

            fig3 = px.pie(
                status_chart,
                names="Status",
                values="Orders",
                hole=0.68,
                color="Status",
                color_discrete_map={"Delayed": RED, "On Time": GREEN}
            )
            fig3.update_traces(
                textposition="outside",
                textinfo="label+percent",
                marker=dict(line=dict(color="white", width=3))
            )
            fig3.update_layout(
                template="plotly_white",
                paper_bgcolor="white",
                font=dict(family="Inter, sans-serif", color="#526079"),
                margin=dict(l=10, r=10, t=15, b=10),
                height=300,
                legend=dict(orientation="h", y=-0.08)
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No data available.")

with b:
    with st.container(border=True):
        st.markdown("**City performance summary**")

        if total:
            summary = (
                filtered.assign(
                    delayed_flag=(filtered["status"] == "Delayed").astype(int)
                )
                .groupby("city", as_index=False)
                .agg(
                    Orders=("order_id", "count"),
                    Delayed=("delayed_flag", "sum")
                )
            )
            summary["On Time"] = summary["Orders"] - summary["Delayed"]
            summary["Delay Rate (%)"] = (
                summary["Delayed"] / summary["Orders"] * 100
            ).round(1)

            st.dataframe(
                summary.rename(columns={"city": "City"}),
                hide_index=True,
                use_container_width=True,
                height=240
            )
        else:
            st.info("No data available.")


# =====================================================
# ORDER EXPLORER
# =====================================================
st.write("")
st.markdown(
    '<div class="section-title">Order Explorer</div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="section-caption">'
    'Search and export orders from the current selection.'
    '</div>',
    unsafe_allow_html=True
)

search = st.text_input(
    "Search orders",
    placeholder="Enter an order ID or city name..."
)

display_data = filtered.copy()

if search.strip():
    query = search.strip().lower()
    display_data = display_data[
        display_data["order_id"].astype(str).str.contains(query, case=False)
        | display_data["city"].str.lower().str.contains(query)
    ]

display_data["expected_date"] = display_data["expected_date"].dt.strftime("%Y-%m-%d")
display_data["actual_date"] = display_data["actual_date"].dt.strftime("%Y-%m-%d")

display_data = display_data.rename(columns={
    "order_id": "Order ID",
    "city": "City",
    "shipping_method": "Shipping Method",
    "expected_date": "Expected Date",
    "actual_date": "Actual Date",
    "delay_days": "Delay (days)",
    "status": "Status"
})

st.dataframe(
    display_data,
    hide_index=True,
    use_container_width=True,
    height=330
)

st.download_button(
    "⬇ Download filtered orders",
    data=display_data.to_csv(index=False).encode("utf-8"),
    file_name="delivery_analysis.csv",
    mime="text/csv"
)


# =====================================================
# HADOOP MAPREDUCE RESULTS
# =====================================================
st.write("")
st.markdown(
    '<div class="section-title">Hadoop MapReduce Engine</div>',
    unsafe_allow_html=True
)
st.markdown(
    '<div class="section-caption">'
    'Counts produced by the separately executed Hadoop Streaming job.'
    '</div>',
    unsafe_allow_html=True
)

with st.container(border=True):
    if HADOOP_OUTPUT.exists():
        hadoop_counts = {}

        try:
            with open(HADOOP_OUTPUT, "r", encoding="utf-8") as file:
                for line in file:
                    parts = line.strip().split("\t")
                    if len(parts) == 2:
                        hadoop_counts[parts[0]] = int(parts[1])

            h_delayed = hadoop_counts.get("Delayed", 0)
            h_on_time = hadoop_counts.get("On Time", 0)
            h_total = h_delayed + h_on_time
            h_rate = h_delayed / h_total * 100 if h_total else 0

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Processed Orders", h_total)
            m2.metric("Delayed", h_delayed)
            m3.metric("On Time", h_on_time)
            m4.metric("Delay Rate", f"{h_rate:.1f}%")

            hadoop_chart = pd.DataFrame({
                "Status": ["Delayed", "On Time"],
                "Orders": [h_delayed, h_on_time]
            })

            fig4 = px.bar(
                hadoop_chart,
                x="Orders",
                y="Status",
                orientation="h",
                text="Orders",
                color="Status",
                color_discrete_map={"Delayed": RED, "On Time": GREEN}
            )
            fig4.update_traces(
                textposition="outside",
                cliponaxis=False,
                marker_line_width=0
            )
            fig4.update_layout(
                **chart_layout,
                height=230,
                xaxis=dict(
                    showgrid=True,
                    gridcolor="#EDF0F6",
                    rangemode="tozero",
                    dtick=1
                ),
                yaxis=dict(showgrid=False, automargin=True)
            )
            st.plotly_chart(fig4, use_container_width=True)

            st.success("Hadoop MapReduce output loaded successfully.")

        except (ValueError, OSError) as error:
            st.error(f"Could not read Hadoop output: {error}")
    else:
        st.warning(
            "Hadoop output not found. Run the Hadoop job and copy its "
            "part-00000 file to mapreduce_output.txt."
        )
        st.code(
            "cp ~/delivery-output/part-00000 "
            "/mnt/c/Users/DELL/OneDrive/Desktop/"
            "delivery-dashboard/mapreduce_output.txt",
            language="bash"
        )


# =====================================================
# FOOTER
# =====================================================
st.markdown("---")
st.markdown("""
<div class="footer">
    DELIVERIQ · DELIVERY INTELLIGENCE<br>
    Apache Spark · Hadoop MapReduce · Python · Streamlit<br>
    Fictional sample data for academic demonstration.
</div>
""", unsafe_allow_html=True)
