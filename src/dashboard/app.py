import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

st.set_page_config(page_title = "Server Access Log Analytics", layout = "wide")

@st.cache_resource
def get_spark_session():
    return SparkSession.builder.appName("LogDashboard").getOrCreate()

@st.cache_data
def load_aggregated_data()
    spark = get_spark_session()

    df = spark.read.parquet("data/processed/logs_parquet")

    volume_df = df.groupBy("date").count().orderBy("date").toPandas()

    status_df = df.groupBy("status").count().orderBy("status").toPandas()
    status_df['status'] = status_df['status'].astype(str)

    top_endpoints_df = df.groupBy("request").count().orderBy(F.desc("count")).limit(10).toPandas()

    return volume_df, status_df, top_endpoints_df

st.title("Web Server Access Logs Dashboard")
st.markdown("**Business Goasl:** Analyze web traffic to identigy spikes, error patterns, and popular resources")

with st.spinner("Querying Parquet Files with PySpark"):
    volume_df, status_df, top_endpoints_df = load_aggregated_data()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Traffic Volume Over Time")
    st.line_chart(data = volume_df.set_index("date"))

with col2:
    st.subheader("HTTP Status Code Distribution")
    st.bar_chart(data = status_df.set_index("status"))

st.subheader("Top 10 Requested Endpoints")
st.dataframe(top_endpoints_df, use_container_width = True)
