import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 生成时间字段
time_range = pd.date_range(start='2019-01-01', end='2021-12-31', freq='D')
# 生成水果和用户
fruit_list = random.choices(['苹果', '香蕉', '橙子', '葡萄', '西瓜'], k=len(time_range))
name_list = random.choices(['张三', '李四', '王五', '赵六', '孙七'], k=len(time_range))

# 生成订单数据
order = pd.DataFrame({
"time": time_range,
"fruit": fruit_list,
"name": name_list,
"kilogram": np.random.choice(list(range(50, 100)), size=len(time_range), replace=True)
})

# 生成水果的信息数据
information = pd.DataFrame({
"fruit": ['苹果', '香蕉', '橙子', '葡萄', '西瓜'],
"price": [3.8, 8.9, 12.8, 6.8, 15.8],
"region": ["华南", "华北", "西北", "华中", "西北"]
})

# 数据合并
df = pd.merge(order, information, on="fruit")
# 生成新的字段：订单金额
df["amount"] = df["kilogram"] * df["price"]

print(df)




import plotly.express as px
import plotly.graph_objects as go

# 提取年份和月份
df["year"] = df["time"].dt.year
df["month"] = df["time"].dt.month
df["year_month"] = df["time"].dt.strftime('%Y%m')

# 分年月统计销量
df1 = df.groupby(["year_month"])["kilogram"].sum().reset_index()
fig = px.bar(df1, x="year_month", y="kilogram", color="kilogram")
fig.update_layout(xaxis_tickangle=45)
fig.show()

# 分年月统计销售额
df2 = df.groupby(["year_month"])["amount"].sum().reset_index()
df2["amount"] = df2["amount"].apply(lambda x: round(x, 2))
fig = go.Figure()
fig.add_trace(go.Scatter(x=df2["year_month"], y=df2["amount"], mode='lines+markers', name='lines'))
fig.update_layout(xaxis_tickangle=45)
fig.show()


# 计算每位客户的订单次数和总金额
rfm = df.groupby("name").agg({"time": "count", "amount": "sum"}).reset_index().rename(columns={"time": "frequency"})
# 计算最近一次购买日期
rfm["recency"] = df.groupby("name")["time"].max().apply(lambda x: (datetime.now() - x).days)





# 计算每个用户的复购周期
df["prev_time"] = df.groupby("name")["time"].shift(1)
df["repurchase_cycle"] = (df["time"] - df["prev_time"]).dt.days
df = df.dropna(subset=["repurchase_cycle"])

# 可视化复购周期
import plotly.express as px
fig = px.bar(df, x="repurchase_cycle", y="name", orientation="h", color="repurchase_cycle", color_continuous_scale="spectral")
fig.show()