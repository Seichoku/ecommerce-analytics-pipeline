"""
E-Commerce Sales Analytics Pipeline
=====================================
Comprehensive analysis covering:
  1. Exploratory Data Analysis (EDA)
  2. Sales Trend Analysis & Forecasting
  3. Customer Cohort & Retention Analysis
  4. RFM Segmentation
  5. Product Performance Analysis
  6. Channel Attribution Analysis

Author: Emeka Ichoku
Dataset: Synthetic e-commerce data (2022-2024)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import warnings
warnings.filterwarnings('ignore')

# ── Style ─────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.facecolor':   '#f8f9fa',
    'axes.grid':        True,
    'grid.alpha':       0.4,
    'axes.spines.top':  False,
    'axes.spines.right':False,
    'font.family':      'sans-serif',
    'axes.titlesize':   13,
    'axes.titleweight': 'bold',
})
PALETTE = ['#2E86AB','#A23B72','#F18F01','#C73E1D','#3B1F2B','#44BBA4','#E94F37','#393E41']

DATA_DIR    = "data"
OUTPUT_DIR  = "analysis"

# ═══════════════════════════════════════════════════════════════════════════════
# 1. LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════════
print("Loading data...")
orders     = pd.read_csv(f"{DATA_DIR}/orders.csv",      parse_dates=["order_date"])
customers  = pd.read_csv(f"{DATA_DIR}/customers.csv",   parse_dates=["registration_date"])
products   = pd.read_csv(f"{DATA_DIR}/products.csv")
items      = pd.read_csv(f"{DATA_DIR}/order_items.csv")

# Join core fact table
fact = (orders
    .merge(items.groupby("order_id").agg(
        gross_revenue=("line_total","sum"),
        gross_profit=("line_profit","sum"),
        total_units=("quantity","sum"),
        n_items=("order_item_id","count")
    ).reset_index(), on="order_id", how="left")
    .merge(customers[["customer_id","country","region","acquisition_channel",
                       "is_loyalty_member","age_group","registration_date"]], 
           on="customer_id", how="left")
)
fact["order_month"]   = fact["order_date"].dt.to_period("M")
fact["order_year"]    = fact["order_date"].dt.year
fact["order_quarter"] = fact["order_date"].dt.quarter
completed             = fact[fact["order_status"] == "completed"].copy()

print(f"  Orders: {len(orders):,}  |  Customers: {len(customers):,}  |  "
      f"Products: {len(products):,}  |  Line items: {len(items):,}")
print(f"  Completed orders: {len(completed):,}  |  "
      f"Total revenue: ${completed['gross_revenue'].sum():,.0f}")

# ═══════════════════════════════════════════════════════════════════════════════
# 2. MONTHLY REVENUE TREND + FORECAST
# ═══════════════════════════════════════════════════════════════════════════════
print("\nRunning sales trend analysis...")

monthly = (completed
    .groupby("order_month")
    .agg(revenue=("gross_revenue","sum"),
         profit=("gross_profit","sum"),
         orders=("order_id","count"),
         customers=("customer_id","nunique"))
    .reset_index()
)
monthly["order_month_dt"] = monthly["order_month"].dt.to_timestamp()
monthly["revenue_ma3"]    = monthly["revenue"].rolling(3).mean()
monthly["mom_growth"]     = monthly["revenue"].pct_change() * 100

# Simple linear trend forecast (last 6 months extrapolated 3 months)
x = np.arange(len(monthly))
z = np.polyfit(x[-12:], monthly["revenue"].values[-12:], 1)
p = np.poly1d(z)
forecast_x  = np.arange(len(monthly), len(monthly) + 3)
forecast_rev = p(forecast_x)
last_date    = monthly["order_month_dt"].iloc[-1]
forecast_dates = pd.date_range(last_date + pd.offsets.MonthBegin(1), periods=3, freq="MS")

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle("Sales Performance Dashboard — 2022–2024", fontsize=16, fontweight="bold", y=1.01)

# Revenue trend
ax = axes[0, 0]
ax.bar(monthly["order_month_dt"], monthly["revenue"]/1e3,
       color=PALETTE[0], alpha=0.6, width=20, label="Monthly Revenue")
ax.plot(monthly["order_month_dt"], monthly["revenue_ma3"]/1e3,
        color=PALETTE[2], linewidth=2.5, label="3-Month MA")
ax.plot(forecast_dates, forecast_rev/1e3,
        color=PALETTE[3], linewidth=2, linestyle="--", marker="o", label="Forecast")
ax.set_title("Monthly Revenue & 3-Month Forecast")
ax.set_ylabel("Revenue ($K)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:.0f}K"))
ax.legend(fontsize=9)

# Profit margin trend
ax = axes[0, 1]
monthly["margin_pct"] = monthly["profit"] / monthly["revenue"] * 100
ax.plot(monthly["order_month_dt"], monthly["margin_pct"],
        color=PALETTE[1], linewidth=2.5, marker="o", markersize=4)
ax.axhline(monthly["margin_pct"].mean(), color="grey", linestyle="--", alpha=0.6,
           label=f"Avg: {monthly['margin_pct'].mean():.1f}%")
ax.set_title("Gross Margin % by Month")
ax.set_ylabel("Margin %")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:.0f}%"))
ax.legend()

# Revenue by channel
ax = axes[1, 0]
channel_rev = (completed.groupby("channel")["gross_revenue"]
               .sum().sort_values(ascending=True))
bars = ax.barh(channel_rev.index, channel_rev.values/1e3,
               color=PALETTE[:len(channel_rev)])
ax.set_title("Total Revenue by Acquisition Channel")
ax.set_xlabel("Revenue ($K)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:.0f}K"))
for bar, val in zip(bars, channel_rev.values):
    ax.text(val/1e3 + 5, bar.get_y() + bar.get_height()/2,
            f"${val/1e3:.0f}K", va="center", fontsize=9)

# Quarterly revenue
ax = axes[1, 1]
qtr = (completed.groupby(["order_year","order_quarter"])["gross_revenue"]
       .sum().reset_index())
qtr["label"] = qtr["order_year"].astype(str) + " Q" + qtr["order_quarter"].astype(str)
colors = [PALETTE[y-2022] for y in qtr["order_year"]]
ax.bar(qtr["label"], qtr["gross_revenue"]/1e3, color=colors, alpha=0.8)
ax.set_title("Revenue by Quarter")
ax.set_ylabel("Revenue ($K)")
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:.0f}K"))

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/01_sales_dashboard.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ Saved: 01_sales_dashboard.png")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. CUSTOMER COHORT RETENTION ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
print("Running cohort retention analysis...")

completed["cohort_month"] = (completed
    .groupby("customer_id")["order_date"]
    .transform("min")
    .dt.to_period("M")
)
completed["order_period"] = completed["order_date"].dt.to_period("M")
completed["period_number"] = (
    (completed["order_period"] - completed["cohort_month"]).apply(lambda x: x.n)
)

cohort_data = (completed
    .groupby(["cohort_month","period_number"])["customer_id"]
    .nunique()
    .reset_index()
    .rename(columns={"customer_id": "customers"})
)
cohort_pivot = cohort_data.pivot_table(
    index="cohort_month", columns="period_number", values="customers"
)
cohort_size   = cohort_pivot[0]
retention     = cohort_pivot.divide(cohort_size, axis=0).round(3) * 100

# Plot first 12 months of retention for recent cohorts
ret_plot = retention.iloc[-18:, :13]

fig, ax = plt.subplots(figsize=(14, 8))
import matplotlib.colors as mcolors
cmap = plt.cm.RdYlGn
im = ax.imshow(ret_plot.values, cmap=cmap, vmin=0, vmax=100, aspect="auto")

ax.set_xticks(range(ret_plot.shape[1]))
ax.set_xticklabels([f"Month {i}" for i in range(ret_plot.shape[1])], fontsize=9)
ax.set_yticks(range(len(ret_plot)))
ax.set_yticklabels([str(c) for c in ret_plot.index], fontsize=9)

for i in range(ret_plot.shape[0]):
    for j in range(ret_plot.shape[1]):
        val = ret_plot.values[i, j]
        if not np.isnan(val):
            ax.text(j, i, f"{val:.0f}%", ha="center", va="center",
                    fontsize=7.5, color="black" if val > 40 else "white",
                    fontweight="bold" if j == 0 else "normal")

plt.colorbar(im, ax=ax, label="Retention %", shrink=0.8)
ax.set_title("Customer Cohort Retention Matrix (Month 0–12)", fontsize=14, fontweight="bold")
ax.set_xlabel("Months Since First Purchase")
ax.set_ylabel("Cohort (First Purchase Month)")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/02_cohort_retention.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ Saved: 02_cohort_retention.png")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. RFM SEGMENTATION
# ═══════════════════════════════════════════════════════════════════════════════
print("Running RFM segmentation...")

snapshot_date = completed["order_date"].max() + pd.Timedelta(days=1)

rfm = (completed.groupby("customer_id").agg(
    recency=("order_date",   lambda x: (snapshot_date - x.max()).days),
    frequency=("order_id",   "nunique"),
    monetary=("gross_revenue","sum")
).reset_index())

for col, labels, ascending in [
    ("recency",   [5,4,3,2,1], True),
    ("frequency", [1,2,3,4,5], True),
    ("monetary",  [1,2,3,4,5], True),
]:
    rfm[f"{col}_score"] = pd.qcut(
        rfm[col], q=5, labels=labels, duplicates="drop"
    ).astype(int)

rfm["rfm_score"] = rfm["recency_score"] + rfm["frequency_score"] + rfm["monetary_score"]

def rfm_segment(row):
    r, f = row["recency_score"], row["frequency_score"]
    if r >= 4 and f >= 4: return "Champions"
    if r >= 3 and f >= 3: return "Loyal Customers"
    if r >= 4 and f <= 2: return "Recent Customers"
    if r <= 2 and f >= 3: return "At Risk"
    if r <= 2 and f <= 2: return "Lost"
    return "Potential Loyalists"

rfm["segment"] = rfm.apply(rfm_segment, axis=1)

seg_summary = (rfm.groupby("segment").agg(
    customers=("customer_id","count"),
    avg_revenue=("monetary","mean"),
    avg_frequency=("frequency","mean"),
    avg_recency=("recency","mean")
).reset_index().sort_values("customers", ascending=False))

fig, axes = plt.subplots(1, 2, figsize=(15, 6))
fig.suptitle("RFM Customer Segmentation", fontsize=15, fontweight="bold")

# Segment sizes
ax = axes[0]
colors_rfm = [PALETTE[i % len(PALETTE)] for i in range(len(seg_summary))]
wedges, texts, autotexts = ax.pie(
    seg_summary["customers"],
    labels=seg_summary["segment"],
    autopct="%1.1f%%",
    colors=colors_rfm,
    startangle=140,
    pctdistance=0.8,
)
for t in autotexts: t.set_fontsize(9)
ax.set_title("Customer Distribution by Segment")

# Avg revenue by segment
ax = axes[1]
bars = ax.barh(seg_summary["segment"], seg_summary["avg_revenue"],
               color=colors_rfm)
ax.set_title("Average Revenue per Customer by Segment")
ax.set_xlabel("Average Revenue ($)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:,.0f}"))
for bar, val in zip(bars, seg_summary["avg_revenue"]):
    ax.text(val + 5, bar.get_y() + bar.get_height()/2,
            f"${val:,.0f}", va="center", fontsize=9)

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/03_rfm_segmentation.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ Saved: 03_rfm_segmentation.png")

# ═══════════════════════════════════════════════════════════════════════════════
# 5. PRODUCT PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════
print("Running product performance analysis...")

prod_perf = (items
    .merge(orders[["order_id","order_status"]], on="order_id")
    .query("order_status == 'completed'")
    .merge(products[["product_id","category","brand"]], on="product_id")
)

cat_perf = (prod_perf.groupby("category").agg(
    revenue=("line_total","sum"),
    profit=("line_profit","sum"),
    units=("quantity","sum"),
    orders=("order_id","nunique")
).reset_index())
cat_perf["margin_pct"] = cat_perf["profit"] / cat_perf["revenue"] * 100
cat_perf = cat_perf.sort_values("revenue", ascending=False)

fig, axes = plt.subplots(1, 2, figsize=(15, 6))
fig.suptitle("Product & Category Performance", fontsize=15, fontweight="bold")

ax = axes[0]
x = np.arange(len(cat_perf))
w = 0.4
b1 = ax.bar(x - w/2, cat_perf["revenue"]/1e3, w, label="Revenue ($K)", color=PALETTE[0], alpha=0.8)
ax2 = ax.twinx()
ax2.plot(x, cat_perf["margin_pct"], color=PALETTE[2], marker="o",
         linewidth=2, label="Margin %")
ax.set_xticks(x)
ax.set_xticklabels(cat_perf["category"], rotation=30, ha="right")
ax.set_ylabel("Revenue ($K)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:.0f}K"))
ax2.set_ylabel("Margin %")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:.0f}%"))
ax.set_title("Revenue & Margin by Category")
lines1, labels1 = ax.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax.legend(lines1+lines2, labels1+labels2, loc="upper right", fontsize=9)

# Top 15 products
ax = axes[1]
top_prods = (prod_perf.groupby("product_id")["line_total"].sum()
             .nlargest(15).reset_index()
             .merge(products[["product_id","product_name","category"]], on="product_id"))
top_prods["short_name"] = top_prods["product_name"].str[:30]
colors_p = [PALETTE[list(cat_perf["category"]).index(c) % len(PALETTE)]
            for c in top_prods["category"]]
ax.barh(top_prods["short_name"], top_prods["line_total"]/1e3, color=colors_p)
ax.set_title("Top 15 Products by Revenue")
ax.set_xlabel("Revenue ($K)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:.0f}K"))
ax.invert_yaxis()

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/04_product_performance.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ Saved: 04_product_performance.png")

# ═══════════════════════════════════════════════════════════════════════════════
# 6. KEY METRICS SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("KEY BUSINESS METRICS SUMMARY")
print("="*60)
total_rev    = completed["gross_revenue"].sum()
total_profit = completed["gross_profit"].sum()
total_orders = len(completed)
total_custs  = completed["customer_id"].nunique()
aov          = completed["gross_revenue"].mean()
margin       = total_profit / total_rev * 100
repeat_rate  = (completed.groupby("customer_id")["order_id"].count() > 1).mean() * 100
return_rate  = (orders["order_status"] == "returned").mean() * 100

print(f"  Total Revenue:        ${total_rev:>12,.0f}")
print(f"  Total Gross Profit:   ${total_profit:>12,.0f}")
print(f"  Gross Margin:         {margin:>11.1f}%")
print(f"  Total Orders:         {total_orders:>12,}")
print(f"  Unique Customers:     {total_custs:>12,}")
print(f"  Avg Order Value:      ${aov:>12,.2f}")
print(f"  Repeat Customer Rate: {repeat_rate:>11.1f}%")
print(f"  Return Rate:          {return_rate:>11.1f}%")
print(f"\n  RFM Segments:")
for _, row in seg_summary.iterrows():
    pct = row["customers"] / len(rfm) * 100
    print(f"    {row['segment']:<22} {row['customers']:>5,} customers ({pct:.1f}%)"
          f"  |  Avg Revenue: ${row['avg_revenue']:>8,.0f}")
print("="*60)
print("\nAll analysis complete. Charts saved to /analysis/")
