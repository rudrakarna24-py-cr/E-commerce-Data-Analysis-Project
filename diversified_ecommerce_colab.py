# ============================================================
# Diversified E-Commerce Product Analysis — EDA Project
# Course  : Data Science - I  |  Internal Component II
# Dataset : diversified_ecommerce_dataset.csv
# Tools   : Python (Pandas, NumPy, Matplotlib) + PostgreSQL
# Platform: Google Colab
# ============================================================


# -------------------------------------------------------
# STEP 1: Import Libraries
# -------------------------------------------------------
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import io

from google.colab import files   # Colab file upload

plt.rcParams['figure.figsize']   = (10, 5)
plt.rcParams['axes.spines.top']  = False
plt.rcParams['axes.spines.right']= False

COLORS = ['#0D9488','#14B8A6','#0F766E','#5EEAD4','#99F6E4']

print("=" * 55)
print("  Diversified E-Commerce EDA  |  Data Science - I")
print("=" * 55)
print("✅ Step 1: Libraries imported successfully!\n")


# -------------------------------------------------------
# STEP 2: Upload CSV via Google Colab
# -------------------------------------------------------
print("=" * 55)
print("STEP 2: UPLOAD CSV FILE")
print("=" * 55)

print("\n📂 Please upload: diversified_ecommerce_dataset.csv")
uploaded = files.upload()   # file picker dialog appears

filename = list(uploaded.keys())[0]
df = pd.read_csv(io.BytesIO(uploaded[filename]))

# Rename columns to clean snake_case for easier coding
df.columns = ['product_id','product_name','category','price','discount','tax_rate',
              'stock_level','supplier_id','age_group','location','gender',
              'shipping_cost','shipping_method','return_rate','seasonality','popularity_index']

# Calculate final_price after discount
df['final_price'] = (df['price'] * (1 - df['discount'] / 100)).round(2)

print(f"\n✅ File loaded : '{filename}'")
print(f"   Shape       : {df.shape[0]:,} rows × {df.shape[1]} columns\n")


# -------------------------------------------------------
# STEP 3: Data Exploration
# -------------------------------------------------------
print("=" * 55)
print("STEP 3: DATA EXPLORATION")
print("=" * 55)

print("\n📌 First 5 rows:")
print(df.head())

print("\n📌 Data Types:")
print(df.dtypes)

print("\n📌 Statistical Summary:")
print(df[['price','discount','stock_level','shipping_cost',
          'return_rate','popularity_index','final_price']].describe().round(2))

print("\n📌 Missing Values:")
print(df.isnull().sum())

print("\n📌 Unique Categories:")
print(df['category'].value_counts())

print("\n📌 Unique Locations:")
print(df['location'].value_counts())

print("\n📌 Shipping Methods:")
print(df['shipping_method'].value_counts())

print("\n📌 Age Groups:")
print(df['age_group'].value_counts())


# -------------------------------------------------------
# STEP 4: Data Cleaning
# -------------------------------------------------------
print("\n" + "=" * 55)
print("STEP 4: DATA CLEANING")
print("=" * 55)

before = len(df)

# Drop duplicates
df = df.drop_duplicates()
print(f"  Duplicates removed    : {before - len(df)}")

# Fill any missing numerics
for col in ['price','discount','shipping_cost','return_rate','popularity_index']:
    if df[col].isnull().sum() > 0:
        df[col] = df[col].fillna(df[col].median())
        print(f"  {col} nulls → filled with median")

# Fill missing categoricals
for col in ['category','location','shipping_method','gender','age_group']:
    if df[col].isnull().sum() > 0:
        df[col] = df[col].fillna(df[col].mode()[0])
        print(f"  {col} nulls → filled with mode")

# Recalculate final_price after cleaning
df['final_price'] = (df['price'] * (1 - df['discount'] / 100)).round(2)

print(f"\n  Rows after cleaning   : {len(df):,}")
print(f"  Missing values left   : {df.isnull().sum().sum()}")
print("✅ Cleaning complete!\n")


# -------------------------------------------------------
# STEP 5: Pandas Analysis (mirrors SQL queries)
# -------------------------------------------------------
print("=" * 55)
print("STEP 5: PANDAS ANALYSIS")
print("=" * 55)

# A) High popularity products (SQL Query 1 equivalent)
popular = df[df['popularity_index'] > 70][
    ['product_name','category','price','discount','popularity_index']
].sort_values('popularity_index', ascending=False)
print(f"\n📌 A) High Popularity Products (index > 70): {len(popular):,} products")
print(popular.head(10).to_string(index=False))

# B) Category summary (SQL Query 2 equivalent)
cat_summary = df.groupby('category').agg(
    total_products  = ('product_id',       'count'),
    avg_price       = ('price',            'mean'),
    avg_final_price = ('final_price',      'mean'),
    avg_return_rate = ('return_rate',      'mean'),
    total_stock     = ('stock_level',      'sum'),
    avg_popularity  = ('popularity_index', 'mean')
).round(2).reset_index().sort_values('avg_popularity', ascending=False)
print(f"\n📌 B) Category Summary:")
print(cat_summary.to_string(index=False))

# C) Price spread per category (SQL Query 3 equivalent)
price_spread = df.groupby('category').agg(
    highest_price    = ('price', 'max'),
    lowest_price     = ('price', 'min'),
    avg_shipping     = ('shipping_cost', 'mean')
).round(2).reset_index()
price_spread['price_spread'] = (price_spread['highest_price'] - price_spread['lowest_price']).round(2)
price_spread = price_spread.sort_values('price_spread', ascending=False)
print(f"\n📌 C) Price Spread per Category:")
print(price_spread.to_string(index=False))

# D) Rank products by final_price within category (SQL Query 4 equivalent)
df['price_rank_in_category'] = df.groupby('category')['final_price'].rank(
    method='dense', ascending=False).astype(int)
top_per_cat = df[df['price_rank_in_category'] <= 3].sort_values(
    ['category','price_rank_in_category'])
print(f"\n📌 D) Top 3 by Final Price per Category (DENSE_RANK):")
print(top_per_cat[['product_id','product_name','category','final_price',
                    'price_rank_in_category']].head(15).to_string(index=False))

# E) High return rate products (SQL Query 5 equivalent)
high_return = df[df['return_rate'] > 15][
    ['product_id','product_name','category','price','return_rate','popularity_index','location']
].sort_values('return_rate', ascending=False)
print(f"\n📌 E) High Return Rate Products (> 15%): {len(high_return):,} products")
print(high_return.head(10).to_string(index=False))

# F) Product price vs category avg (SQL Query 6 equivalent)
df['category_avg_price'] = df.groupby('category')['price'].transform('mean').round(2)
df['price_vs_cat_avg']   = (df['price'] - df['category_avg_price']).round(2)
print(f"\n📌 F) Price vs Category Average (Window Function style):")
print(df[['product_name','category','price','category_avg_price','price_vs_cat_avg']]
      .drop_duplicates('product_name').head(10).to_string(index=False))

# G) CTE-style: Avg popularity per shipping method & category (SQL Query 7 equivalent)
shipping_cat = df.groupby(['shipping_method','category']).agg(
    avg_popularity  = ('popularity_index', 'mean'),
    avg_return_rate = ('return_rate',      'mean'),
    total_products  = ('product_id',       'count')
).round(2).reset_index()
shipping_cat['rank'] = shipping_cat.groupby('shipping_method')['avg_popularity'].rank(
    method='dense', ascending=False).astype(int)
shipping_cat = shipping_cat.sort_values(['shipping_method','rank'])
print(f"\n📌 G) Popularity Rank per Shipping Method (CTE + DENSE_RANK style):")
print(shipping_cat.to_string(index=False))


# -------------------------------------------------------
# STEP 6: PostgreSQL Query Reference
# -------------------------------------------------------
print("\n" + "=" * 55)
print("STEP 6: POSTGRESQL QUERY REFERENCE")
print("=" * 55)

queries = {
    "Q1 — WHERE: High Popularity Products": """
    SELECT product_id, product_name, category,
           price, discount, popularity_index
    FROM   product_sales
    WHERE  popularity_index > 70
    ORDER BY popularity_index DESC;
    """,

    "Q2 — GROUP BY: Category Summary": """
    SELECT category,
           COUNT(*)                        AS total_products,
           ROUND(AVG(price), 2)            AS avg_price,
           ROUND(AVG(return_rate), 2)      AS avg_return_rate,
           SUM(stock_level)                AS total_stock,
           ROUND(AVG(popularity_index), 2) AS avg_popularity
    FROM   product_sales
    GROUP BY category
    ORDER BY avg_popularity DESC;
    """,

    "Q3 — MAX-MIN: Price Spread per Category": """
    SELECT category,
           ROUND(MAX(price) - MIN(price), 2) AS price_spread,
           ROUND(MAX(price), 2)              AS highest_price,
           ROUND(MIN(price), 2)              AS lowest_price
    FROM   product_sales
    GROUP BY category
    ORDER BY price_spread DESC;
    """,

    "Q4 — DENSE_RANK: Price Rank in Category": """
    SELECT product_id, product_name, category,
           price, discount, final_price,
           DENSE_RANK() OVER (
               PARTITION BY category
               ORDER BY final_price DESC
           ) AS price_rank_in_category
    FROM   product_sales
    ORDER BY category, price_rank_in_category;
    """,

    "Q5 — WHERE: High Return Rate Products": """
    SELECT product_id, product_name, category,
           price, return_rate, popularity_index, location
    FROM   product_sales
    WHERE  return_rate > 15
    ORDER BY return_rate DESC;
    """,

    "Q6 — AVG OVER PARTITION: Price vs Category Avg": """
    SELECT category, product_name,
           ROUND(AVG(price), 2) AS product_avg_price,
           ROUND(AVG(AVG(price)) OVER (PARTITION BY category), 2)
               AS category_avg_price
    FROM   product_sales
    GROUP BY category, product_name
    ORDER BY category, product_avg_price DESC;
    """,

    "Q7 — CTE + DENSE_RANK: Rank by Shipping Method": """
    WITH ShippingCategorySummary AS (
        SELECT shipping_method, category,
               ROUND(AVG(popularity_index), 2) AS avg_popularity,
               ROUND(AVG(return_rate), 2)      AS avg_return_rate,
               COUNT(*)                        AS total_products
        FROM   product_sales
        GROUP BY shipping_method, category
    )
    SELECT shipping_method, category,
           avg_popularity, avg_return_rate, total_products,
           DENSE_RANK() OVER (
               PARTITION BY shipping_method
               ORDER BY avg_popularity DESC
           ) AS rank_within_shipping_method
    FROM   ShippingCategorySummary
    ORDER BY shipping_method, rank_within_shipping_method;
    """
}

for title, query in queries.items():
    print(f"\n-- {title}")
    print(query)


# -------------------------------------------------------
# STEP 7: Visualisations using Matplotlib
# -------------------------------------------------------
print("\n" + "=" * 55)
print("STEP 7: GENERATING VISUALISATIONS")
print("=" * 55)

# ── Dashboard: 6 charts ───────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.suptitle('Diversified E-Commerce Product EDA Dashboard',
             fontsize=16, fontweight='bold', color='#0A2342', y=1.01)

# Plot 1: Avg Popularity by Category (Bar)
cats = cat_summary['category'].tolist()
pops = cat_summary['avg_popularity'].tolist()
bars = axes[0,0].bar(cats, pops, color=COLORS, edgecolor='white', width=0.6)
axes[0,0].set_title('Avg Popularity Index by Category', fontweight='bold')
axes[0,0].set_xlabel('Category')
axes[0,0].set_ylabel('Avg Popularity Index')
axes[0,0].tick_params(axis='x', rotation=15)
for bar, val in zip(bars, pops):
    axes[0,0].text(bar.get_x() + bar.get_width()/2,
                   bar.get_height() + 0.5,
                   f'{val:.1f}', ha='center', fontsize=9, color='#0A2342')

# Plot 2: Avg Return Rate by Category (Bar)
ret = cat_summary.sort_values('avg_return_rate', ascending=False)
axes[0,1].bar(ret['category'], ret['avg_return_rate'],
              color=COLORS, edgecolor='white', width=0.6)
axes[0,1].set_title('Avg Return Rate (%) by Category', fontweight='bold')
axes[0,1].set_xlabel('Category')
axes[0,1].set_ylabel('Avg Return Rate (%)')
axes[0,1].tick_params(axis='x', rotation=15)

# Plot 3: Price Distribution (Histogram)
axes[0,2].hist(df['price'], bins=40, color='#0D9488', edgecolor='white', alpha=0.85)
axes[0,2].axvline(df['price'].mean(), color='#F59E0B', linestyle='--',
                  linewidth=2, label=f"Mean ${df['price'].mean():.0f}")
axes[0,2].set_title('Product Price Distribution', fontweight='bold')
axes[0,2].set_xlabel('Price ($)')
axes[0,2].set_ylabel('Frequency')
axes[0,2].legend(fontsize=9)

# Plot 4: Total Stock by Category (Horizontal Bar)
stock = cat_summary.sort_values('total_stock')
axes[1,0].barh(stock['category'], stock['total_stock'],
               color=COLORS[:len(stock)], edgecolor='white')
axes[1,0].set_title('Total Stock Level by Category', fontweight='bold')
axes[1,0].set_xlabel('Total Stock')
for i, v in enumerate(stock['total_stock']):
    axes[1,0].text(v + 100, i, f'{v:,}', va='center', fontsize=9)

# Plot 5: Shipping Method Distribution (Pie)
ship_counts = df['shipping_method'].value_counts()
axes[1,1].pie(ship_counts.values, labels=ship_counts.index,
              autopct='%1.1f%%', colors=COLORS[:3],
              startangle=90, wedgeprops={'edgecolor':'white','linewidth':2})
axes[1,1].set_title('Orders by Shipping Method', fontweight='bold')

# Plot 6: Avg Price vs Discount % (Line)
disc_group = df.groupby('discount').agg(
    avg_price      = ('price',       'mean'),
    avg_final_price= ('final_price', 'mean')
).round(2).reset_index()
x = np.arange(len(disc_group))
axes[1,2].plot(disc_group['discount'], disc_group['avg_price'],
               marker='o', color='#0A2342', linewidth=2, label='Original Price')
axes[1,2].plot(disc_group['discount'], disc_group['avg_final_price'],
               marker='s', color='#0D9488', linewidth=2, linestyle='--', label='Final Price')
axes[1,2].set_title('Avg Price vs Discount %', fontweight='bold')
axes[1,2].set_xlabel('Discount %')
axes[1,2].set_ylabel('Avg Price ($)')
axes[1,2].legend(fontsize=9)

plt.tight_layout()
plt.savefig('eda_dashboard.png', dpi=150, bbox_inches='tight')
plt.show()
print("✅ Dashboard saved → eda_dashboard.png")


# ── Correlation Heatmap (no Seaborn) ─────────────────
numeric_cols = ['price','discount','stock_level','shipping_cost',
                'return_rate','popularity_index','final_price']
corr   = df[numeric_cols].corr().values
labels = ['Price','Discount','Stock','Ship Cost','Return Rate','Popularity','Final Price']

fig2, ax = plt.subplots(figsize=(8, 7))
im = ax.imshow(corr, cmap='RdYlGn', vmin=-1, vmax=1)
ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels, rotation=35, ha='right')
ax.set_yticks(range(len(labels))); ax.set_yticklabels(labels)
for i in range(len(labels)):
    for j in range(len(labels)):
        ax.text(j, i, f'{corr[i,j]:.2f}',
                ha='center', va='center',
                color='black', fontsize=9, fontweight='bold')
plt.colorbar(im, ax=ax, shrink=0.8)
ax.set_title('Correlation Heatmap — Numeric Features',
             fontsize=13, fontweight='bold', color='#0A2342')
plt.tight_layout()
plt.savefig('correlation_heatmap.png', dpi=150, bbox_inches='tight')
plt.show()
print("✅ Correlation heatmap saved → correlation_heatmap.png")


# ── Return Rate by Location (Top 10) ─────────────────
loc_return = df.groupby('location')['return_rate'].mean().round(2).sort_values(ascending=False).head(10)
fig3, ax3  = plt.subplots(figsize=(10, 5))
bars3 = ax3.barh(loc_return.index, loc_return.values,
                 color='#EF4444', edgecolor='white', alpha=0.85)
ax3.set_title('Avg Return Rate by Customer Location (Top 10)',
              fontweight='bold', color='#0A2342')
ax3.set_xlabel('Avg Return Rate (%)')
for bar, val in zip(bars3, loc_return.values):
    ax3.text(val + 0.1, bar.get_y() + bar.get_height()/2,
             f'{val}%', va='center', fontsize=10)
plt.tight_layout()
plt.savefig('return_rate_by_location.png', dpi=150, bbox_inches='tight')
plt.show()
print("✅ Return rate chart saved → return_rate_by_location.png\n")


# -------------------------------------------------------
# STEP 8: Key Insights
# -------------------------------------------------------
print("=" * 55)
print("STEP 8: KEY INSIGHTS")
print("=" * 55)

top_cat      = cat_summary.iloc[0]['category']
top_pop_cat  = cat_summary.sort_values('avg_popularity', ascending=False).iloc[0]['category']
worst_return = cat_summary.sort_values('avg_return_rate', ascending=False).iloc[0]['category']
top_loc      = df.groupby('location')['price'].mean().idxmax()
top_ship     = df['shipping_method'].value_counts().idxmax()
high_pop_pct = round(len(df[df['popularity_index'] > 70]) / len(df) * 100, 2)

print(f"\n  📦 Total Products in Dataset  : {len(df):,}")
print(f"  💰 Avg Product Price           : ${df['price'].mean():,.2f}")
print(f"  🏷️  Avg Discount               : {df['discount'].mean():.1f}%")
print(f"  📈 Avg Final Price after Disc  : ${df['final_price'].mean():,.2f}")
print(f"  🏆 Most Popular Category       : {top_pop_cat}")
print(f"  ⚠️  Highest Return Rate Cat    : {worst_return}")
print(f"  🌍 Highest Avg Price Location  : {top_loc}")
print(f"  🚚 Most Used Shipping Method   : {top_ship}")
print(f"  🔥 High Popularity (>70) %     : {high_pop_pct}% of products")

print(f"\n  📊 Return Rate by Category:")
for _, row in cat_summary[['category','avg_return_rate']].iterrows():
    print(f"     {row['category']:20s} → {row['avg_return_rate']:.2f}%")

print(f"\n  📦 Stock by Category:")
for _, row in cat_summary[['category','total_stock']].iterrows():
    print(f"     {row['category']:20s} → {row['total_stock']:,} units")

print("\n✅ EDA Complete! All charts saved above in Colab.")
