from pyspark import pipelines as dp

# Step 1: Extract product information from source as a temporary view
@dp.temporary_view()
def product_changes():
    return (
        spark.readStream.table("dataengineering.silver.cleaned_transactions")
        .select("product_id", "product_name", "category")
        .dropDuplicates(["product_id"])
    )

# Step 2: Create target streaming table for products
dp.create_streaming_table(name="dim_products")

# Step 3: Apply Auto CDC to maintain current product dimension (SCD Type 1)
dp.create_auto_cdc_flow(
    target="dim_products",
    source="product_changes",
    keys=["product_id"],
    sequence_by="product_id"
)
