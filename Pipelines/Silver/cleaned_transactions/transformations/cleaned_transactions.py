from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Cleaned transaction data from bronze layer with standardized formatting and data quality checks"
)
@dp.expect_or_drop("valid_payment_method", "payment_method IN ('Cash', 'Debit Card', 'Credit Card', 'Google Pay', 'Apple Pay')")
@dp.expect("valid_total_amount", "ABS(total_amount - (quantity * unit_price)) < 0.01")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_unit_price", "unit_price > 0")
@dp.expect_or_drop("required_fields", "transaction_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL")
def cleaned_transactions():
    return (
        spark.readStream.table("dataengineering.bronze.raw_transactions")
        .withColumn("product_name", F.trim(F.initcap(F.col("product_name"))))
    )
