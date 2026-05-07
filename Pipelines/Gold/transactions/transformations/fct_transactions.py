from pyspark import pipelines as dp

@dp.table(name="fct_transactions")
def fct_transactions():
    return (
        spark.readStream.table("dataengineering.silver.cleaned_transactions")
        .select(
            "transaction_id",
            "transaction_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "store_location",
            "payment_method"
        )
    )
