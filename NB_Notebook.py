#!/usr/bin/env python
# coding: utf-8

# ## NB_Notebook
# 
# null

# In[4]:


from pyspark.sql.functions import *
from pyspark.sql.types import *



# ### **Clean Customer**

# In[9]:


customers = spark.table("Source_Bronze.customers")
display(customers)


# In[13]:


customers_clean = (
    customers
    .withColumn("email", lower(trim(col("EMAIL"))))
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("gender", \
    when(lower(col("gender")).isin("f", "female"), "Female").\
    when(lower(col("gender")).isin("m", "male"), "Male").\
    otherwise("Other"))
    .withColumn("dob", to_date(regexp_replace(col("dob"), "/", "-")))
    .withColumn("location", initcap(col("location")))
    .dropDuplicates(["customer_id"])
    .dropna(subset=["customer_id", "email"])
)
display(customers_clean)


# ## **Ingested in Silver**

# In[15]:


customers_clean.write.format("delta").mode("overwrite").saveAsTable("Silver.Customers")


# # **Orders**

# In[18]:


orders = spark.table("Source_Bronze.orders")
orders.printSchema()
display(orders)


# ## **Clean Orders**

# In[31]:


orders_clean = (
    orders.
    withColumn("order_date", when(col("order_date").rlike("^\d{4}/\d{2}/\d{2}$"), to_date(col("order_date"), "yyyy/MM/dd"))
                .when(col("order_date").rlike("^\d{2}-\d{2}-\d{4}$"), to_date(col("order_date"), "dd-MM-yyyy"))
                .when(col("order_date").rlike("^\d{8}$"), to_date(col("order_date"), "yyyyMMdd"))
                .otherwise(to_date(col("order_date"), "yyyy-MM-dd")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .withColumn("status", initcap(col("status")))
    .dropna(subset=["customer_id", "order_date"])
    .dropDuplicates(["order_id"])
    # .filter(col("amount") >= 100)
 )
display(orders_clean)


# ### **Ingest to Silver**

# In[32]:


orders_clean.write.format("delta").mode("overwrite").saveAsTable("Silver.Orders")


# Payments

# In[34]:


payments = spark.table("Source_Bronze.payment")
payments_clean = (
    payments
    .withColumn("payment_date", to_date(regexp_replace(col("payment_date"), "/", "-")))
    .withColumn("payment_method", initcap(col("payment_method")))
    .replace({"creditcard": "Credit Card"}, subset=["payment_method"])
    .withColumn("payment_status", initcap(col("payment_status")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .dropna(subset=["customer_id", "payment_date", "amount"])
)


# In[ ]:


payments_clean.write.format("delta").mode("overwrite").saveAsTable("silver.payments")


# ## **Support**

# In[17]:


support = spark.table("Source_bronze.support_tickets")
support.select("ticket_date").distinct().show()


# In[21]:


support_clean = (
    support
    # .withColumn("ticket_date", to_date(regexp_replace(col("ticket_date"), "/", "-")))
    .withColumn("issue_type", initcap(trim(col("issue_type"))))
    .withColumn("resolution_status", initcap(trim(col("resolution_status"))))
    .replace({"NA": None, "": None}, subset=["issue_type", "resolution_status"])
    .dropDuplicates(["ticket_id"])
    .withColumn(
        "ticket_date",
        coalesce(
        to_date(col("ticket_date"),"yyyy-dd-mm"),
        to_date(col("ticket_date"), "yyyy/MM/dd"),
        to_date(col("ticket_date"),"yyyddmm"),
        to_date(col("ticket_date"),"dd-mm-yyyy")
    ))
    .dropna(subset=["customer_id", "ticket_date"])
)
display(support_clean)


# In[22]:


support_clean.write.format("delta").mode("overwrite").saveAsTable("silver.support")


# ## **Web**

# In[31]:


web = spark.table("Source_Bronze.web_activities")
web_clean = (
    web
    .withColumn("page_viewed", lower(col("page_viewed")))
    .withColumn("session_time",
    coalesce(
    to_date(col("session_time"),"yyyy-dd-mm"),
    to_date(col("session_time"), "yyyy/MM/dd"),
    to_date(col("session_time"), "yyyyMMdd"),
    to_date(col("session_time"), "dd-MM-yyyy")
    ))
    .withColumn("device_type", initcap(col("device_type")))
    .dropDuplicates(["session_id"])
    .dropna(subset=["customer_id", "session_time", "page_viewed"])
)
# display(web_clean)
web_clean.write.format("delta").mode("overwrite").saveAsTable("silver.web")

