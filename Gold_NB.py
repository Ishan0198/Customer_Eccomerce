#!/usr/bin/env python
# coding: utf-8

# ## Gold_NB
# 
# null

# In[10]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[11]:


cust = spark.table("silver.customers").alias("c")
orders = spark.table("silver.orders").alias("o")
payments = spark.table("silver.payments").alias("p")
support = spark.table("silver.support").alias("s")
web = spark.table("silver.web").alias("w")


# ## **Join**

# In[12]:


customer360 = (
    cust
    .join(orders, "customer_id", "left")
    .join(payments, "customer_id", "left")
    .join(support, "customer_id", "left")
    .join(web, "customer_id", "left")
        .select(
        col("c.customer_id"),
        col("c.name"),
        col("c.email"),
        col("c.gender"),
        col("c.dob"),
        col("c.location"),

        col("o.order_id"),
        col("o.order_date"),
        col("o.amount").alias("order_amount"),
        col("o.status"),

        col("p.payment_method"),
        col("p.payment_status"),
        col("p.amount").alias("payment_amount"),

        col("s.ticket_id"),
        col("s.issue_type"),
        col("s.ticket_date"),
        col("s.resolution_status"),

        col("w.page_viewed"),
        col("w.device_type"),
        col("w.session_time")
    )
)


# In[13]:


customer360.write.format("delta").mode("overwrite").saveAsTable("Gold.Customer360")

