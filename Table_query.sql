
/*=============================================================
 CUSTOMER 360 ANALYTICS & CHURN RISK MODEL
 Purpose:
 Create a single customer-level analytical dataset by combining
 Orders, Payments, Support Tickets, and Web Activity data.

 Business Use Cases:
 - Customer Segmentation
 - Churn Prediction
 - Customer Health Scoring
 - Revenue Analysis
 - Executive Reporting

 Optimization Strategy:
 Aggregate fact tables BEFORE joins to avoid row multiplication.
=============================================================*/


/*-------------------------------------------------------------
 STEP 1: Aggregate Orders Data
 Purpose:
 Calculate revenue and order-related KPIs per customer.

 Why:
 Orders table is transactional and may contain millions of rows.
 Aggregating first reduces data volume significantly.
-------------------------------------------------------------*/
WITH customer_orders AS
(
    SELECT
        customer_id,

        /* Number of unique orders placed */
        COUNT(DISTINCT order_id) AS total_orders,

        /* Total customer revenue */
        SUM(amount) AS total_revenue,

        /* Average order value */
        AVG(amount) AS avg_order_value,

        /* Most recent order date */
        MAX(order_date) AS last_order_date,

        /* First purchase date */
        MIN(order_date) AS first_order_date

    FROM orders

    /* Consider only completed orders */
    WHERE status = 'Completed'

    GROUP BY customer_id
),


/*-------------------------------------------------------------
 STEP 2: Aggregate Payment Data
 Purpose:
 Calculate payment success metrics.

 Why:
 Helps identify customers with successful transactions.
-------------------------------------------------------------*/
customer_payments AS
(
    SELECT
        customer_id,

        /* Number of successful payments */
        COUNT(*) AS successful_payments,

        /* Total payment amount received */
        SUM(amount) AS total_payment_amount,

        /* Last successful payment date */
        MAX(payment_date) AS last_payment_date

    FROM payment

    WHERE payment_status = 'Success'

    GROUP BY customer_id
),


/*-------------------------------------------------------------
 STEP 3: Aggregate Support Data
 Purpose:
 Measure customer satisfaction and support burden.

 Why:
 Customers with many open tickets may have lower satisfaction.
-------------------------------------------------------------*/
customer_support AS
(
    SELECT
        customer_id,

        /* Total support tickets */
        COUNT(ticket_id) AS total_tickets,

        /* Open issues requiring action */
        SUM(
            CASE
                WHEN resolution_status = 'Pending'
                THEN 1
                ELSE 0
            END
        ) AS open_tickets,

        /* Count payment-related complaints */
        SUM(
            CASE
                WHEN LOWER(issue_type) LIKE '%payment%'
                THEN 1
                ELSE 0
            END
        ) AS payment_related_issues

    FROM support_tickets

    GROUP BY customer_id
),


/*-------------------------------------------------------------
 STEP 4: Aggregate Website Activity
 Purpose:
 Measure customer engagement.

 Why:
 Highly engaged customers are less likely to churn.
-------------------------------------------------------------*/
customer_web AS
(
    SELECT
        customer_id,

        /* Total sessions */
        COUNT(DISTINCT session_id) AS total_sessions,

        /* Distinct pages visited */
        COUNT(DISTINCT page_viewed) AS unique_pages,

        /* Most recent website activity */
        MAX(session_time) AS last_activity_date

    FROM web_activities

    GROUP BY customer_id
),


/*-------------------------------------------------------------
 STEP 5: Create Customer 360 Dataset
 Purpose:
 Combine all customer information into one row per customer.

 Optimization:
 Joining aggregated datasets prevents data explosion.
-------------------------------------------------------------*/
customer_metrics AS
(
    SELECT

        c.customer_id,
        c.name,
        c.email,
        c.location,

        /* Order KPIs */
        COALESCE(o.total_orders,0) AS total_orders,
        COALESCE(o.total_revenue,0) AS total_revenue,
        COALESCE(o.avg_order_value,0) AS avg_order_value,

        /* Payment KPIs */
        COALESCE(p.successful_payments,0) AS successful_payments,

        /* Support KPIs */
        COALESCE(s.total_tickets,0) AS total_tickets,
        COALESCE(s.open_tickets,0) AS open_tickets,
        COALESCE(s.payment_related_issues,0) AS payment_related_issues,

        /* Engagement KPIs */
        COALESCE(w.total_sessions,0) AS total_sessions,
        COALESCE(w.unique_pages,0) AS unique_pages,

        o.first_order_date,
        o.last_order_date,
        p.last_payment_date,
        w.last_activity_date,

        /* Days since last purchase */
        DATEDIFF(
            DAY,
            o.last_order_date,
            GETDATE()
        ) AS days_since_last_order

    FROM customers c

    LEFT JOIN customer_orders o
        ON c.customer_id = o.customer_id

    LEFT JOIN customer_payments p
        ON c.customer_id = p.customer_id

    LEFT JOIN customer_support s
        ON c.customer_id = s.customer_id

    LEFT JOIN customer_web w
        ON c.customer_id = w.customer_id
),


/*-------------------------------------------------------------
 STEP 6: Calculate Customer Health Score
 Purpose:
 Generate a single metric representing customer value.

 Formula Logic:
 Revenue contributes most.
 Orders and engagement increase score.
 Open tickets decrease score.
-------------------------------------------------------------*/
customer_scoring AS
(
    SELECT
        *,

        (
            (total_revenue * 0.40)
            +
            (total_orders * 20)
            +
            (total_sessions * 5)
            -
            (open_tickets * 15)
        ) AS customer_health_score

    FROM customer_metrics
),


/*-------------------------------------------------------------
 STEP 7: Rank Customers
 Purpose:
 Identify top customers and segment by revenue.

 Window Functions:
 DENSE_RANK -> Customer ranking
 NTILE -> Revenue quartiles
-------------------------------------------------------------*/
customer_ranked AS
(
    SELECT
        *,

        DENSE_RANK() OVER
        (
            ORDER BY total_revenue DESC
        ) AS revenue_rank,

        NTILE(4) OVER
        (
            ORDER BY total_revenue DESC
        ) AS revenue_quartile

    FROM customer_scoring
)


/*-------------------------------------------------------------
 STEP 8: Final Business Output
 Purpose:
 Produce customer-level insights for Power BI and executives.

 Outputs:
 - Customer Tier
 - Churn Risk
 - Revenue Rank
 - Health Score
-------------------------------------------------------------*/
SELECT

    customer_id,
    name,
    email,
    location,

    total_orders,
    total_revenue,
    avg_order_value,

    successful_payments,

    total_tickets,
    open_tickets,

    total_sessions,
    unique_pages,

    customer_health_score,

    revenue_rank,

    revenue_quartile,

    days_since_last_order,

    /* Churn Risk Classification */
    CASE

        WHEN days_since_last_order > 90
             AND total_sessions < 3
        THEN 'High Churn Risk'

        WHEN days_since_last_order BETWEEN 30 AND 90
        THEN 'Medium Churn Risk'

        ELSE 'Low Churn Risk'

    END AS churn_risk,

    /* Customer Value Segmentation */
    CASE

        WHEN customer_health_score >= 1000
        THEN 'Platinum'

        WHEN customer_health_score >= 500
        THEN 'Gold'

        WHEN customer_health_score >= 200
        THEN 'Silver'

        ELSE 'Bronze'

    END AS customer_tier

FROM customer_ranked

ORDER BY
    customer_health_score DESC,
    revenue_rank;