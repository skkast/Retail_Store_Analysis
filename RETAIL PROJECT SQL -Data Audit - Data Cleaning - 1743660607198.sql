Create Database Retail_Project
use Retail_Project

--________________________________________________ DATA CLEANING ___ (Slide No. 16-18)___________________________________________________


---------- STEP 1 ---
/*
Cust_order CTE: Aggregates customer order information by summing up the total amount (Total_Amount) for each Customer_id and Order_id, and rounding it off to the nearest integer.

Orderpayment_grouped CTE: Aggregates payment data from the Orders_Payement table by summing up the payment_value for each Order_id and rounding it to the nearest integer.

Match_order CTE: Performs an inner join between Cust_order and Orderpayment_grouped based on Order_id and ensures that the total amount matches the total payment value.

Final Selection: Inserts the results of the Match_order CTE (where total order amount equals payment value) into a new table called Matched_order_1.
*/
---
with   Cust_order as (select A.Customer_id, A.Order_id, round(sum(A.[Total Amount]),0) as Total_amt from Orders A
group by A.Customer_id, A.Order_id),

Orderpayment_grouped as(select  A.order_ID, round(sum(A.payment_value),0) as pay_value_total from OrderPayments A 
 group by A.Order_id),

Match_order as (select A.* from Cust_order as A inner join Orderpayment_grouped as B 
on A.Order_id =B.order_ID and A.Total_amt=B.pay_value_total)
 
select * into Matched_orders from Match_order
select * from Matched_orders

-------- STEP 2------
/*
i. Cust_order CTE: This Common Table Expression (CTE) aggregates the total amount spent per customer for each order in the `Orders` table, rounding the total amount to the nearest integer.

ii. Orderpayment_grouped CTE: This CTE calculates the total payment value for each order from the `Orders_Payment` table, grouping by the order and rounding the total payment value.

iii. Null_list CTE: A right join is performed between `Cust_order` and `Orderpayment_grouped` to find orders where the total amount from `Orders` doesn't match the payment amount from `Orders_Payment`. It filters for cases where no matching customer ID is found, meaning the total order amount is not equal to the payment amount.

iv. Remaining_ids CTE: This part joins the mismatched payment orders from `Null_list` with the `Orders` table to retrieve the correct customer ID and order information where there are discrepancies in payment values.

v. Final Output: The result from `Remaining_ids`, which contains orders with mismatched payment and total amounts, is stored into a new table named `Remaining_orders_1`.
*/


WITH Cust_order AS (
    SELECT 
        A.Customer_id, 
        A.Order_id, 
        Round(sum(A.[Total Amount]),0) AS Total_amt 
    FROM 
        Orders A
    GROUP BY 
        A.Customer_id, 
        A.Order_id
),

Orderpayment_grouped AS (
    SELECT 
        A.Order_ID, 
        Round(sum(A.payment_value ),0) AS pay_value_total 
    FROM 
        orderpayments A
    GROUP BY 
        A.Order_ID
),
--- We are right joining as we are having null values 
Null_list AS (
    SELECT 
        B.* 
    FROM 
        Cust_order AS A 
    RIGHT JOIN 
        Orderpayment_grouped AS B 
    ON 
        A.order_id = B.Order_ID 
        AND A.Total_amt = B.pay_value_total
    WHERE 
        A.Customer_id IS NULL
) ,
Remaining_ids as (SELECT 
    B.Customer_id ,B.Order_id,A.pay_value_total
FROM 
    Null_list  A inner join Orders B on A.order_id =B.Order_id and  A.pay_value_total = round(B.[Total Amount],0))	 

select * into Remaining_orders_1 from Remaining_ids;


----------
with T1 as (select B.* from Matched_orders A inner join Orders B on A.Customer_id=B.Customer_id and A.Order_id =B.Order_id),
	T2 as (select B.* from Remaining_orders_1 A inner join  Orders B on A.Customer_id=B.Customer_id and A.Order_id =B.Order_id and A.pay_value_total=round(B.[Total Amount],0) ),

	T as (select * from T1 union all select * from T2 )

	Select * into NEW_ORDER_TABLE_1 from T

------

Select * into Integrated_Table_1 from (select A.*, D.Category ,C.Avg_rating,E.seller_city ,E.seller_state,E.Region,F.customer_city,F.customer_state,F.Gender from NEW_ORDER_TABLE_1 A  
	inner join (select A.ORDER_id,avg(A.Customer_Satisfaction_Score) as Avg_rating from OrderReview_Ratings A group by A.ORDER_id) as C on C.ORDER_id =A.Order_id 
	inner join productsinfo as D on A.product_id =D.product_id
	inner join (Select distinct * from ['Stores Info]) as E on A.Delivered_StoreID =E.StoreID
	inner join Customers as F on A.Customer_id =F.Custid) as T

Select * From Integrated_Table_1


--------------FINALISED RECORDS AFTER DATA CLEANING -- 98379 DATA RECORDS------------------------

Select * Into Finalised_Records_no from (
Select * From Integrated_Table_1

UNION ALL

(Select T.Customer_id,T.order_id,T.product_id,T.Channel,T.Delivered_StoreID,T.Bill_date_timestamp,Sum(T.Net_QTY)as Quantity,T.[Cost Per Unit],
T.MRP,T.Discount,SUM(Net_amount) as Total_Amount ,C.Category,F.Customer_Satisfaction_Score as Avg_rating,
G.seller_city,G.seller_state,G.Region,E.customer_city,E.customer_state,E.Gender
from (
Select Distinct A.*,(A.[Total Amount]/A.Quantity) as Net_amount, (A.Quantity/A.Quantity) as Net_QTY From Orders A
join Orders B
on A.order_id = B.order_id
where A.Delivered_StoreID <> B.Delivered_StoreID 
) as T
Inner Join productsinfo C
on T.product_id = C.product_id
inner join orderpayments as D
on T.order_id = D.order_id
inner Join Customers As E
on T.Customer_id = E.Custid
inner join OrderReview_Ratings F
on T.order_id = F.order_id
inner join ['Stores Info] G
on T.Delivered_StoreID = G.StoreID
Group by T.Customer_id,T.order_id,T.product_id,T.Channel,T.Bill_date_timestamp,T.[Cost Per Unit],T.Delivered_StoreID,
T.Discount,T.MRP,T.[Total Amount],T.Quantity,T.Net_amount,T.Net_QTY,C.Category,F.Customer_Satisfaction_Score,
G.seller_city,G.seller_state,G.Region,E.customer_city,E.customer_state,E.Gender) 
) AS x


------------ Creating the Table and storing the above Code output to Add_records table------------

Select * into Add_records from (
Select T.Customer_id,T.order_id,T.product_id,T.Channel,T.Delivered_StoreID,T.Bill_date_timestamp,Sum(T.Net_QTY)as Quantity,T.[Cost Per Unit],
T.MRP,T.Discount,SUM(Net_amount) as Total_Amount ,C.Category,F.Customer_Satisfaction_Score as Avg_rating,
G.seller_city,G.seller_state,G.Region,E.customer_city,E.customer_state,E.Gender
from (
Select Distinct A.*,(A.[Total Amount]/A.Quantity) as Net_amount, (A.Quantity/A.Quantity) as Net_QTY From Orders A
join Orders B
on A.order_id = B.order_id
where A.Delivered_StoreID <> B.Delivered_StoreID 
) as T
Inner Join productsinfo C
on T.product_id = C.product_id
inner join orderpayments as D
on T.order_id = D.order_id
inner Join Customers As E
on T.Customer_id = E.Custid
inner join OrderReview_Ratings F
on T.order_id = F.order_id
inner join ['Stores Info] G
on T.Delivered_StoreID = G.StoreID
Group by T.Customer_id,T.order_id,T.product_id,T.Channel,T.Bill_date_timestamp,T.[Cost Per Unit],T.Delivered_StoreID,
T.Discount,T.MRP,T.[Total Amount],T.Quantity,T.Net_amount,T.Net_QTY,C.Category,F.Customer_Satisfaction_Score,
G.seller_city,G.seller_state,G.Region,E.customer_city,E.customer_state,E.Gender) a




Select * Into Finalised_Records_1 From (
Select * From Finalised_Records_no
except
---------------Checking whether the records in Add_records table are also available with Integratable_Table _1 
(Select A.* From Add_records A
inner Join Integrated_Table_1 B
on A.order_id = B.order_id) 
) x
----- We found some records thus these needed to be deleted so using the Except function from Finalised Records 
----- And storing the data into new table Finalised_Records_1 
Select * From Finalised_Records_1

Select * from Add_records

---- Example for you all how to use the data set if you want the distinct Order and calculation
Select Distinct order_id, Sum([Total Amount]) From Finalised_Records_1
Group by order_id


--Main Table :
select * from final_table
Select * From Finalised_Records_1

ALTER TABLE Finalised_Records_1
ALTER COLUMN Bill_date_timestamp DATE;

select MIN(Bill_date_timestamp)as min_date,MAX(Bill_date_timestamp)as max_date from Finalised_Records_1


SELECT * into final_table
FROM Finalised_Records_1
WHERE Bill_date_timestamp >= '2021-09-01'
  AND Bill_date_timestamp < '2023-11-01';

--Discrepancy IN Final Table
--To check One order mapped with multiple stores where instore channel --1001 records
Select Customer_id,order_id,Channel,
count(*) as store_id_count from final_table
group by Customer_id,order_id,Channel
having count(*)>1

Select * from final_table
where Customer_id='1111521581'

--To Handle One order mapped with multiple stores where instore channel
WITH store_update AS (
    SELECT Customer_id, Order_id, Channel, MIN(Delivered_StoreID) AS New_StoreID
    FROM final_table
    GROUP BY Customer_id, Order_id, Channel
)
UPDATE o
SET Delivered_StoreID = su.New_StoreID
FROM final_table o
JOIN store_update su
ON o.Customer_id = su.Customer_id 
AND o.Order_id = su.Order_id 
AND o.Channel = su.Channel;

Select * from final_table
where Customer_id='1111521581'

--To check One order mapped with mult bill_date_timestamp -265 records
SELECT Customer_id, order_id, Channel, 
       COUNT(DISTINCT Bill_date_timestamp) AS distinct_timestamps
FROM final_table
GROUP BY Customer_id, order_id, Channel
HAVING COUNT(DISTINCT Bill_date_timestamp) > 1;

select * from final_table
where Customer_id='2984566067'

--To handle One order mapped with mult bill_date_timestamp
UPDATE o
SET o.Bill_date_timestamp = o2.recent_timestamp
FROM final_table o
JOIN (
    SELECT Customer_id, order_id, Channel, MAX(Bill_date_timestamp) AS recent_timestamp
    FROM final_table
    GROUP BY Customer_id, order_id, Channel
) o2
ON o.Customer_id = o2.Customer_id 
AND o.order_id = o2.order_id 
AND o.Channel = o2.Channel;

select * from final_table
where Customer_id='2984566067'

--There are around 1442 #N/A record in Category Column
Select * from final_table
where Category ='#N/A'

update final_table
set Category='Others'
where Category='#N/A'

-------------------------------------------------------------------------------------------------------------

-- Need to create customer 360, order 360, store 360 tables for further analysis

--STORE360

Select * from final_table

select * into store_360
from(
SELECT 
  Delivered_StoreID, 
  s.seller_city,
  COUNT(order_id) AS no_of_items,
  SUM(Quantity) AS no_quantity,
  SUM(Quantity * MRP) AS amount,
  SUM(Discount) AS total_disc,
  MAX(CASE 
        WHEN Discount > 0 THEN 1 
        ELSE 0 
      END) AS is_discounted,
  SUM(Quantity * [Cost Per Unit]) AS Total_cost,
  SUM(Quantity * MRP) - SUM(Quantity * [Cost Per Unit]) AS total_profit,
  CASE 
    WHEN SUM(Quantity * MRP) - SUM(Quantity * [Cost Per Unit]) < 0 THEN 1 
    ELSE 0 
  END AS is_loss_making,
  COUNT(DISTINCT Category) AS distinct_category,
    MAX(CASE 
    WHEN DATENAME(WEEKDAY, Bill_date_timestamp) IN ('Saturday', 'Sunday') THEN 1
    ELSE 0
  END) AS is_weekend,
  SUM(CASE 
     WHEN DATENAME(WEEKDAY, Bill_date_timestamp) IN ('Saturday', 'Sunday') 
     THEN [Total Amount] 
     ELSE 0 
   END) AS weekend_sale,
   SUM(CASE 
     WHEN DATENAME(WEEKDAY, Bill_date_timestamp) Not IN ('Saturday', 'Sunday') 
     THEN [Total Amount] 
     ELSE 0 
   END) AS weekday_sale,
  AVG([Total Amount]) AS avg_ord_value,
  SUM((MRP - Discount - [Cost Per Unit]) * Quantity) / COUNT(DISTINCT order_id) AS average_profit_per_transaction,
  (SUM(Quantity * MRP) - SUM(Quantity * [Cost Per Unit])) / NULLIF(COUNT(DISTINCT Customer_id), 0) AS avg_profit_per_customer,
  COUNT(order_id) * 1.0 / NULLIF(COUNT(DISTINCT Customer_id), 0) AS avg_customer_visits_per_store
FROM 
  final_table f
  inner join ['Stores Info] s
  on f.Delivered_StoreID=s.StoreID
  and f.seller_city=s.seller_city
GROUP BY 
  Delivered_StoreID, s.seller_city
) As X

select * from store_360
select * from ['Stores Info]

--ORDER 360
Select Distinct(Category) from final_table

select * into order360 
from(
SELECT 
  order_id,
  COUNT(order_id) AS no_of_items,
  SUM(quantity) AS total_quantity,
  round(sum(ISNULL([Total Amount], 0)), 2) as Total_Amount,
  --SUM(Quantity * MRP) AS amount,
  SUM(Discount) AS total_discount,
  SUM(Quantity * [Cost Per Unit]) AS Total_cost,
  COUNT(DISTINCT Category) AS dist_category,
  SUM(Quantity * MRP) - SUM(Quantity * [Cost Per Unit]) AS total_profit,
  CASE 
    WHEN SUM(quantity * MRP) - SUM(quantity * [Cost Per Unit]) < 0 THEN 1
    ELSE 0
  END AS is_loss_making,

  MAX(CASE 
    WHEN discount > 0 THEN 1
    ELSE 0
  END) AS is_discounted,

  MAX(CASE 
    WHEN DATENAME(WEEKDAY, Bill_date_timestamp) IN ('Saturday', 'Sunday') THEN 1
    ELSE 0
  END) AS is_weekend,

  CASE 
    WHEN SUM(quantity * MRP) - SUM(quantity * [Cost Per Unit]) > 100 THEN 1
    ELSE 0
  END AS is_high_profit

FROM final_table
GROUP BY order_id
) AS X

select * from order360



---CUSTOMER 360

select distinct(channel) from final_table
select * from Customers
select distinct(payment_type) from OrderPayments
/*
Select * into customer_360
from(
SELECT 
  f.Customer_id,
  c.customer_city,
  c.Gender,
  c.customer_state,

  -- Transaction Timeline
  MIN(f.Bill_date_timestamp) AS first_transact,
  MAX(f.Bill_date_timestamp) AS last_transact,
  DATEDIFF(DAY, MIN(f.Bill_date_timestamp), MAX(f.Bill_date_timestamp)) AS tenure_in_days,

  -- Overall Spend and Quantity
  SUM(f.[Total Amount]) AS total_amount,
  SUM(f.Discount) AS total_discount,
  SUM(f.Quantity) AS total_quantity,

  -- Overall Items and Category
  COUNT(DISTINCT f.product_id) AS distinct_item,
  COUNT(DISTINCT f.Category) AS distinct_category,

  -- Overall Discounts and Channels
  COUNT(CASE WHEN f.Discount > 0 THEN 1 END) AS trans_discount,
  COUNT(DISTINCT f.Channel) AS channel_used,

  -- Store & Location Diversity
  COUNT(DISTINCT f.Delivered_StoreID) AS distinct_storeid,
  COUNT(DISTINCT f.seller_city) AS distinct_purchase_cities,

  -- Payment Methods
  COUNT(DISTINCT op.payment_type) AS distinct_payment_types,
  COUNT(CASE WHEN op.payment_type = 'credit_card' THEN 1 END) AS credit_card_txns,
  COUNT(CASE WHEN op.payment_type = 'debit_card' THEN 1 END) AS debit_card_txns,
  COUNT(CASE WHEN op.payment_type = 'upi' THEN 1 END) AS upi_txns,
  COUNT(CASE WHEN op.payment_type = 'voucher' THEN 1 END) AS voucher_txns,

  -- =====================
  -- 📌 In-Store Metrics
  -- =====================

  -- In-store Frequency
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' THEN f.order_id END) AS instore_txns,

  -- In-store Revenue
  SUM(CASE WHEN f.Channel = 'Instore' THEN f.[Total Amount] END) AS instore_revenue,

  -- In-store Profit
  SUM(CASE WHEN f.Channel = 'Instore' THEN (f.MRP - f.[Cost Per Unit] - f.Discount) * f.Quantity END) AS instore_profit,

  -- In-store Discount
  SUM(CASE WHEN f.Channel = 'Instore' THEN f.Discount END) AS instore_discount,

  -- In-store Quantity
  SUM(CASE WHEN f.Channel = 'Instore' THEN f.Quantity END) AS instore_quantity,

  -- In-store Item Diversity
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' THEN f.product_id END) AS instore_distinct_items,

  -- In-store Category Diversity
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' THEN f.Category END) AS instore_distinct_categories,

  -- In-store Transactions with Discount
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' AND f.Discount > 0 THEN f.order_id END) AS instore_txns_with_discount,

  -- In-store Transactions with Loss
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' AND (f.MRP - f.[Cost Per Unit] - f.Discount) < 0 THEN f.order_id END) AS instore_txns_with_loss,

  -- In-store Payment Methods
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' THEN op.payment_type END) AS instore_distinct_payment_types,
  COUNT(CASE WHEN f.Channel = 'Instore' AND op.payment_type = 'credit_card' THEN 1 END) AS instore_credit_txns,
  COUNT(CASE WHEN f.Channel = 'Instore' AND op.payment_type = 'debit_card' THEN 1 END) AS instore_debit_txns,
  COUNT(CASE WHEN f.Channel = 'Instore' AND op.payment_type = 'upi' THEN 1 END) AS instore_upi_txns,
  COUNT(CASE WHEN f.Channel = 'Instore' AND op.payment_type = 'voucher' THEN 1 END) AS instore_voucher_txns,

  -- In-store Preferred Payment Method (as subquery)
  (
    SELECT TOP 1 op_inner.payment_type
    FROM final_table f_inner
    JOIN OrderPayments op_inner ON f_inner.order_id = op_inner.order_id
    WHERE f_inner.Channel = 'Instore' AND f_inner.Customer_id = f.Customer_id
    GROUP BY op_inner.payment_type
    ORDER BY COUNT(*) DESC
  ) AS instore_preferred_payment,

  -- =====================
  -- 📌 Online Metrics
  -- =====================

  -- In-store Frequency
  COUNT(DISTINCT CASE WHEN f.Channel = 'Online' THEN f.order_id END) AS Online_txns,

  -- In-store Revenue
  SUM(CASE WHEN f.Channel = 'Online' THEN f.[Total Amount] END) AS Online_revenue,

  -- In-store Profit
  SUM(CASE WHEN f.Channel = 'Online' THEN (f.MRP - f.[Cost Per Unit] - f.Discount) * f.Quantity END) AS Online_profit,

  -- In-store Discount
  SUM(CASE WHEN f.Channel = 'Online' THEN f.Discount END) AS Online_discount,

  -- In-store Quantity
  SUM(CASE WHEN f.Channel = 'Online' THEN f.Quantity END) AS Online_quantity,

  -- In-store Item Diversity
  COUNT(DISTINCT CASE WHEN f.Channel = 'Online' THEN f.product_id END) AS Online_distinct_items,

  -- In-store Category Diversity
  COUNT(DISTINCT CASE WHEN f.Channel = 'Online' THEN f.Category END) AS Online_distinct_categories,

  -- In-store Transactions with Discount
  COUNT(DISTINCT CASE WHEN f.Channel = 'Online' AND f.Discount > 0 THEN f.order_id END) AS Online_txns_with_discount,

  -- In-store Transactions with Loss
  COUNT(DISTINCT CASE WHEN f.Channel = 'Instore' AND (f.MRP - f.[Cost Per Unit] - f.Discount) < 0 THEN f.order_id END) AS Online_txns_with_loss,

  -- In-store Payment Methods
  COUNT(DISTINCT CASE WHEN f.Channel = 'Online' THEN op.payment_type END) AS Online_distinct_payment_types,
  COUNT(CASE WHEN f.Channel = 'Online' AND op.payment_type = 'credit_card' THEN 1 END) AS Online_credit_txns,
  COUNT(CASE WHEN f.Channel = 'Online' AND op.payment_type = 'debit_card' THEN 1 END) AS Online_debit_txns,
  COUNT(CASE WHEN f.Channel = 'Online' AND op.payment_type = 'Upi/Cash' THEN 1 END) AS Online_upi_txns,
  COUNT(CASE WHEN f.Channel = 'Online' AND op.payment_type = 'voucher' THEN 1 END) AS Online_voucher_txns,

  -- In-store Preferred Payment Method (as subquery)
  (
    SELECT TOP 1 op_inner.payment_type
    FROM final_table f_inner
    JOIN OrderPayments op_inner ON f_inner.order_id = op_inner.order_id
    WHERE f_inner.Channel = 'Online' AND f_inner.Customer_id = f.Customer_id
    GROUP BY op_inner.payment_type
    ORDER BY COUNT(*) DESC
  ) AS Online_preferred_payment,

  -- =====================
  -- 📌 Phone Delivery Metrics
  -- =====================

  -- In-store Frequency
  COUNT(DISTINCT CASE WHEN f.Channel = 'Phone Delivery' THEN f.order_id END) AS PhoneDelivery_txns,

  -- In-store Revenue
  SUM(CASE WHEN f.Channel = 'Phone Delivery' THEN f.[Total Amount] END) AS PhoneDelivery_revenue,

  -- In-store Profit
  SUM(CASE WHEN f.Channel = 'Phone Delivery' THEN (f.MRP - f.[Cost Per Unit] - f.Discount) * f.Quantity END) AS PhoneDelivery_profit,

  -- In-store Discount
  SUM(CASE WHEN f.Channel = 'Phone Delivery' THEN f.Discount END) AS PhoneDelivery_discount,

  -- In-store Quantity
  SUM(CASE WHEN f.Channel = 'Phone Delivery' THEN f.Quantity END) AS PhoneDelivery_quantity,

  -- In-store Item Diversity
  COUNT(DISTINCT CASE WHEN f.Channel = 'Phone Delivery' THEN f.product_id END) AS PhoneDelivery_distinct_items,

  -- In-store Category Diversity
  COUNT(DISTINCT CASE WHEN f.Channel = 'Phone Delivery' THEN f.Category END) AS PhoneDelivery_distinct_categories,

  -- In-store Transactions with Discount
  COUNT(DISTINCT CASE WHEN f.Channel = 'Phone Delivery' AND f.Discount > 0 THEN f.order_id END) AS PhoneDelivery_txns_with_discount,

  -- In-store Transactions with Loss
  COUNT(DISTINCT CASE WHEN f.Channel = 'Phone Delivery' AND (f.MRP - f.[Cost Per Unit] - f.Discount) < 0 THEN f.order_id END) AS PhoneDelivery_txns_with_loss,

  -- In-store Payment Methods
  COUNT(DISTINCT CASE WHEN f.Channel = 'Phone Delivery' THEN op.payment_type END) AS PhoneDelivery_distinct_payment_types,
  COUNT(CASE WHEN f.Channel = 'Phone Delivery' AND op.payment_type = 'credit_card' THEN 1 END) AS PhoneDelivery_credit_txns,
  COUNT(CASE WHEN f.Channel = 'Phone Delivery' AND op.payment_type = 'debit_card' THEN 1 END) AS PhoneDelivery_debit_txns,
  COUNT(CASE WHEN f.Channel = 'Phone Delivery' AND op.payment_type = 'Upi/Cash' THEN 1 END) AS PhoneDelivery_upi_txns,
  COUNT(CASE WHEN f.Channel = 'Phone Delivery' AND op.payment_type = 'voucher' THEN 1 END) AS PhoneDelivery_voucher_txns,

  -- In-store Preferred Payment Method (as subquery)
  (
    SELECT TOP 1 op_inner.payment_type
    FROM final_table f_inner
    JOIN OrderPayments op_inner ON f_inner.order_id = op_inner.order_id
    WHERE f_inner.Channel = 'Phone Delivery' AND f_inner.Customer_id = f.Customer_id
    GROUP BY op_inner.payment_type
    ORDER BY COUNT(*) DESC
  ) AS PhoneDelivery_preferred_payment

  
FROM 
  Customers c
JOIN 
  final_table f ON c.Custid = f.Customer_id
                AND c.customer_city = f.customer_city
                AND c.customer_state = f.customer_state
                AND c.Gender = f.Gender
LEFT JOIN 
  OrderPayments op ON op.order_id = f.order_id

GROUP BY 
  f.Customer_id, c.customer_city, c.Gender, c.customer_state
) AS X

Select * from customer360
*/

WITH Payments_Agg AS (
    SELECT 
        order_id,
        COUNT(DISTINCT payment_type) AS Distinct_Payment_Types,
        COUNT(CASE WHEN payment_type = 'Voucher' THEN 1 END) AS Voucher_Payments,
        COUNT(CASE WHEN payment_type = 'Credit_Card' THEN 1 END) AS Credit_Payments,
        COUNT(CASE WHEN payment_type = 'Debit_Card' THEN 1 END) AS Debit_Payments,
        COUNT(CASE WHEN payment_type = 'UPI/Cash' THEN 1 END) AS UPI_CASH_Payments
    FROM orderpayments
    GROUP BY order_id
)


select * into customer360 from(SELECT 
    A.Customer_id,
    customer_city,
    customer_state,
    Gender,
    MIN(Bill_date_timestamp) AS First_Transaction_Date,
    MAX(Bill_date_timestamp) AS Last_Transaction_Date,
    DATEDIFF(Day, MIN(Bill_date_timestamp), MAX(Bill_date_timestamp)) AS Tenure,
    DATEDIFF(DAY, MAX(Bill_date_timestamp), (SELECT MAX(Bill_date_timestamp) FROM Finalised_Records_1)) AS Inactive_Days,
    COUNT(DISTINCT A.order_id) AS Frequency,
    SUM([Total Amount]) AS Total_expenditure,
    SUM(Discount) AS Total_Discount,
    SUM(Quantity) AS Total_Quantity,
    COUNT(DISTINCT product_id) AS Distinct_Items,
    COUNT(DISTINCT Category) AS Distinct_Categories,
    COUNT(CASE WHEN Discount > 0 THEN 1 END) AS Transactions_With_Discount,
    COUNT(Channel) AS Channels_used,
    COUNT(DISTINCT Delivered_StoreID) AS Distinct_Stores,
    COUNT(DISTINCT seller_city) AS Distinct_Cities,
    COUNT(CASE WHEN Channel = 'Instore' THEN 1 END) AS Instore_Transactions,
    COUNT(CASE WHEN Channel = 'Online' THEN 1 END) AS Online_Transactions,
    COUNT(CASE WHEN Channel = 'Phone Delivery' THEN 1 END) AS Calling_Transactions,
    SUM(P.Distinct_Payment_Types) AS Distinct_Payment_Types,
    SUM(P.Voucher_Payments) AS Voucher_Payments,
    SUM(P.Credit_Payments) AS Credit_Payments,
    SUM(P.Debit_Payments) AS Debit_Payments,
    SUM(P.UPI_CASH_Payments) AS UPI_CASH_Payments,
    COUNT(CASE WHEN DATENAME(WEEKDAY, Bill_date_timestamp) IN ('Saturday', 'Sunday') THEN 1 END) AS Weekend_Transactions,
    COUNT(CASE WHEN DATENAME(WEEKDAY, Bill_date_timestamp) NOT IN ('Saturday', 'Sunday') THEN 1 END) AS Weekday_Transactions
FROM final_table AS A
left JOIN Payments_Agg AS P ON A.order_id = P.order_id
GROUP BY 
    A.Customer_id, customer_city, customer_state, Gender) as A




--High Level Metrics
--NO of Orders
Select Count(Distinct order_id) as order_count from final_table

--Total  Discount
Select Sum(Discount)as total_Discount from final_table

--Average Discount per customer
Select Round(Sum(Discount)/count(Distinct Customer_id),2) as avg_disc_per_Cust from final_table

--Average Discount per order
Select Round(Sum(Discount)/count(Distinct order_id),2) as avg_disc_per_order from final_table

--Average Profit per customer
SELECT Round((SUM([Total Amount]) - SUM([Cost Per Unit] * [Quantity]))/count(Distinct Customer_id),2) AS avg_prof_per_Cust
FROM final_table;

--Average Order Value
select Round(SUM(Total_Amount)/count(order_id),2) As Average_Order_Val from order360

--Average Sales per Customer
select Round(SUM(Total_expenditure)/count(Customer_id),2) As Average_Order_Val from customer360

--Transaction per Customer
Select count(Distinct order_id)*1.0/count(Distinct Customer_id)*1.0 as transaction_per_cust
from final_table

--Average No of Categories per Order
WITH order_category_counts AS (
    SELECT Order_id, COUNT(DISTINCT Category) AS category_per_order
    FROM final_table
    GROUP BY Order_id
)
SELECT AVG(category_per_order * 1.0) AS avg_categories_per_order
FROM order_category_counts;

--average number of items per order
Select Avg(Product_per_order*1.0) As avg_Product_per_order from(
Select order_id,count(Distinct product_id)AS Product_per_order from final_table
group by order_id
) As P

--Total Revenue
select Sum([Total Amount])as Total_revenue from final_table
Select Sum(Total_expenditure) as Total_revenue from customer360
Select Sum(Total_Amount) as Total_revenue from order360


--Total Cost
select sum([Cost Per Unit]*Quantity)as Total_cost from final_table

--Total Category
Select count( Distinct Category) as Category_count from final_table

--Total Quantity
Select  sum(Quantity)as Total_Quantity from final_table

--Total Store
Select count(Distinct Delivered_StoreID) as store_count from final_table

--Total Channel
select count( Distinct Channel)as channel_count from final_table

--No of Customer
select count(Customer_id)as customer_count from customer360

select * from final_table
Select * from customer360
Select * from order360
Select * from store_360

--Total product
Select count(Distinct product_id) as product_count from final_table

--Total Region
Select count(Distinct Region) as total_region from final_table

--Total Payment Method
Select  count(Distinct payment_type)as payment_type from OrderPayments
--Select count(Distinct(instore_preferred_payment)) as total_payment_method from customer360
--where instore_preferred_payment is not null

--Total Location
Select count(Distinct customer_city)as customer_loc from final_table

--Total Profit
SELECT SUM([Total Amount]) - SUM([Cost Per Unit] * [Quantity]) AS Total_Profit
FROM final_table;

--Total Profit Percentage
Select  Round((SUM([Total Amount]) - SUM([Cost Per Unit] * [Quantity]))/SUM([Cost Per Unit]* [Quantity])*100,2) as profit_percentage 
from final_table

--Total Discount Percentage
Select Round(sum(Discount)/sum([Total Amount])*100,2) as Discount_percent 
from final_table

--Average No of Days between two transaction(if the customer has more than one transaction)
WITH ranked_txns AS (
select Customer_id,Bill_date_timestamp,
ROW_NUMBER() over(partition by customer_id order by Bill_date_timestamp) as row_no 
from final_table
),
diff As(
 SELECT 
        t1.Customer_id,
        DATEDIFF(DAY, t1.Bill_date_timestamp, t2.Bill_date_timestamp) AS days_between
    FROM ranked_txns t1
    JOIN ranked_txns t2 
        ON t1.Customer_id = t2.Customer_id 
        AND t1.row_no = t2.row_no - 1
),
final_avg AS (
    SELECT Customer_id, AVG(days_between * 1.0) AS avg_days_per_customer
    FROM diff
    GROUP BY Customer_id
)
SELECT AVG(avg_days_per_customer) AS avg_days_between_transactions
FROM final_avg;

--Repeat Customer Percentage
Select (count(*)*1.0/(Select count(Customer_id) from customer360)*1.0) *100 as repeat_cust_Pert from( 
Select Customer_id,count(*) as cust_count from final_table
group by Customer_id
having count(*)>1
) As P

--Repeat Transaction Rate
select (Sum(cust_count)*1.0/(select count(order_id) from order360)*1.0)*100 as repeat_tran_rate from (
Select Customer_id,count(*)as cust_count from  final_table
group by Customer_id
having count(*)>1
) As T

--One time Buyer percentage
Select Round((count(*)*1.0/(Select count(*) from customer360)*1.0)*100,2) as one_time_buyer from(
Select Customer_id,count(*)as cust_count from  final_table
group by Customer_id
having count(*)=1
) As U