USE Retail_Project;
Select * from final_table
Select * from customer360
Select * from order360
Select * from store_360

--Exploratory Analysis
---Top 10 perfrming Store in terms of Sales
SELECT TOP 10 
    Delivered_StoreID,
    ROUND(SUM([Total Amount]) / 1000000.0, 3) AS revenue_in_million
FROM 
    final_table
GROUP BY 
    Delivered_StoreID
ORDER BY 
    revenue_in_million DESC;


---Top 10 worst perfrming Store in terms of Sales
Select top 10 Delivered_StoreID,ROUND(SUM([Total Amount]) / 1000000.0, 3) AS revenue_in_million from final_table
group by Delivered_StoreID
order by revenue_in_million asc

--List the top 10 most expensive products sorted by price and their contribution to sales

--Select * from final_table
--where product_id='0c761b0e33d708e6a3694a94ac4b82c4'


WITH ProductSales AS (
    SELECT 
        product_id, 
        MRP, 
        SUM([Total Amount]) AS revenue
    FROM final_table
    GROUP BY product_id, MRP
),
TotalRevenue AS (
    SELECT SUM(revenue) AS total_revenue FROM ProductSales
)
SELECT TOP 10 
    p.product_id,
    p.revenue,
    Round((p.revenue * 1.0 / t.total_revenue*1.0)*100,3)  AS contribution_percent
FROM 
    ProductSales p, TotalRevenue t
ORDER BY 
    p.MRP DESC,contribution_percent desc;



--Sales By region
Select Region, Round(SUM([Total Amount]) / 1000000,2) AS Sales_in_Million
from final_table
group by Region
order by Sales_in_Million desc

--Sales By Category
SELECT 
  Category, 
  Round(SUM([Total Amount]) / 1000000,2) AS Sales_in_Million
FROM 
  final_table
GROUP BY 
  Category
ORDER BY 
  Sales_in_Million DESC;

--Sales By State
SELECT 
 seller_state, 
  Round(SUM([Total Amount])/100000,1)  AS Sales
FROM 
  final_table
GROUP BY 
  seller_state
ORDER BY 
  Sales DESC;

--Sales By channel
SELECT 
  Channel, 
  Round(SUM([Total Amount]) / 1000000,2) AS Sales_in_Million,
  Round(Sum(Quantity)/1000,2) as quantity_in_Thousands
FROM 
  final_table
GROUP BY 
  Channel
ORDER BY 
  Sales_in_Million DESC;

---Count of New Customer Acquired Every Month


WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(CAST(Bill_date_timestamp AS DATE)) AS first_purchase_date
    FROM Finalised_Records_1
    GROUP BY customer_id
)

SELECT 
    
    MONTH(first_purchase_date) AS order_month,
    DATENAME(MONTH, first_purchase_date) AS month_name,
    COUNT(DISTINCT customer_id) AS new_customers
FROM first_orders
GROUP BY 
  
    MONTH(first_purchase_date),
    DATENAME(MONTH, first_purchase_date)
ORDER BY 
   
    MONTH(first_purchase_date);

 ---popular category by Region

with Ranked_Categ AS(
    SELECT
        Region,
        Category,
        COUNT(*) AS category_count,
       ROW_NUMBER() OVER (PARTITION BY Region ORDER BY COUNT(*) DESC) AS rn
    FROM
        final_table
    GROUP BY
        Region, Category
)
Select Region, category,category_count from Ranked_Categ 
where rn<=5

 ---popular category by State
WITH category_counts AS (
    SELECT 
        seller_state,
        Category,
        COUNT(*) AS category_count
    FROM final_table
    GROUP BY seller_state, Category
),
Ranked_Categ AS (
    SELECT
        seller_state,
        Category,
        category_count,
        ROW_NUMBER() OVER (PARTITION BY seller_state ORDER BY category_count DESC) AS rn
    FROM category_counts
)
SELECT 
    seller_state,
    Category,
    category_count
FROM Ranked_Categ 
WHERE rn <= 5
ORDER BY seller_state, rn;



--Cross Selling Product

SELECT 
    p1.category AS category_1,
    p2.category AS category_2,
    p3.category AS category_3,
    COUNT(DISTINCT p1.order_id) AS combo_count
FROM final_table p1
JOIN final_table p2 
    ON p1.order_id = p2.order_id 
    AND p1.category < p2.category
JOIN final_table p3 
    ON p1.order_id = p3.order_id 
    AND p2.category < p3.category
GROUP BY p1.category, p2.category, p3.category
ORDER BY combo_count DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;


--Customer Behavior

select count(Distinct customer_state) from final_table;

--No of Customer in each States
Select customer_state,count(*) as cust_count from final_table 
Group by customer_state
order by cust_count desc;

--CUSTOMER COUNT BASED ON GENDER
select Gender,count(*) as No_customer,ROund(Sum([Total Amount])/1000000,2) as total_sales_millions,
Concat(ROund(sum([Total Amount])/(select sum([Total Amount]) from final_table)*100,2) ,'%') as sales_percentage 
from final_table
Group by Gender;

--One time buyer customer count in each state
Select customer_state,count(*) as one_time_buyercount from final_table
where Customer_id in(
Select customer_id from final_table
 group by customer_id
 having count(*)=1
)
group by customer_state
order by one_time_buyercount desc



--One time buyer customer count in each region
Select Region,count(*) as one_time_buyercount from final_table
where Customer_id in(
Select customer_id from final_table
 group by customer_id
 having count(*)=1
)
group by Region
order by one_time_buyercount desc

--One time buyer customer count in each region
Select Delivered_StoreID,count(*) as one_time_buyercount from final_table
where Customer_id in(
Select customer_id from final_table
 group by customer_id
 having count(*)=1
)
group by Delivered_StoreID
order by one_time_buyercount desc

--Repeat buyer customer count in each state
Select customer_state,count(*)as customer_count from final_table
where Customer_id in(
Select Customer_id  from final_table
group by Customer_id
having count(*)>1
)
group by customer_state
order by customer_count desc

--Repeat buyer customer count in each store
Select Delivered_StoreID,count(*)as customer_count from final_table
where Customer_id in(
Select Customer_id  from final_table
group by Customer_id
having count(*)>1
)
group by Delivered_StoreID
order by customer_count desc

--Repeat buyer customer count in each region
Select Region,count(*)as customer_count from final_table
where Customer_id in(
Select Customer_id  from final_table
group by Customer_id
having count(*)>1
)
group by Region
order by customer_count desc

--DISCOUNT SEEKER COUNT VS NON DISCOUNT SEEKER Sales
Select 
Round(Sum(case when Discount>0 then [Total Amount] else 0 End)/1000000,2) As Discount_Seeker_sales_Million,
Round(Sum(case when Discount=0 then [Total Amount] else 0 End)/1000000,2) As Non_Discount_Seeker_sales_Million
from final_table;

--DISCOUNT SEEKER COUNT VS NON DISCOUNT SEEKER Count
SELECT 
  COUNT(CASE WHEN Discount > 0 THEN order_id ELSE NULL END) AS Discount_Seeker_Count,
  COUNT(CASE WHEN Discount = 0 THEN order_id ELSE NULL END) AS Non_Discount_Seeker_Count
FROM final_table;

--NO of Discount Seeker in each store
Select Delivered_StoreID,count(*) as customer_count from final_table 
where Customer_id in(
Select Customer_id from final_table
where Discount>0
)
group by Delivered_StoreID
order by customer_count desc

--NO of Discount Seeker in each state
Select seller_state,count(*) as customer_count from final_table 
where Customer_id in(
Select Customer_id from final_table
where Discount>0
)
group by seller_state
order by customer_count desc

--NO of Discount Seeker in each region
Select Region,count(*) as customer_count from final_table 
where Customer_id in(
Select Customer_id from final_table
where Discount>0
)
group by Region
order by customer_count desc

--NO of Non Discount Seeker in each store
Select Delivered_StoreID,count(*) as customer_count from final_table 
where Customer_id in(
Select Customer_id from final_table
where Discount=0
)
group by Delivered_StoreID
order by customer_count desc

--NO of Non-Discount Seeker in each state
Select seller_state,count(*) as customer_count from final_table 
where Customer_id in(
Select Customer_id from final_table
where Discount=0
)
group by seller_state
order by customer_count desc

--NO of Non-Discount Seeker in each region
Select Region,count(*) as customer_count from final_table 
where Customer_id in(
Select Customer_id from final_table
where Discount=0
)
group by Region
order by customer_count desc

--Segment the customers (divide the customers into groups) based on the revenue
With Rev_percent AS(
SELECT *,
    PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY [Total Amount]) OVER () AS percentile_33,
    PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY [Total Amount]) OVER () AS percentile_66,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY [Total Amount]) OVER () AS percentile_99
FROM
   final_table
),
Cust_segment AS(
Select Customer_id,sum([Total Amount]) as revenue,
case  when sum([Total Amount])<Min(percentile_33) then 'low_revenue'
      WHEN SUM([Total Amount]) BETWEEN MIN(percentile_33) AND MIN(percentile_66) THEN 'medium_revenue'
      WHEN SUM([Total Amount]) BETWEEN MIN(percentile_66) AND MIN(percentile_99) THEN 'high_revenue'
      ELSE 'very_high_revenue'
end as customer_segment
from Rev_percent
group by Customer_id
),
Customer_count AS(
Select customer_segment,count(*) as customer_count from Cust_segment
group by customer_segment
)
Select *,Concat(ROund(customer_count*1.0/sum(customer_count) over()*100,2),'%') as percentage from Customer_count
order by customer_count desc



---RFM Segmentation
-- Step 1: Create RFM metrics
WITH RFM AS (
    SELECT 
        Customer_id,
        MAX(Bill_date_timestamp) AS LastPurchaseDate,
        COUNT(*) AS Frequency,
        SUM([Total Amount]) AS Monetary
    FROM final_table
    GROUP BY Customer_id
),
RecencyCalc AS (
    SELECT 
        Customer_id,
        DATEDIFF(DAY, LastPurchaseDate, (SELECT MAX(Bill_date_timestamp) FROM final_table)) AS Recency,
        Frequency,
        Monetary
    FROM RFM
),
RFM_Scores AS (
    SELECT 
        Customer_id,
        NTILE(4) OVER (ORDER BY Recency ASC) AS R_Score,
        NTILE(4) OVER (ORDER BY Frequency DESC) AS F_Score,
        NTILE(4) OVER (ORDER BY Monetary DESC) AS M_Score
    FROM RecencyCalc
),
FinalRFM AS (
    SELECT 
        Customer_id,
        CONCAT(R_Score, F_Score, M_Score) AS RFM_Score,
        CASE 
            WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Premium'
            WHEN R_Score >= 2 AND F_Score >= 2 AND M_Score >= 2 THEN 'Gold'
            WHEN R_Score = 2 AND (F_Score >= 1 OR M_Score >= 1) THEN 'Silver'
            ELSE 'Standard'
        END AS Segment
    FROM RFM_Scores
)

--  Final step: Calculate revenue by Segment
SELECT 
    F.Segment,
    Round(SUM(T.[Total Amount])/1000000,2) AS Total_Revenue_millions,
    COUNT(DISTINCT T.Customer_id)/1000 AS No_of_Customers_lakhs
FROM
    final_table T
INNER JOIN 
    FinalRFM F
ON 
    T.Customer_id = F.Customer_id
GROUP BY 
    F.Segment
ORDER BY 
    Total_Revenue_millions DESC;


--Understanding Category Behavior
--Most profitable category and its contribution

SELECT 
    category,
    categ_profit_Lakhs,
    Concat(ROUND(categ_profit_Lakhs * 1.0 / SUM(categ_profit_Lakhs) OVER ()*100, 3),'%') AS profit_contribution
FROM (
    SELECT 
        Category, 
        ROUND(SUM([Total Amount] - ([Cost Per Unit] * Quantity))/100000, 2) AS categ_profit_Lakhs
    FROM final_table
    GROUP BY Category
) AS X
order by categ_profit_Lakhs desc;

--Total Sales & Percentage of sales by category (Perform Pareto Analysis)

WITH Total_CategSales AS (
    SELECT 
        Category,
        ROUND(SUM([Total Amount]), 2) AS total_sales
    FROM final_table
    GROUP BY Category
),
Cumm_Sales AS (
    SELECT 
        *,
        SUM(total_sales) OVER (ORDER BY total_sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cummutative_sales
    FROM Total_CategSales
),
Cumm_per As(
Select *,ROUND(cummutative_sales * 1.0 / SUM(total_sales) OVER ()*100, 4) as cummutative_per from Cumm_Sales
)
SELECT 
    Category,
    ROUND(total_sales / 1000000.0, 2) AS Sales_Million,
    ROUND(cummutative_sales / 1000000.0, 2) AS Cumulative_Sales_Million,
    CONCAT(ROUND(cummutative_per, 0), '%') AS [CUM% of Sales]
FROM Cumm_per;

-- Sales contribution of Each Category
Select Category,ROund(sales/1000000,1)as Sales_million,Concat(Round(sales/(select sum([Total Amount]) from final_table)*100,1),'%') sales_perc from (
Select Category,sum([Total Amount]) as sales
from final_table
Group by Category
) AS X
Order by sales desc

--Most popular category during first purchase of customer

WITH FirstPurchaseDates AS (
    SELECT 
        Customer_id,
        MIN(Bill_date_timestamp) AS FirstPurchaseDate
    FROM final_table
    GROUP BY Customer_id
)
SELECT 
    f.Category,
    COUNT(*) AS categ_count
FROM final_table f
JOIN FirstPurchaseDates fp
  ON f.Customer_id = fp.Customer_id
 AND f.Bill_date_timestamp = fp.FirstPurchaseDate
GROUP BY f.Category
ORDER BY categ_count DESC;


--Category Penetration(Category Penetration Analysis by month on month (Category Penetration = number of orders containing the category/number of orders))


------CUSTOMER SATISFACTION

--Average Rating By Category
Select Category,Round(Avg(Avg_rating),2) as Avg_rating from final_table
group by Category
order by Avg_rating desc

--Average Rating By Store
Select Delivered_StoreID,Round(Avg(Avg_rating),2) as Avg_rating from final_table
group by Delivered_StoreID
order by Avg_rating desc

--Average Rating By State
Select customer_state,Round(Avg(Avg_rating),2) as Avg_rating from final_table
where customer_state not in ('Goa')
group by customer_state
order by Avg_rating desc

--Average Rating By Month
Select  year(Bill_date_timestamp)as rating_Year,MONTH(Bill_date_timestamp) as rating_Month,Round(Avg(Avg_rating),2) as Avg_rating from final_table
group by year(Bill_date_timestamp),MONTH(Bill_date_timestamp) 
order by year(Bill_date_timestamp),MONTH(Bill_date_timestamp),Avg_rating desc

---SALES TREND

--SALES TREND BY YEAR AND MONTH
SELECT year(Bill_date_timestamp)as year,DATENAME(MONTH,Bill_date_timestamp) AS month_name,Sum([Total Amount]) as revenue FROM final_table
group by DATENAME(MONTH,Bill_date_timestamp),year(Bill_date_timestamp),MONTH(Bill_date_timestamp)
order by year(Bill_date_timestamp),MONTH(Bill_date_timestamp)

--SALES TREND BY Weekdays

select DATENAME(WEEKDAY,Bill_date_timestamp) as weekday,Round(sum([Total Amount])/100000,2) as revenue_Lakhs from final_table
group by DATENAME(WEEKDAY,Bill_date_timestamp)
order by revenue_Lakhs desc
