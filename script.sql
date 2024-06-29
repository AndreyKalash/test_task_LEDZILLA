WITH "Supply_CTE" AS 
(SELECT "LEDZILLA"."SUPPLY"."Product ID" AS "Product ID", "LEDZILLA"."SUPPLY"."Supply QTY" AS "Supply QTY", "LEDZILLA"."SUPPLY"."Costs Per PCS" AS "Costs Per PCS", "LEDZILLA"."SUPPLY"."#Supply" AS "#Supply", row_number() OVER (PARTITION BY "LEDZILLA"."SUPPLY"."Product ID" ORDER BY "LEDZILLA"."SUPPLY"."#Supply") AS "Supply_Rank" 
FROM "LEDZILLA"."SUPPLY"), 
"Sales_CTE" AS 
(SELECT "LEDZILLA"."SALES"."Product ID" AS "Product ID", "LEDZILLA"."SALES"."Sales QTY" AS "Sales QTY", "LEDZILLA"."SALES"."Date" AS "Date", row_number() OVER (PARTITION BY "LEDZILLA"."SALES"."Product ID" ORDER BY "LEDZILLA"."SALES"."Date") AS "Sales_Rank" 
FROM "LEDZILLA"."SALES"), 
"Matched_CTE" AS 
(SELECT "Supply_CTE"."Product ID" AS "Product ID", "Supply_CTE"."Supply QTY" AS "Supply QTY", "Supply_CTE"."Costs Per PCS" AS "Costs Per PCS", "Supply_CTE"."#Supply" AS "#Supply", "Sales_CTE"."Sales QTY" AS "Sales QTY", "Sales_CTE"."Date" AS "Date", "Sales_CTE"."Sales_Rank" AS "Sales_Rank", "Supply_CTE"."Supply_Rank" AS "Supply_Rank", row_number() OVER (PARTITION BY "Supply_CTE"."Product ID" ORDER BY "Supply_CTE"."Product ID", "Sales_CTE"."Date") AS "Match_Rank" 
FROM "Supply_CTE" JOIN "Sales_CTE" ON "Supply_CTE"."Product ID" = "Sales_CTE"."Product ID" 
WHERE "Sales_CTE"."Sales_Rank" >= "Supply_CTE"."Supply_Rank"), 
"FIFO_Costs_CTE" AS 
(SELECT "Matched_CTE"."Product ID" AS "Product ID", "Matched_CTE"."Date" AS "Date", min("Matched_CTE"."#Supply") AS min_1, "Matched_CTE"."Sales QTY" AS "Sales QTY", "Matched_CTE"."Supply QTY" AS "Supply QTY", "Matched_CTE"."Costs Per PCS" AS "Costs Per PCS", "Matched_CTE"."Costs Per PCS" * CASE WHEN ("Matched_CTE"."Sales QTY" <= "Matched_CTE"."Supply QTY") THEN "Matched_CTE"."Sales QTY" ELSE "Matched_CTE"."Supply QTY" END AS "Costs", CASE WHEN ("Matched_CTE"."Sales QTY" <= "Matched_CTE"."Supply QTY") THEN "Matched_CTE"."Sales QTY" ELSE "Matched_CTE"."Supply QTY" END AS "Qty_Sold" 
FROM "Matched_CTE" GROUP BY "Matched_CTE"."Product ID", "Matched_CTE"."Date", "Matched_CTE"."Sales QTY", "Matched_CTE"."Supply QTY", "Matched_CTE"."Costs Per PCS" ORDER BY "Matched_CTE"."Date", min("Matched_CTE"."#Supply"))
 SELECT "FIFO_Costs_CTE"."Product ID" AS "FIFO_Costs_CTE_Product ID", to_char("FIFO_Costs_CTE"."Date", %(to_char_1)s) AS "YearMonth", sum("FIFO_Costs_CTE"."Costs") AS "Total_Costs" 
FROM "FIFO_Costs_CTE" GROUP BY "FIFO_Costs_CTE"."Product ID", to_char("FIFO_Costs_CTE"."Date", %(to_char_2)s) ORDER BY "FIFO_Costs_CTE"."Product ID", to_char("FIFO_Costs_CTE"."Date", %(to_char_3)s)