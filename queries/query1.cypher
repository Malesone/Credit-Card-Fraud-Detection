1. For each customer identifies the amount that he/she has spent for every day of the current
month.

MATCH (c:Customer)-[:make]->(t:Transaction)
WHERE datetime({date:t.date}) >= datetime('2022-1-1') and datetime({date:t.date}) <= datetime('2022-1-31')
RETURN c.name, t.date, sum(t.amount)

2. For each terminal identify the possible fraudulent transactions. The fraudulent transactions
are those whose import is higher than 50% of the average import of the transactions
executed on the same terminal in the last month.


