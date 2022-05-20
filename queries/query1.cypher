1. For each customer identifies the amount that he/she has spent for every day of the current
month.

MATCH (c:Customer)-[:make]->(t:Transaction)
WHERE datetime({date:t.date}) >= datetime('2022-1-1') and datetime({date:t.date}) <= datetime('2022-1-31')
RETURN c.name, t.date, sum(t.amount)

2. For each terminal identify the possible fraudulent transactions. The fraudulent transactions
are those whose import is higher than 50% of the average import of the transactions
executed on the same terminal in the last month.

match (t:Transaction)<-[:from]-(tr:Terminal) 
with tr.name as name, avg(t.amount) as avg_amount, date.truncate('month', t.date) as month
match (trr:Terminal)-[:from]->(t1:Transaction)
where t1.amount > avg_amount/2 and date.truncate('month', t1.date) = month and trr.name = name 
return t1

3. 
match (c:Customer {name: 1})-[:connect]-(t:Terminal)-[:connect *6]-(c1:Customer)
where c <> c1
return c1