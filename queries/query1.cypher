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

4.
match (c:Customer)-[m:make]->(t:Transaction)<-[:from]-(tr:Terminal)-[:from]->(t1:Transaction)<-[m1:make]-(c1:Customer)
with count(m) as mc, count(m1) as mc1, t, t1, c, c1
where t.product = t1.product and mc>1 and mc1>1 and c<>c1
create (c)-[:buying_friends]->(c1)
return c, c1


ottengo il conteggio di tutti i tipi di prodotti di un customer
match (t:Transaction)<-[:make]-(c:Customer)-[:make]->(t1:Transaction)
where t.product = t1.product and c.name = 11
return c, count(t1.product), t1.product

MATCH (c: Customer)-[m:make]->(tr: Transaction)<-[f:from]-(t: Terminal)
MATCH (c1: Customer)-[m1:make]->(tr1: Transaction)<-[f1:from]-(t1: Terminal)
with count(m) as make, tr.product as product, c, t, count(m1) as make1, tr1.product as product1, c1, t1
where make > 2 and product = product1 and make1 > 2 and t=t1 and c<>c1
MERGE (c)-[:friends]->(c1)