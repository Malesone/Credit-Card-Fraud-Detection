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


match (c: Customer)-[:make]->(tr: Transaction)<-[f:from]-(t: Terminal)
with count(tr) as ttr, c, t, t.name as name, tr.product as p
where ttr > 2
call{
    with name, c
    match (c1: Customer)-[:make]->(tr1: Transaction)<-[:from]-(t1: Terminal {name: name})
    with count(tr1) as ttr1, c1, t1, tr1.product as p1, c
    where ttr1 > 2 and c<>c1
    merge (c)-[:buying_friends]-(c1)
    return c1
}
return c1

5. 
call{
    match (t: Transaction)
    with t.moment as moment, count(t) as transactions
    return moment, transactions
}
match (t:Transaction)<-[:from]-(tr:Terminal) 
with tr.name as name, avg(t.amount) as avg_amount, date.truncate('month', t.date) as month, transactions, moment
match (trr:Terminal)-[:from]->(t1:Transaction {moment: moment})
where t1.amount > avg_amount/2 and date.truncate('month', t1.date) = month and trr.name = name 
return moment, transactions, count(t1)