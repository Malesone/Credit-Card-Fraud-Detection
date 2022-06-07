from asyncio import run_coroutine_threadsafe
from datetime import date, datetime
from re import A
from time import strftime
from typing import Any
import neo4j
from sqlalchemy import Date
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import numpy as np
from collections import defaultdict
import time
import ast

class App:
  session: Any
  uri = "bolt://localhost:7687"
  user = "neo4j"
  password = "test"
    
  def __init__(self):
    self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
    self.session = self.driver.session()
    print("Connected")

  def close(self):
    self.driver.close()

  ############ start CREATION ############ 
  def create_all(self, customers, terminals, transactions):    
    arrayC = customers.to_numpy()
    arrayT = terminals.to_numpy()

    self.create_customers(arrayC)
    self.create_terminals(arrayT)

    arrayTransactions = transactions.to_numpy()
    print(len(arrayTransactions))
    arrayT = [[val[2], val[3], [val[0], val[1].date().strftime('%Y-%m-%d'), val[4], val[7]]] for val in arrayTransactions]
    
    connections = {}
    for val in arrayT:
      if not str([val[0], val[1]]) in connections:
        connections[str([val[0], val[1]])] = [val[2]]
      else:
        connections[str([val[0], val[1]])].append(val[2])

    arrayT = []
    for key, value in connections.items():
      v = ast.literal_eval(key)
      arrayT.append([v[0], v[1], value])

    at_split = [arrayT]
    t = 1000
    while len (at_split [-1]) >= 2 * t:
      at_split [-1:] = [at_split [-1][:t], at_split [-1][t:]]
    
    for chunk in at_split:
      self.create_transaction(str(chunk))

  def create_customers(self, array):
    arrayC = [[val[0], val[1], val[2], val[8]] for val in array]
    arrayC = str(arrayC)
    query = (
          "WITH " + arrayC + " AS array "
          "UNWIND array as value "
          "WITH value[0] as id, value[1] as lng, value[2] as lat, value[3] as totalAmount " 
          "CREATE (:Customer {id: id, lng: lng, lat: lat, totalAmount: totalAmount}) "
    )

    self.session.run(query)
    print("Customer nodes created")

  def create_terminals(self, array):
    arrayTr = [[val[0], val[1], val[2]] for val in array]
    arrayTr = str(arrayTr)
    query = (
          "WITH " + arrayTr + " AS array "
          "UNWIND array as value "
          "WITH value[0] as id, value[1] as lng, value[2] as lat " 
          "CREATE (:Terminal {id: id, lng: lng, lat: lat}) "
    )
    self.session.run(query)
    print("Terminal nodes created")

  def create_transaction(self, array):
    print("RUNNING QUERY TRANSACTION")
    query = (
      "WITH " + array +" AS array "
      "UNWIND array as unw "
      "WITH unw[0] as cn, unw[1] as tn, unw[2] as tr "
      "MATCH (c:Customer {id: cn}), (t:Terminal {id: tn}) "
      "UNWIND tr as value "
      "WITH value[0] as id, value[1] as date, value[2] as amount, value[3] as fraud, c, t "
      "CREATE (c)-[u:Transaction {id: id, date: date(date), amount: amount, fraud: fraud}]->(t) "   
    )
    self.session.run(query)

  ############ end CREATION ############ 


  @staticmethod
  def _create_connection_customer_terminal(tx, dict):
    for key, value in dict.items():
        for item in value:
            query = (
                "match (c:Customer {name: $key}) MATCH (tr:Terminal {name: $item})"
                "CREATE (c)-[:connect]->(tr) "
                "RETURN tr"
            )
            tx.run(query, key=key, item=item)

  def amount_customer(self, month):
    with self.driver.session() as session:
        result = session.read_transaction(self._return_amount_customer, month)
        for row in result:
            print(row)

  @staticmethod
  def _return_amount_customer(tx, month):
    query = (
        "MATCH (c:Customer)-[:make]->(t:Transaction)"
        "WHERE datetime({date:t.date}) >= datetime('2022-1-1') and datetime({date:t.date}) <= datetime('2022-1-31')"
        "RETURN c.name, t.date, sum(t.amount)"
    )
    result = tx.run(query, month=month)
    return [row for row in result]

  def fraudolent_transactions(self):
    with self.driver.session() as session:
        result = session.read_transaction(self._return_fraudolent_transactions, )
        for row in result:
            print(row)

  @staticmethod
  def _return_fraudolent_transactions(tx):
    query = (
        "match (t:Transaction)<-[:from]-(tr:Terminal) with tr.name as name, avg(t.amount) as avg_amount, date.truncate('month', t.date) as month match (trr:Terminal)-[:from]->(t1:Transaction) where t1.amount > avg_amount/2 and date.truncate('month', t1.date) = month and trr.name = name return t1"
    )
    result = tx.run(query)
    return [row for row in result]

  @staticmethod
  def _return_cocustomer(tx, id, n):
    cCollected = "[]"
    tCollected = "[]"
    p = "[]"

    if (n == 0):
      return cCollected

    
    query = (
          """
          match(c:Customer {id: """ + str(id) + """})-[]->(t)
          with collect(DISTINCT t.id) AS tCollected, collect(DISTINCT c.id) as preC

          UNWIND tCollected as tCol
          match (t: Terminal {id: tCol})-[]-(c:Customer)
          where not c.id in preC
          return collect(DISTINCT c.id) AS cCollected, tCollected, preC as p

          """
    )
    
    result = [row for row in tx.run(query)]
    cCollected = str([el for el in result[0]][0])
    tCollected = str([el for el in result[0]][1])
    p = str([el for el in result[0]][2])
  
    for _ in range(1, n):
      query = (
          """
          UNWIND """ + cCollected + """ AS col
          match (c: Customer {id: col})-[]-(t)
          WHERE NOT t.id IN """ + tCollected + """
          with collect(DISTINCT t.id) AS tCollected, collect(DISTINCT c.id) + """ + p + """ as preC

          UNWIND tCollected as tCol
          match (t: Terminal {id: tCol})-[]-(c:Customer)
          where not c.id in preC
          RETURN collect(DISTINCT c.id) AS cCollected, tCollected, preC as p

          """
      )
      result = [row for row in tx.run(query)]
      cCollected = str([el for el in result[0]][0])
      tCollected = str([el for el in result[0]][1])
      p = str([el for el in result[0]][2])
      
    return cCollected

  ############ start EXTENSION ############ 
  def extension(self):
    self.extend_transactions()
    self.buying_friends()

  def extend_transactions(self):
    query = (
      "match (c)-[t]-(tr) "
      "with ['morning','afternoon', 'evening', 'night'] as moments, ['high-tech','food', 'clothing', 'consumable', 'other'] AS products, t "
      "SET t.moment = CASE toInteger(rand() * 4) "
        "WHEN 0 THEN moments[0] "
        "WHEN 1 THEN moments[1] "
        "WHEN 2 THEN moments[2] "
        "WHEN 3 THEN moments[3] "
      "END "
      "SET t.product = CASE toInteger(rand() * 5) "
        "WHEN 0 THEN products[0] "
        "WHEN 1 THEN products[1] "
        "WHEN 2 THEN products[2] "
        "WHEN 3 THEN products[3] "
        "WHEN 4 THEN products[4] "
        "END "
    )
    self.session.run(query)

  def buying_friends(self):
    query = (
      "match (c: Customer)-[tr: Transaction]->(t: Terminal) "
      "with count(tr) as ttr, c , t, tr.product as p "
      "where ttr > 2 "
      "WITH distinct(c) as distinctC, t, p "
      "match (c1: Customer)-[tr: Transaction]->(t) "
      "with count(tr) as ttr, c1, tr.product as p1, distinctC as cc, t as tt, p as pp "
      "where ttr > 2 and cc<>c1 "
      "match (cc)-[]->()<-[]-(c1) "
      "WHERE pp = p1 "
      "MERGE (cc)-[:buying_friends]->(c1) "
      )
    self.session.run(query)

  ############ end EXTENSION ############ 

  def transactions_per_period(self):
    query = (
      "match ()-[t: Transaction]-() "
      "with t.moment as moment, count(t) as transactions, sum(t.fraud) as fraudTransaction "
      "return moment, transactions, fraudTransaction "
    )
    result = self.session.run(query)
    for row in result:
      concat = "PERIOD: " + row["moment"] + "\nTransactions: " + str(row["transactions"]) + "\nFraud transactions: " + str(row["fraudTransaction"])
      print(concat + "\n")


  def delete_all(self):
    query = (
        "MATCH (n)"
        "detach DELETE n"
        )

    self.session.run(query)
    print("old db deleted")
