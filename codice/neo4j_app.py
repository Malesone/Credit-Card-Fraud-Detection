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
from tqdm import tqdm

class App:
  session: Any
  uri = "bolt://localhost:7687"
  user = "neo4j"
  password = "test"
  created = False

  def __init__(self):
    self.created = False

  def create_app(self):
    self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
    self.session = self.driver.session()
    print("Connected")
    self.created = True

  def close(self):
    self.driver.close()

  ############ start CREATION ############ 
  def create_all(self, customers, terminals, transactions):    
    arrayC = customers.to_numpy()
    arrayT = terminals.to_numpy()

    self.create_customers(arrayC)
    self.create_terminals(arrayT)

    arrayTransactions = transactions.to_numpy()
    lenT = len(arrayTransactions)
    for i in tqdm(range(lenT)):
        val = arrayTransactions[i]
        self.create_transaction(str([val[2], val[3], val[0], val[1].date().strftime('%Y-%m-%d'), val[4], val[7]]))    
        
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

  def create_transaction(self, unw):
    query = (
      "WITH " + unw +" AS unw "
      "MATCH (c:Customer {id: unw[0]}), (t:Terminal {id: unw[1]}) "
      "CREATE (c)-[u:Transaction {id: unw[2], date: date(unw[3]), amount: unw[4], fraud: unw[5]}]->(t) "   
    )
    self.session.run(query)

  ############ end CREATION ############ 


  ############ start QUERIES ############ 
  def execute_queries(self, month):
    print("Stampa somma totale per customer... ")
    #self.return_amount_customer(month)
    print("Stampa transazioni fraudolente... ")
    self.fraudolent_transactions()

  def return_amount_customer(self, month):
    query = (
        """
        MATCH (c:Customer)-[t:Transaction]->() 
        WHERE datetime({date:t.date}) >= datetime('AAAA-MM-GG') and datetime({date:t.date}) <= datetime('AAAA-MM-GG') 
        RETURN c.id, t.date, sum(t.amount)
        """
    )
    result = self.session.run(query, month=month)
    print([row for row in result])

  def fraudolent_transactions(self):
    query = (
        """
        match (tr:Terminal)<-[t:Transaction]-() 
        with tr.id as id, avg(t.amount) as avg_amount, date.truncate('month', t.date) as month 
        match (trr:Terminal)<-[t1:Transaction]-() 
        where t1.amount > avg_amount/2 and date.truncate('month', t1.date) = month and trr.id = id 
        return t1 
        """
    )
    result = self.session.run(query)
    print([row for row in result])

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
  ############ end QUERIES ############ 

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
