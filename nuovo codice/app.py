from datetime import date, datetime
from re import A
from time import strftime
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
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
             
    def create_all(self, customers, terminals, transactions):
        customer_dict = {}
        with self.driver.session() as session:
            arrayC = customers['CUSTOMER_ID'].to_numpy().astype(int)
            arrayT = terminals['TERMINAL_ID'].to_numpy().astype(int)

            
            session.write_transaction(self._create_and_return_customers, arrayC)
            print("Customer nodes created")
            session.write_transaction(self._create_and_return_terminals, arrayT)
            print("Terminal nodes created")
  
            arrayTransactions = transactions.to_numpy()
            arrayT = [[val[2], val[3], [val[0], "date(" +val[1].date().strftime('%Y-%m-%d')+")", val[4], val[7], val[8]]] for val in arrayTransactions]
            
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
            t = 2000
            while len (at_split [-1]) >= 2 * t:
              at_split [-1:] = [at_split [-1][:t], at_split [-1][t:]]
 
            
            for chunk in at_split:
              session.write_transaction(self._create_and_return_transactions, str(chunk))

    @staticmethod
    def _create_and_return_customers(tx, array):
        arrayString = "["
        for value in array:
          arrayString += str(value)
          arrayString += ","

        arrayString = arrayString[0:len(arrayString)-1]
        arrayString += "]"
        print(arrayString)
        query = (
            "WITH " + arrayString + " AS array "
            "UNWIND array as value "
            "CREATE (:Customer {name: value}) "
        )

        tx.run(query)

   

   
    def _chunks (self, l, n):
      r = [l]
      while len (r [-1]) >= 2 * n:
        r [-1:] = [r [-1][:n], r [-1][n:]]
      return r

    @staticmethod
    def _create_and_return_terminals(tx, array):
        arrayString = "["
        for value in array:
          arrayString += str(value)
          arrayString += ","

        arrayString = arrayString[0:len(arrayString)-1]
        arrayString += "]"
        query = (
            "WITH " + arrayString + " AS array "
            "UNWIND array as value "
            "CREATE (:Terminal {name: value}) "    
        )


        tx.run(query)


    @staticmethod
    def _create_and_return_transactions(tx, array):
      print("RUNNING QUERY TRANSACTION")
      query = (
        "WITH " + array +" AS array "
        "UNWIND array as unw "
        "WITH unw[0] as cn, unw[1] as tn, unw[2] as tr "
        "MATCH (c:Customer {name: cn}), (t:Terminal {name: tn}) "
        "UNWIND tr as value "
        "WITH value[0] as id, value[1] as date, value[2] as amount, value[3] as moment, value[4] as product, c, t "
        "CREATE (c)-[u:Transaction {id: id, date: date, amount: amount, moment: moment, product: product}]->(t) "   
      )
      tx.run(query)

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

    def delete_all(self):
        with self.driver.session() as session:
            session.write_transaction(self._delete_a)

    @staticmethod
    def _delete_a(tx):      
        query = (
            "MATCH (n)"
            "detach DELETE n"
            )
        tx.run(query)