from datetime import date, datetime
from re import A
from time import strftime
import neo4j
from pytest import PytestCollectionWarning
from sqlalchemy import Date
from gen import generate_all, get_dataset
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import numpy as np
from collections import defaultdict

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
            
    
    def create_all(self, customers, terminals, transactions):
        customer_dict = {}
        with self.driver.session() as session:
            for row in customers.CUSTOMER_ID: 
                session.write_transaction(
                    self._create_and_return_customers, row)

            for row in terminals.TERMINAL_ID:
                session.write_transaction(
                    self._create_and_return_terminals, row)

            for row in range(len(transactions)):
                id = transactions.iloc[row].TRANSACTION_ID #ritorna un formato numpy.int64
                id_int = np.int64(id)
                idC = transactions.iloc[row].CUSTOMER_ID
                idT = transactions.iloc[row].TERMINAL_ID
                amount = transaction.iloc[row].TX_AMOUNT
                date = transaction.iloc[row].TX_DATETIME #pandas._libs.tslibs.timestamps.Timestamp
                
                session.write_transaction(
                    self._create_and_return_transactions, id_int.item(), idC, idT, amount, date.date()) 

                if not idC in customer_dict:
                    customer_dict[idC] = [idT]
                else:
                    if not idT in customer_dict[idC]:
                        customer_dict[idC].append(idT)

            session.write_transaction(
                    self._create_tmp, customer_dict) 


    @staticmethod
    def _create_and_return_customers(tx, person1_name):
        query = (
            "CREATE (c:Customer { name: $person1_name }) "
            "RETURN c"
        )
        tx.run(query, person1_name=person1_name)

    @staticmethod
    def _create_and_return_terminals(tx, terminal):
        query = (
            "CREATE (c:Terminal { name: $terminal }) "
            "RETURN c"
        )
        tx.run(query, terminal=terminal)

    @staticmethod
    def _create_and_return_transactions(tx, id, idC, idT, amount, date):
        query = (
            "MATCH (c:Customer {name: $idC}) MATCH (tr:Terminal {name: $idT})"
            "CREATE (t:Transaction { name: $id, amount: $amount, date: $date }) "
            "CREATE (t)<-[:make]-(c) "
            "CREATE (tr)-[:from]->(t) "
            "RETURN t"
        )
        tx.run(query, id=id, idC=idC, idT=idT, amount=amount, date=date)

    @staticmethod
    def _create_tmp(tx, dict):
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

if __name__ == "__main__":
    generate_all()
    (customer, terminal, transaction) = get_dataset()
    uri = "neo4j+s://858239b5.databases.neo4j.io"
    user = "neo4j"
    password = "9-GTN-UU2SCO75wnczqxYZiC-GUsUBc1Jv5hCyA3KZA"
    app = App(uri, user, password)
    
    app.delete_all()
    app.create_all(customer, terminal, transaction)
    
    #app.amount_customer(2)
    #app.fraudolent_transactions()
    app.close()

