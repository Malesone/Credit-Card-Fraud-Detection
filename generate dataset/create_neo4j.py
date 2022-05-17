from pytest import PytestCollectionWarning
from gen import generate_all, get_dataset
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import numpy as np

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_customers(self, customers):
        with self.driver.session() as session:
            for row in customers.CUSTOMER_ID:
                session.write_transaction(
                    self._create_and_return_customers, row)
            
    @staticmethod
    def _create_and_return_customers(tx, person1_name):
        query = (
            "CREATE (c:Customer { name: $person1_name }) "
            "RETURN c"
        )
        tx.run(query, person1_name=person1_name)

    def create_terminals(self, terminals):
        with self.driver.session() as session:
            for row in terminals.TERMINAL_ID:
                session.write_transaction(
                    self._create_and_return_terminals, row)
            
    @staticmethod
    def _create_and_return_terminals(tx, terminal):
        query = (
            "CREATE (c:Terminal { name: $terminal }) "
            "RETURN c"
        )
        tx.run(query, terminal=terminal)

    def create_transactions(self, transactions):
        with self.driver.session() as session:
            for row in range(len(transactions)):
                id = transactions.iloc[row].TRANSACTION_ID #ritorna un formato numpy.int64
                idC = transactions.iloc[row].CUSTOMER_ID
                idT = transactions.iloc[row].TERMINAL_ID
                id_int = np.int64(id)
                #print(type(idT))
                session.write_transaction(
                    self._create_and_return_transactions, id_int.item(), idC, idT)
            
    @staticmethod
    def _create_and_return_transactions(tx, id, idC, idT):
        query = (
            "MATCH (c:Customer {name: $idC})"
            "MATCH (tr:Terminal {name: $idT})"
            "CREATE (t:Transaction { name: $id }) "
            "CREATE (t)<-[:make]-(c) "
            "CREATE (tr)-[:from]->(t) "
            "RETURN t"
        )
        tx.run(query, id=id, idC=idC, idT=idT)

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]

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
    uri = "neo4j+s://300d5335.databases.neo4j.io"
    user = "neo4j"
    password = "dwbybX86j40mk62hG2Jv4jWDH7zn4FVHO5AdyL7lbj4"
    app = App(uri, user, password)
    
    app.delete_all()
    app.create_customers(customer)
    app.create_terminals(terminal)
    app.create_transactions(transaction)
    
    app.close()