import logging
import os

from neo4j import GraphDatabase
from retry import retry

BRANCHES_CSV_PATH = os.getenv("BRANCHES_CSV_PATH")
EMPLOYEES_CSV_PATH = os.getenv("EMPLOYEES_CSV_PATH")
CUSTOMERS_CSV_PATH = os.getenv("CUSTOMERS_CSV_PATH")
ACCOUNTS_CSV_PATH = os.getenv("ACCOUNTS_CSV_PATH")
TRANSACTIONS_CSV_PATH = os.getenv("TRANSACTIONS_CSV_PATH")
REVIEWS_CSV_PATH = os.getenv("REVIEWS_CSV_PATH")

HOSPITALS_CSV_PATH = os.getenv("HOSPITALS_CSV_PATH")


NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger(__name__)

NODES = ["Branch"] #, "Employee", "Customer", "Account", "Transaction", "Review"]


def _set_uniqueness_constraints(tx, node):
    query = f"""CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
        REQUIRE n.id IS UNIQUE;"""
    _ = tx.run(query, {})


@retry(tries=5, delay=10)
def load_finance_graph_from_csv() -> None:
    """Load structured financial CSV data following
    a specific ontology into Neo4j"""

    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

    LOGGER.info("Setting uniqueness constraints on nodes")
    with driver.session(database="neo4j") as session:
        for node in NODES:
            session.execute_write(_set_uniqueness_constraints, node)

    # Load Nodes
    LOGGER.info("Loading branch nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS 
        FROM '{BRANCHES_CSV_PATH}' AS branches 
        MERGE (b:Branch {{id: toInteger(branches.branch_id),
                        name: branches.branch_name,
                location: branches.location}});
                """

        _ = session.run(query, {})



    


#     LOGGER.info("Loading employee nodes")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{EMPLOYEES_CSV_PATH}' AS employees
#         MERGE (e:Employee {{id: toInteger(employees.employee_id), name: employees.employee_name, position: employees.position, year_of_joining: toInteger(employees.year_of_joining), salary: toFloat(employees.salary)}});
#         """
#         _ = session.run(query, {})

#     LOGGER.info("Loading customer nodes")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{CUSTOMERS_CSV_PATH}' AS customers
#         MERGE (c:Customer {{id: toInteger(customers.customer_id), name: customers.name, phone: customers.phone, email: customers.email, date_joined: date(customers.date_joined)}});
#         """
#         _ = session.run(query, {})

#     LOGGER.info("Loading account nodes")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{ACCOUNTS_CSV_PATH}' AS accounts
#         MERGE (a:Account {{id: toInteger(accounts.account_id), balance: toFloat(accounts.balance), date_opened: date(accounts.date_opened)}});
#         """
#         _ = session.run(query, {})

#     LOGGER.info("Loading transaction nodes")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{TRANSACTIONS_CSV_PATH}' AS transactions
#         MERGE (t:Transaction {{id: toInteger(transactions.transaction_id), transaction_type: transactions.transaction_type, amount: toFloat(transactions.amount), date: datetime(transactions.date)}});
#         """
#         _ = session.run(query, {})

#     LOGGER.info("Loading review nodes")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{REVIEWS_CSV_PATH}' AS reviews
#         MERGE (r:Review {{id: toInteger(reviews.review_id), review: reviews.review, date_submitted: date(reviews.date_submitted)}});
#         """
#         _ = session.run(query, {})

#     # Load Relationships
#     # EMPLOYS (Branch → Employee)
#     LOGGER.info("Loading 'EMPLOYS' relationships")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{EMPLOYEES_CSV_PATH}' AS row
#         MATCH (e:Employee {{id: toInteger(row.employee_id)}})
#         MATCH (b:Branch {{id: toInteger(row.branch_id)}})
#         MERGE (b)-[:EMPLOYS]->(e);
#         """
#         session.run(query, {})



#     # BELONGS_TO (Account → Branch)
#     LOGGER.info("Loading 'BELONGS_TO' relationships")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{ACCOUNTS_CSV_PATH}' AS row
#         MATCH (a:Account {{id: toInteger(row.account_id)}})
#         MATCH (b:Branch {{id: toInteger(row.branch_id)}})
#         MERGE (a)-[:BELONGS_TO]->(b);
#         """
#         session.run(query, {})

#     # LINKED_WITH (Account → Customer)
#     LOGGER.info("Loading 'LINKED_WITH' relationships")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{ACCOUNTS_CSV_PATH}' AS row
#         MATCH (a:Account {{id: toInteger(row.account_id)}})
#         MATCH (c:Customer {{id: toInteger(row.customer_id)}})
#         MERGE (c)-[:LINKED_WITH]->(a);
#         """
#         session.run(query, {})

#     # PROCESSED_BY (Transaction → Employee)
#     LOGGER.info("Loading 'PROCESSED_BY' relationships")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{TRANSACTIONS_CSV_PATH}' AS row
#         MATCH (t:Transaction {{id: toInteger(row.transaction_id)}})
#         MATCH (e:Employee {{id: toInteger(row.employee_id)}})
#         MERGE (t)-[:PROCESSED_BY]->(e);
#         """
#         session.run(query, {})

#     # WRITTEN_BY (Review → Customer)
#     LOGGER.info("Loading 'WRITTEN_BY' relationships")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{REVIEWS_CSV_PATH}' AS row
#         MATCH (r:Review {{id: toInteger(row.review_id)}})
#         MATCH (c:Customer {{id: toInteger(row.customer_id)}})
#         MERGE (c)-[:WRITTEN_BY]->(r);
#         """
#         session.run(query, {})

#     # RECEIVED_ABOUT (Review → Branch)
#     LOGGER.info("Loading 'RECEIVED_ABOUT' relationships")
#     with driver.session(database="neo4j") as session:
#         query = f"""
#         LOAD CSV WITH HEADERS FROM '{REVIEWS_CSV_PATH}' AS row
#         MATCH (r:Review {{id: toInteger(row.review_id)}})
#         MATCH (b:Branch {{id: toInteger(row.branch_id)}})
#         MERGE (r)-[:RECEIVED_ABOUT]->(b);
#         """
#         session.run(query, {})
# 

LOGGER.info("All relationships loaded successfully")

if __name__ == "__main__":
    load_finance_graph_from_csv()
