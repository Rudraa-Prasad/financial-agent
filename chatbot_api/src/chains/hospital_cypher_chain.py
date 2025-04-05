import os

from langchain.chains import GraphCypherQAChain
from langchain.prompts import PromptTemplate
from langchain_community.graphs import Neo4jGraph
from langchain_groq import ChatGroq

FINANCE_QA_MODEL = os.getenv("FINANCE_QA_MODEL")
FINANCE_CYPHER_MODEL = os.getenv("FINANCE_CYPHER_MODEL")

graph = Neo4jGraph(
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USERNAME"),
    password=os.getenv("NEO4J_PASSWORD"),
)

graph.refresh_schema()

cypher_generation_template = """
Task:
Generate Cypher query for a Neo4j graph database.

Instructions:
Use only the provided relationship types and properties in the schema.
Do not use any other relationship types or properties that are not provided.

Schema:
{schema}

Note:
Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything other than
for you to construct a Cypher statement. Do not include any text except
the generated Cypher statement. Make sure the direction of the relationship is
correct in your queries. Make sure you alias both entities and relationships
properly. Do not run any queries that would add to or delete from
the database. Make sure to alias all statements that follow with a
WITH clause (e.g., WITH t AS transaction, a.amount AS amount) to ensure 
clear reference to variables and properties in subsequent steps.
If you need to divide numbers, make sure to
filter the denominator to be non zero.


# Who processed the highest total transaction amount?
MATCH (t:Transaction)-[:PROCESSED_BY]->(e:Employee)
RETURN e.employee_name AS employee, SUM(t.amount) AS total_processed
ORDER BY total_processed DESC
LIMIT 1

# Which customer has written the most reviews?
MATCH (c:Customer)-[:WRITTEN_BY]->(r:Review)
RETURN c.name AS customer_name, COUNT(r) AS review_count
ORDER BY review_count DESC
LIMIT 1

# Which branch holds the highest total account balance?
MATCH (a:Account)-[:BELONGS_TO]->(b:Branch)
RETURN b.name AS branch_name, SUM(a.balance) AS total_balance
ORDER BY total_balance DESC
LIMIT 1

# What is the average transaction amount per transaction type?
MATCH (t:Transaction)
RETURN t.transaction_type AS type, AVG(t.amount) AS avg_amount
ORDER BY avg_amount DESC

# How many employees were hired before 2015 and earn more than $100k?
MATCH (e:Employee)
WHERE e.year_of_joining < 2015 AND e.salary > 100000
RETURN COUNT(e) AS senior_high_earners

String category values:
Test results are one of: 'Inconclusive', 'Normal', 'Abnormal'
Visit statuses are one of: 'OPEN', 'DISCHARGED'
Admission Types are one of: 'Elective', 'Emergency', 'Urgent'
Payer names are one of: 'Cigna', 'Blue Cross', 'UnitedHealthcare', 'Medicare',
'Aetna'

A visit is considered open if its status is 'OPEN' and the discharge date is
missing.
Use abbreviations when
filtering on FINANCE states (e.g. "Texas" is "TX",
"Colorado" is "CO", "North Carolina" is "NC",
"Florida" is "FL", "Georgia" is "GA, etc.)

Make sure to use IS NULL or IS NOT NULL when analyzing missing properties.
Never return embedding properties in your queries. You must never include the
statement "GROUP BY" in your query. Make sure to alias all statements that
follow as with statement (e.g. WITH v as visit, c.billing_amount as
billing_amount)
If you need to divide numbers, make sure to filter the denominator to be non
zero.

The question is:
{question}
"""

cypher_generation_prompt = PromptTemplate(
    input_variables=["schema", "question"], template=cypher_generation_template
)

qa_generation_template = """You are an assistant that takes the results
from a Neo4j Cypher query and forms a human-readable response. The
query results section contains the results of a Cypher query that was
generated based on a users natural language question. The provided
information is authoritative, you must never doubt it or try to use
your internal knowledge to correct it. Make the answer sound like a
response to the question.

Query Results:
{context}

Question:
{question}

If the provided information is empty, say you don't know the answer.
Empty information looks like this: []

If the information is not empty, you must provide an answer using the
results. If the question involves a time duration, assume the query
results are in units of days unless otherwise specified.

When names are provided in the query results, such as FINANCE names,
beware  of any names that have commas or other punctuation in them.
For instance, 'Jones, Brown and Murray' is a single FINANCE name,
not multiple FINANCEs. Make sure you return any list of names in
a way that isn't ambiguous and allows someone to tell what the full
names are.

Never say you don't have the right information if there is data in
the query results. Make sure to show all the relevant query results
if you're asked.

Helpful Answer:
"""

qa_generation_prompt = PromptTemplate(
    input_variables=["context", "question"], template=qa_generation_template
)

FINANCE_cypher_chain = GraphCypherQAChain.from_llm(
    cypher_llm=ChatOpenAI(model=FINANCE_CYPHER_MODEL, temperature=0),
    qa_llm=ChatOpenAI(model=FINANCE_QA_MODEL, temperature=0),
    graph=graph,
    verbose=True,
    qa_prompt=qa_generation_prompt,
    cypher_prompt=cypher_generation_prompt,
    validate_cypher=True,
    top_k=100,
)
