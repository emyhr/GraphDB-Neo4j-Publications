from neo4j import GraphDatabase
import pandas as pd

# Console ouput configuration
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', 1000)

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

def page_rank(tx):
    return tx.run("CALL algo.pageRank.stream('Article', 'cites',\
                    {iterations:20, dampingFactor:0.85})\
                    YIELD nodeId, score\
                    RETURN algo.getNodeById(nodeId).title AS article, score\
                    ORDER BY score DESC")

def article_rank(tx):
    return tx.run("CALL algo.articleRank.stream('Article', 'cites',\
                    {iterations:20, dampingFactor:0.85})\
                    YIELD nodeId, score\
                    RETURN algo.getNodeById(nodeId).title AS article, score\
                    ORDER BY score DESC")

def triangle_detection(tx):
    return tx.run("CALL algo.triangle.stream('Article','cites')\
                    YIELD nodeA,nodeB,nodeC\
                    RETURN algo.asNode(nodeA).title AS articleA,\
                    algo.asNode(nodeB).title AS articleB,\
                    algo.asNode(nodeC).title AS articleC")

with driver.session() as session:

    # pageRank
    print(pd.DataFrame(session.write_transaction(page_rank).data()))
    # articleRank
    print(pd.DataFrame(session.write_transaction(article_rank).data()))
    # triangle detection
    # print(pd.DataFrame(session.write_transaction(triangle_detection).data()))