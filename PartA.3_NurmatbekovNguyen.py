from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

#____________AFFILIATION(as properties)____________#
def import_affiliations(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///authors.csv\" AS row\
            WITH row\
            MATCH (au:Author{authorID:row.authorID})\
            SET au.organisation = row.orgName, au.orgType=row.orgType;")

#______________MODIFYING REVIEWS EDGE_____________#
def modify_reviews(tx):
    tx.run("LOAD CSV WITH HEADERS FROM \"file:///reviewed_by_decision.csv\" AS row FIELDTERMINATOR ';'\
            WITH split(row.review1, '|') AS review1_info,\
            split(row.review2, '|') AS review2_info,\
            split(row.review3, '|') AS review3_info, row\
            MATCH (a:Article{articleID:row.articleID}),\
            (au1:Author{authorID: review1_info[0]}),\
            (au2:Author{authorID: review2_info[0]}),\
            (au3:Author{authorID: review3_info[0]})\
            MATCH(au1)-[r1:reviews]->(a)\
            MATCH(au2)-[r2:reviews]->(a)\
            MATCH (au3)-[r3:reviews]->(a)\
            SET r1.review = review1_info[1]\
            SET r1.decision = review1_info[2]\
            SET r2.review = review2_info[1]\
            SET r2.decision = review2_info[2]\
            SET r3.review = review3_info[1]\
            SET r3.decision = review3_info[2];")

with driver.session() as session:
    session.write_transaction(import_affiliations)
    print("finished loading affiliations")

    session.write_transaction(modify_reviews)  # 10 000 rows
    print("finished loading modified reviews")