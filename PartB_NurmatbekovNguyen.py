from neo4j import GraphDatabase
import pandas as pd

# Console ouput configuration
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', 1000)

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

def find_h_indexes(tx):
    return tx.run("MATCH (au:Author)-[w:write]->(a1:Article)<--(a2:Article)\
                    WITH au.name as authorName, a1.title as articleName, count(a2) as citations\
                    ORDER BY authorName, citations DESC\
                    WITH authorName, collect(citations) as articles\
                    UNWIND range(0, size(articles)-1) as index\
                    WITH authorName,\
                    CASE WHEN articles[index] <= (index+1)\
                    THEN articles[index]\
                    ELSE (index+1)\
                    END as indexes\
                    RETURN authorName, max(indexes) as hIndex")

def top_3_articles(tx):
    return tx.run("MATCH (c:Conference)--(:Edition)--(a1:Article)<--(a2:Article)\
            WITH c.conferenceID as conferenceID, a1.title as articleName, count(a2) as citations\
            ORDER BY conferenceID, citations DESC\
            WITH conferenceID, collect(articleName) as articles\
            RETURN conferenceID, articles[..3] as top3Articles")

def authors_publish_at_least_4_editions(tx):
    return tx.run("MATCH (c:Conference)--(e:Edition)--(a:Article)<-[:write]-(au:Author)\
                    WITH c.conferenceID as conferenceID, au.name as authorName, count(distinct e) as nEditions\
                    WHERE nEditions>=4\
                    RETURN conferenceID, collect(authorName) as authors")

def impact_factor(tx, year):
    return tx.run("MATCH (y1:Year {year:{year1}}), (y2:Year {year:{year2}}),\
                    (y1)--(v1:Volume), (v2:Volume)--(y2),\
                    (a1:Article)-[:published_in]->(v1)--(j:Journal)--(v2)<-[:published_in]-(a2:Article)\
                    WITH j, collect(distinct a1) as Articles1, collect(distinct a2) as Articles2\
                    WITH j, Articles1, Articles2, \
                    size(Articles1)+size(Articles2) as publications\
                    UNWIND Articles1 as a1\
                    MATCH (a1)<-[c1:cites]-()\
                    WITH j, count(c1) as n1, Articles2, publications\
                    UNWIND Articles2 as a2\
                    MATCH (a2)<-[c2:cites]-()\
                    WITH j, n1+count(c2) as citations, publications\
                    RETURN j.journalID, publications, citations, citations/publications as impactFactor", year1=year-1, year2=year-2)

with driver.session() as session:

    ### Find h-indexes
    print('h-indexes')
    print(pd.DataFrame(session.write_transaction(find_h_indexes).data()))

    ### 3 MOST CITED PAPERS PER CONFERENCES
    print('3 most cited papers')
    print(pd.DataFrame(session.write_transaction(top_3_articles).data()))

    #For each conference find its community
    print('community')
    print(pd.DataFrame(session.write_transaction(authors_publish_at_least_4_editions).data()))

    # Calculate impact factor
    print('impact factor')
    ipYear=1973 # set the year for calculating impact factor
    print(pd.DataFrame(session.write_transaction(impact_factor, ipYear).data()))