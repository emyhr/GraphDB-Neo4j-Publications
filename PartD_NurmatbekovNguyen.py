from neo4j import GraphDatabase
import pandas as pd

# Console ouput configuration
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', 2000)

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

keywords = ["data management",
            "indexing",
            "data modeling",
            "big data",
            "data processing",
            "data storage",
            "data querying"]

def get_community(keywords):
    with driver.session() as session:
        return session.run("MATCH(a:Article)-->(k:Keyword)\
                    WHERE k.keyword IN {keywords}\
                    RETURN collect(distinct a.title) as listOfArticles", keywords=keywords).data()

def get_related_conferences(keywords):
    with driver.session() as session:
        return session.run("MATCH (c1:Conference)--(e1:Edition)--(a1:Article)\
                    WITH c1.conferenceID as confName, COUNT(distinct a1) as totalArticles\
                    MATCH (c:Conference)--(e:Edition)--(a:Article)--(k:Keyword)\
                    WHERE k.keyword IN {keywords} and confName = c.conferenceID\
                    WITH c.conferenceID as confName, totalArticles,\
                    COLLECT(distinct a.articleID) as listOfRelatedArticles, COUNT(distinct a.articleID) AS totalRelatedArticles\
                    WHERE (toFloat(totalRelatedArticles)/totalArticles>=0.9)\
                    RETURN confName, totalRelatedArticles, totalArticles, listOfRelatedArticles",
                    keywords = keywords).data()

def get_related_journals(keywords):
    with driver.session() as session:
        return session.run("MATCH (j1:Journal)--(v1:Volume)--(a1:Article)\
                    WITH j1.journalID as journalName, COUNT(distinct a1) as totalArticles\
                    MATCH (j:Journal)--(v:Volume)--(a:Article)--(k:Keyword)\
                    WHERE k.keyword IN {keywords} and journalName = j.journalID\
                    WITH j.journalID as journalName, totalArticles,\
                    COLLECT(distinct a.articleID) as listOfRelatedArticles, COUNT(distinct a.articleID) AS totalRelatedArticles\
                    WHERE (toFloat(totalRelatedArticles)/totalArticles>=0.9)\
                    RETURN journalName, totalRelatedArticles, totalArticles, listOfRelatedArticles",
                    keywords=keywords).data()

def run_article_rank(list_of_related_articles):
    with driver.session() as session:
        return session.run("CALL algo.pageRank.stream('MATCH (a:Article) WHERE a.articleID IN {list_of_related_articles} RETURN id(a) as id',\
                            'MATCH (a1:Article)-[:cites]->(a2:Article) WHERE a1.articleID IN {list_of_related_articles} AND a2.articleID IN {list_of_related_articles}\
                             RETURN id(a1) as source, id(a2) as target',{iterations:20, dampingFactor:0.85, graph:'cypher',\
                             params:{list_of_related_articles:{list_of_related_articles}}})\
                             YIELD nodeId, score\
                             RETURN algo.getNodeById(nodeId).articleID AS articleID, algo.getNodeById(nodeId).title AS articleName, score\
                             ORDER BY score DESC LIMIT 100",
                             list_of_related_articles=list_of_related_articles).data()

def get_gurus(top_100_articles):
    with driver.session() as session:
        return session.run("MATCH (au:Author)-[w:write]->(a:Article)\
                            WHERE a.articleID IN {top_100_articles}\
                            WITH au.name AS authorName, COUNT(w) AS numberOfArticles\
                            WHERE numberOfArticles>=2\
                            RETURN authorName AS Gurus, numberOfArticles\
                            ORDER BY numberOfArticles DESC",
                            top_100_articles=top_100_articles).data()

if __name__ == '__main__':

    # 1. Find/define the research communities
    communities = get_community(keywords)
    list_of_community_articles = []
    for community in communities:
        for article in community["listOfArticles"]:
            list_of_community_articles.append(article)
    print(list_of_community_articles)

    # 2. Find the conferences and journals related to the database community
    list_of_related_conf = get_related_conferences(keywords)
    list_of_related_jour = get_related_journals(keywords)
    print(list_of_related_conf)
    print(list_of_related_jour)

    # 3. Identify gurus as those authors that are authors of, at least, two papers among the top-100 identified.
    list_of_related_articles = []
    list_of_related_conf = pd.DataFrame(list_of_related_conf).iloc[:, [3]]
    list_of_related_jour = pd.DataFrame(list_of_related_jour).iloc[:, [3]]
    all_related_conf_jour = pd.concat([list_of_related_conf,list_of_related_jour], axis=0).transpose().values.tolist()[0]

    for conf in all_related_conf_jour:
        for article in conf:
            list_of_related_articles.append(article)

    top_100_articles = run_article_rank(list_of_related_articles)
    print(top_100_articles)

    # 4. Get gurus
    top_100_articles = pd.DataFrame(top_100_articles).iloc[:, [0]].transpose().values.tolist()[0]
    print(pd.DataFrame(get_gurus(top_100_articles)))