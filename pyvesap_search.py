# pip install pyvespa sentence-transformers pandas
import pandas as pd
from vespa.application import Vespa
from vespa.io import VespaQueryResponse
from sentence_transformers import SentenceTransformer

# Initialize the SentenceTransformer model for embedding generation
model = SentenceTransformer('all-MiniLM-L6-v2')

# Function to display the search results as a DataFrame
def display_hits_as_df(response: VespaQueryResponse, fields) -> pd.DataFrame:
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"].get(field, "N/A")
        records.append(record)
    return pd.DataFrame(records)

# Keyword search function using Vespa's BM25 ranking
def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

# Function to compute the embedding for a given text using SentenceTransformer
def compute_embedding(text):
    embedding = model.encode(text).tolist()
    return embedding

# Semantic search function using nearest neighbor search on the embedding
def semantic_search(app, query_text):
    # Compute the embedding vector for the query text
    query_embedding = compute_embedding(query_text)
    
    query_body = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding, query_embedding))",
        "hits": 5,
        "ranking": "semantic",
        "ranking.features.query(query_embedding)": {
            "values": query_embedding
        }
    }
    response = app.query(body=query_body)
    return display_hits_as_df(response, ["doc_id", "title"])

# Function to retrieve the embedding of a specific document
def get_embedding(app, doc_id):
    query = {
        "yql": f"select doc_id, title, text, embedding from content.doc where doc_id contains '{doc_id}'",
        "hits": 1
    }
    result = app.query(query)
    
    if result.hits:
        return result.hits[0]
    return None

# Function to perform recommendation-based search using a given embedding
def query_movies_by_embedding(app, embedding_vector):
    query_body = {
        'yql': 'select * from content.doc where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'hits': 5,
        'ranking.features.query(user_embedding)': {"values": embedding_vector},
        'ranking.profile': 'recommendation'
    }
    response = app.query(query_body)
    return display_hits_as_df(response, ["doc_id", "title", "text"])

# Initialize the Vespa application
app = Vespa(url="http://localhost", port=8080)

# Sample keyword query
query = "Harry Potter and the Half-Blood Prince"
df = keyword_search(app, query)
print("Keyword Search Results:")
print(df.head())

# Sample semantic query
df = semantic_search(app, query)
print("\nSemantic Search Results:")
print(df.head())

# Sample recommendation query using the embedding of a document
doc_id = "767"  # Replace with an actual document ID from your Vespa instance
emb = get_embedding(app, doc_id)
if emb:
    embedding_vector = emb["fields"]["embedding"]
    results = query_movies_by_embedding(app, embedding_vector)
    print("\nRecommendation Results:")
    print(results.head())
else:
    print(f"No embedding found for document ID {doc_id}")
