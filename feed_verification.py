import pandas as pd
from vespa.application import Vespa
import json

def test_search():
    print("\nTesting Vespa Search")
    print("=" * 50)
    
    app = Vespa(url="http://localhost", port=8080)
    
    # Test 1: Basic document retrieval
    print("\n1. Testing direct document retrieval...")
    query = {
        "yql": "select * from sources * where true limit 1"
    }
    try:
        response = app.query(query)
        print(f"Response hits: {len(response.hits)}")
        if response.hits:
            print(f"Sample document: {json.dumps(response.hits[0], indent=2)}")
    except Exception as e:
        print(f"Error in test 1: {e}")
    
    # Test 2: Keyword search
    print("\n2. Testing keyword search...")
    query = {
        "yql": "select * from sources * where text contains 'Harry'",
        "hits": 5
    }
    try:
        response = app.query(query)
        print(f"Found {len(response.hits)} hits")
        for hit in response.hits:
            print(f"Title: {hit['fields'].get('title')}")
    except Exception as e:
        print(f"Error in test 2: {e}")
    
    # Test 3: Field search
    print("\n3. Testing field search...")
    query = {
        "yql": "select * from sources * where title contains 'Avatar'",
        "hits": 5
    }
    try:
        response = app.query(query)
        print(f"Found {len(response.hits)} hits")
        for hit in response.hits:
            print(f"Title: {hit['fields'].get('title')}")
    except Exception as e:
        print(f"Error in test 3: {e}")

if __name__ == "__main__":
    test_search()
