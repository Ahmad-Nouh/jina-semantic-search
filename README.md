# jina-semantic-search
Semantic Search Using Jina


# Environment
- Python 3.9.2

# Requirements
- jina==2.0.16
- sentence_transformers==2.0.0
- pandas==1.3.1
- Flask==1.1.2
- numpy==1.20.2
- requests==2.25.1

# Running
- Start the index & search flows by running the flask api:

```
    python app.py
```

- Start indexing data batches:
```
    python index.py
```

- Search by a query:
```
    python search.py {YOUR_QUERY}
```