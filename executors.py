import numpy as np
from jina import Executor, requests, DocumentArray
from sentence_transformers import SentenceTransformer


class DocumentEncoder(Executor):
    def __init__(self, encoder_name='distiluse-base-multilingual-cased-v1', **kwargs):
        super().__init__(**kwargs)
        self.model = SentenceTransformer(encoder_name)

    @requests
    def encode(self, docs: DocumentArray, *args, **kwargs) -> None:
        text_batch = np.stack(docs.get_attributes('text'))
        embeds = self.model.encode(text_batch)
        for d, e in zip(docs, embeds):
            d.embedding = e
