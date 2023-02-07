from typing import Any, Mapping, Sequence
from bson.codec_options import CodecOptions as _CodecOptions
from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne
from pymongo.typings import _DocumentType as DocumentType
from pymongo.typings import _Pipeline as Pipeline
from pymongo.typings import _CollationIn as CollationIn

CodecOptions = _CodecOptions[DocumentType]
IndexList = Sequence[tuple[str, int | str | Mapping[str, Any]]]
IndexKeyHint = str | IndexList
WriteOp = (
    InsertOne[DocumentType]
    | DeleteOne
    | DeleteMany
    | ReplaceOne[DocumentType]
    | UpdateOne
    | UpdateMany
)
