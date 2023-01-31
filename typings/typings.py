from bson.codec_options import CodecOptions as _CodecOptions
from pymongo.typings import _DocumentType as DocumentType
from pymongo.typings import _Pipeline as Pipeline

CodecOptions = _CodecOptions[DocumentType]
