from typing import Any
from typings import CodecOptions, DocumentType
from pymongo.client_session import ClientSession
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.database import Database
from pymongo.read_preferences import _ServerMode as ServerMode
from collection import AsyncCollection


class AsyncDatabase:
    def __init__(self, database: Database[DocumentType]) -> None:
        self._database = database

    def get_collection(
        self,
        name: str,
        codec_options: CodecOptions[DocumentType] | None = None,
        read_preference: ServerMode | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
    ) -> AsyncCollection:
        """Get a :class:`~pymongo.collection.Collection` with the given name
        and options.

        Useful for creating a :class:`~pymongo.collection.Collection` with
        different codec options, read preference, and/or write concern from
        this :class:`Database`.

          >>> db.read_preference
          Primary()
          >>> coll1 = db.test
          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = db.get_collection(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> coll2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `name`: The name of the collection - a string.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
        """

        collection = self._database.get_collection(
            name,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
        )
        return AsyncCollection(collection)

    def create_collection(
        self,
        name: str,
        codec_options: CodecOptions[DocumentType] | None = None,
        read_preference: ServerMode | None = None,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        session: ClientSession | None = None,
        check_exists: bool | None = True,
        **kwargs: Any,
    ):
        """Create a new :class:`~pymongo.collection.Collection` in this
        database.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.

        :Parameters:
          - `name`: the name of the collection to create
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - ``check_exists`` (optional): if True (the default), send a listCollections command to
            check if the collection already exists before creation.
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the `create collection command`_

        All optional `create collection command`_ parameters should be passed
        as keyword arguments to this method. Valid options include, but are not
        limited to:

          - ``size`` (int): desired initial size for the collection (in
            bytes). For capped collections this size is the max
            size of the collection.
          - ``capped`` (bool): if True, this is a capped collection
          - ``max`` (int): maximum number of objects if capped (optional)
          - ``timeseries`` (dict): a document specifying configuration options for
            timeseries collections
          - ``expireAfterSeconds`` (int): the number of seconds after which a
            document in a timeseries collection expires
          - ``validator`` (dict): a document specifying validation rules or expressions
            for the collection
          - ``validationLevel`` (str): how strictly to apply the
            validation rules to existing documents during an update.  The default level
            is "strict"
          - ``validationAction`` (str): whether to "error" on invalid documents
            (the default) or just "warn" about the violations but allow invalid
            documents to be inserted
          - ``indexOptionDefaults`` (dict): a document specifying a default configuration
            for indexes when creating a collection
          - ``viewOn`` (str): the name of the source collection or view from which
            to create the view
          - ``pipeline`` (list): a list of aggregation pipeline stages
          - ``comment`` (str): a user-provided comment to attach to this command.
            This option is only supported on MongoDB >= 4.4.
          - ``encryptedFields`` (dict): **(BETA)** Document that describes the encrypted fields for
            Queryable Encryption. For example::

                {
                  "escCollection": "enxcol_.encryptedCollection.esc",
                  "eccCollection": "enxcol_.encryptedCollection.ecc",
                  "ecocCollection": "enxcol_.encryptedCollection.ecoc",
                  "fields": [
                      {
                          "path": "firstName",
                          "keyId": Binary.from_uuid(UUID('00000000-0000-0000-0000-000000000000')),
                          "bsonType": "string",
                          "queries": {"queryType": "equality"}
                      },
                      {
                          "path": "ssn",
                          "keyId": Binary.from_uuid(UUID('04104104-1041-0410-4104-104104104104')),
                          "bsonType": "string"
                      }
                    ]
                }
          - ``clusteredIndex`` (dict): Document that specifies the clustered index
            configuration. It must have the following form::

                {
                    // key pattern must be {_id: 1}
                    key: <key pattern>, // required
                    unique: <bool>, // required, must be `true`
                    name: <string>, // optional, otherwise automatically generated
                    v: <int>, // optional, must be `2` if provided
                }
          - ``changeStreamPreAndPostImages`` (dict): a document with a boolean field ``enabled`` for
            enabling pre- and post-images.

        .. versionchanged:: 4.2
           Added the ``check_exists``, ``clusteredIndex``, and  ``encryptedFields`` parameters.

        .. versionchanged:: 3.11
           This method is now supported inside multi-document transactions
           with MongoDB 4.4+.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Added the collation option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.

        .. _create collection command:
            https://mongodb.com/docs/manual/reference/command/create
        """

        return self._database.create_collection(
            name,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
            session,
            check_exists,
            **kwargs,
        )
