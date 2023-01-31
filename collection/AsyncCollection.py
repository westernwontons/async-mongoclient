from bson.raw_bson import RawBSONDocument
from pymongo.typings import _DocumentType
from pymongo.collection import Collection
from pymongo.client_session import ClientSession
from pymongo.results import InsertOneResult, InsertManyResult
from typing import Any, Iterable, Mapping
from typings import DocumentType, Pipeline


class AsyncCollection:
    def __init__(self, collection: Collection[DocumentType]) -> None:
        self._collection = collection

    async def aggregate(
        self,
        pipeline: Pipeline,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any = None,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        """Perform an aggregation using the aggregation framework on this
        collection.

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Collection`, except when ``$out`` or ``$merge`` are used on
        MongoDB <5.0, in which case
        :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY` is used.

        .. note:: This method does not support the 'explain' option. Please
           use :meth:`~pymongo.database.Database.command` instead. An
           example is included in the :ref:`aggregate-examples` documentation.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.

        :Parameters:
          - `pipeline`: a list of aggregation pipeline stages
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): extra `aggregate command`_ parameters.

        All optional `aggregate command`_ parameters should be passed as
        keyword arguments to this method. Valid options include, but are not
        limited to:

          - `allowDiskUse` (bool): Enables writing to temporary files. When set
            to True, aggregation stages can write data to the _tmp subdirectory
            of the --dbpath directory. The default is False.
          - `maxTimeMS` (int): The maximum amount of time to allow the operation
            to run in milliseconds.
          - `batchSize` (int): The maximum number of documents to return per
            batch. Ignored if the connected mongod or mongos does not support
            returning aggregate results using a cursor.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `let` (dict): A dict of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. ``"$$var"``). This option is
            only supported on MongoDB >= 5.0.
          - `comment` (optional): A user-provided comment to attach to this
            command.


        :Returns:
          A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.
           Added ``let`` parameter.
           Support $merge and $out executing on secondaries according to the
           collection's :attr:`read_preference`.
        .. versionchanged:: 4.0
           Removed the ``useCursor`` option.
        .. versionchanged:: 3.9
           Apply this collection's read concern to pipelines containing the
           `$out` stage when connected to MongoDB >= 4.2.
           Added support for the ``$merge`` pipeline stage.
           Aggregations that write always use read preference
           :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
        .. versionchanged:: 3.6
           Added the `session` parameter. Added the `maxAwaitTimeMS` option.
           Deprecated the `useCursor` option.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.0
           The :meth:`aggregate` method always returns a CommandCursor. The
           pipeline argument must be a list.

        .. seealso:: :doc:`/examples/aggregation`

        .. _aggregate command:
            https://mongodb.com/docs/manual/reference/command/aggregate
        """

        # ? if you're wondering why I'm not subclassing
        # ? and just adding the `to_list` to the type signature
        # ? then my answer is that this hacky *metaprogramming*
        # ? is something I'm not even gonna try to get into
        # ? If you do, you're welcome to change it
        return await self._collection.aggregate(
            pipeline,
            session,
            let,
            comment,
            **kwargs,
        ).to_list(  # type: ignore
            None
        )

    async def insert_one(
        self,
        document: DocumentType | RawBSONDocument,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        comment: Any | None = None,
    ) -> InsertOneResult:
        """Insert a single document.

          >>> db.test.count_documents({'x': 1})
          0
          >>> result = db.test.insert_one({'x': 1})
          >>> result.inserted_id
          ObjectId('54f112defba522406c9cc208')
          >>> db.test.find_one({'x': 1})
          {'x': 1, '_id': ObjectId('54f112defba522406c9cc208')}

        :Parameters:
          - `document`: The document to insert. Must be a mutable mapping
            type. If the document does not have an _id field one will be
            added automatically.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          - An instance of :class:`~pymongo.results.InsertOneResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """

        return await self._collection.insert_one(
            document, bypass_document_validation, session, comment  # type: ignore
        )  # type: ignore

    async def insert_many(
        self,
        documents: Iterable[_DocumentType | RawBSONDocument],
        ordered: bool = True,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        comment: Any | None = None,
    ) -> InsertManyResult:
        """
        Insert an iterable of documents.

        >>> db.test.count_documents({})
        0
        >>> result = db.test.insert_many([{'x': i} for i in range(2)])
        >>> result.inserted_ids
        [ObjectId('54f113fffba522406c9cc20e'), ObjectId('54f113fffba522406c9cc20f')]
        >>> db.test.count_documents({})
        2

        :Parameters:
          - `documents`: A iterable of documents to insert.
          - `ordered` (optional): If ``True`` (the default) documents will be
            inserted on the server serially, in the order provided. If an error
            occurs all remaining inserts are aborted. If ``False``, documents
            will be inserted on the server in arbitrary order, possibly in
            parallel, and all document inserts will be attempted.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          An instance of :class:`~pymongo.results.InsertManyResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """

        return await self._collection.insert_many(
            documents, ordered, bypass_document_validation, session, comment  # type: ignore
        )
