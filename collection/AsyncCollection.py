from enum import Enum
from bson.raw_bson import RawBSONDocument
from pymongo.collection import Collection
from pymongo.client_session import ClientSession
from pymongo.results import (
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    BulkWriteResult,
    DeleteResult,
)
from pymongo.collation import Collation
from typing import Any, Iterable, Mapping, NoReturn, Sequence, cast
from typings import DocumentType, Pipeline, CollationIn, IndexKeyHint, WriteOp

try:
    from database import AsyncDatabase
except ImportError:
    import sys

    AsyncDatabase = sys.modules[f"{__package__}.AsyncDatabase"]


class ReturnDocument(Enum):
    """An enum used with
    :meth:`~pymongo.collection.Collection.find_one_and_replace` and
    :meth:`~pymongo.collection.Collection.find_one_and_update`.
    """

    BEFORE = False
    """Return the original document before it was updated/replaced, or
    ``None`` if no document matches the query.
    """
    AFTER = True
    """Return the updated/replaced or inserted document."""


class AsyncCollection:
    def __init__(self, collection: Collection[DocumentType]) -> None:
        self._collection: Collection[DocumentType] = collection

    def __getattr__(self, name: str) -> "AsyncCollection":
        return cast(AsyncCollection, self._collection.__getitem__(name))

    def __getitem__(self, name: str) -> "AsyncCollection":
        return cast(AsyncCollection, self._collection.__getitem__(name))

    def __repr__(self) -> str:
        return (
            f"AsyncCollection({self._collection.__database}, {self._collection.__name})"
        )

    def __eq__(self, other: Any) -> bool:
        return self._collection.__eq__(other)

    def __ne__(self, other: Any) -> bool:
        return self._collection.__ne__(other)

    def __hash__(self) -> int:
        return self._collection.__hash__()

    def __bool__(self) -> NoReturn:
        return self._collection.__bool__()

    @property
    def full_name(self) -> str:
        """
        The full name of this :class:`Collection`.

        The full name is of the form `database_name.collection_name`.
        """

        return self._collection.__full_name

    @property
    def name(self) -> str:
        """
        The name of this :class:`Collection`.
        """
        return self._collection.__name

    @property
    def database(self) -> AsyncDatabase:  # type: ignore
        """The :class:`~pymongo.database.Database` that this
        :class:`Collection` is a part of.
        """

        return cast(AsyncDatabase, self._collection.__database)  # type: ignore

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
        documents: Iterable[DocumentType | RawBSONDocument],
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

    async def replace_one(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: CollationIn | None = None,
        hint: IndexKeyHint | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> UpdateResult:
        """Replace a single document matching the filter.

        >>> for doc in db.test.find({}):
        ...     print(doc)
        ...
        {'x': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}
        >>> result = db.test.replace_one({'x': 1}, {'y': 1})
        >>> result.matched_count
        1
        >>> result.modified_count
        1
        >>> for doc in db.test.find({}):
        ...     print(doc)
        ...
        {'y': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}

        The *upsert* option can be used to insert a new document if a matching
        document does not exist.

        >>> result = db.test.replace_one({'x': 1}, {'x': 1}, True)
        >>> result.matched_count
        0
        >>> result.modified_count
        0
        >>> result.upserted_id
        ObjectId('54f11e5c8891e756a6e1abd4')
        >>> db.test.find_one({'x': 1})
        {'x': 1, '_id': ObjectId('54f11e5c8891e756a6e1abd4')}

        :Parameters:
        - `filter`: A query that matches the document to replace.
        - `replacement`: The new document.
        - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
        - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
        - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
        - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
        - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        - `comment` (optional): A user-provided comment to attach to this
            command.
        :Returns:
        - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 4.1
        Added ``let`` parameter.
        Added ``comment`` parameter.
        .. versionchanged:: 3.11
        Added ``hint`` parameter.
        .. versionchanged:: 3.6
        Added ``session`` parameter.
        .. versionchanged:: 3.4
        Added the `collation` option.
        .. versionchanged:: 3.2
        Added bypass_document_validation support.

        .. versionadded:: 3.0
        """

        return await self._collection.replace_one(
            filter,
            replacement,
            upsert,
            bypass_document_validation,
            collation,
            hint,
            session,
            let,
            comment,
        )  # type: ignore

    async def bulk_write(
        self,
        requests: Sequence[WriteOp[DocumentType]],
        ordered: bool = True,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        comment: Any | None = None,
        let: Mapping[str, Any] | None = None,
    ) -> BulkWriteResult:
        """Send a batch of write operations to the server.

        Requests are passed as a list of write operation instances (
        :class:`~pymongo.operations.InsertOne`,
        :class:`~pymongo.operations.UpdateOne`,
        :class:`~pymongo.operations.UpdateMany`,
        :class:`~pymongo.operations.ReplaceOne`,
        :class:`~pymongo.operations.DeleteOne`, or
        :class:`~pymongo.operations.DeleteMany`).

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634ef')}
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634f0')}
          >>> # DeleteMany, UpdateOne, and UpdateMany are also available.
          ...
          >>> from pymongo import InsertOne, DeleteOne, ReplaceOne
          >>> requests = [InsertOne({'y': 1}), DeleteOne({'x': 1}),
          ...             ReplaceOne({'w': 1}, {'z': 1}, upsert=True)]
          >>> result = db.test.bulk_write(requests)
          >>> result.inserted_count
          1
          >>> result.deleted_count
          1
          >>> result.modified_count
          0
          >>> result.upserted_ids
          {2: ObjectId('54f62ee28891e756a6e1abd5')}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634f0')}
          {'y': 1, '_id': ObjectId('54f62ee2fba5226811f634f1')}
          {'z': 1, '_id': ObjectId('54f62ee28891e756a6e1abd5')}

        :Parameters:
          - `requests`: A list of write operations (see examples above).
          - `ordered` (optional): If ``True`` (the default) requests will be
            performed on the server serially, in the order provided. If an error
            occurs all remaining operations are aborted. If ``False`` requests
            will be performed on the server in arbitrary order, possibly in
            parallel, and all operations will be attempted.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").

        :Returns:
          An instance of :class:`~pymongo.results.BulkWriteResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 4.1
           Added ``comment`` parameter.
           Added ``let`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """

        return await self._collection.bulk_write(
            requests,  # type: ignore
            ordered,
            bypass_document_validation,
            session,
            comment,
            let,
        )  # type: ignore

    async def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: Mapping[str, Any] | Collation | None = None,
        array_filters: Sequence[Mapping[str, Any]] | None = None,
        hint: str | Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> UpdateResult:
        """Update a single document matching the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        If ``upsert=True`` and no documents match the filter, create a
        new document based on the filter criteria and update modifications.

          >>> result = db.test.update_one({'x': -10}, {'$inc': {'x': 3}}, upsert=True)
          >>> result.matched_count
          0
          >>> result.modified_count
          0
          >>> result.upserted_id
          ObjectId('626a678eeaa80587d4bb3fb7')
          >>> db.test.find_one(result.upserted_id)
          {'_id': ObjectId('626a678eeaa80587d4bb3fb7'), 'x': -7}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the ``update``.
        .. versionchanged:: 3.6
           Added the ``array_filters`` and ``session`` parameters.
        .. versionchanged:: 3.4
          Added the ``collation`` option.
        .. versionchanged:: 3.2
          Added ``bypass_document_validation`` support.

        .. versionadded:: 3.0
        """

        return await self._collection.update_one(
            filter,
            update,
            upsert,
            bypass_document_validation,
            collation,
            array_filters,
            hint,
            session,
            let,
            comment,
        )  # type: ignore

    async def update_many(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        upsert: bool = False,
        array_filters: Sequence[Mapping[str, Any]] | None = None,
        bypass_document_validation: bool | None = None,
        collation: Mapping[str, Any] | Collation | None = None,
        hint: str | Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> UpdateResult:
        """Update one or more documents that match the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          3
          >>> result.modified_count
          3
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 4, '_id': 1}
          {'x': 4, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation` (optional): If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added ``array_filters`` and ``session`` parameters.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionchanged:: 3.2
          Added bypass_document_validation support.

        .. versionadded:: 3.0
        """

        return await self._collection.update_many(
            filter,
            update,
            upsert,
            array_filters,
            bypass_document_validation,
            collation,
            hint,
            session,
            let,
            comment,
        )  # type: ignore

    async def delete_one(
        self,
        filter: Mapping[str, Any],
        collation: Mapping[str, Any] | Collation | None = None,
        hint: str | Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> DeleteResult:
        """Delete a single document matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_one({'x': 1})
          >>> result.deleted_count
          1
          >>> db.test.count_documents({'x': 1})
          2

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionadded:: 3.0
        """

        return await self._collection.delete_one(
            filter, collation, hint, session, let, comment
        )  # type: ignore

    async def delete_many(
        self,
        filter: Mapping[str, Any],
        collation: Mapping[str, Any] | Collation | None = None,
        hint: str | Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
    ) -> DeleteResult:
        """Delete one or more documents matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_many({'x': 1})
          >>> result.deleted_count
          3
          >>> db.test.count_documents({'x': 1})
          0

        :Parameters:
          - `filter`: A query that matches the documents to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
          - `comment` (optional): A user-provided comment to attach to this
            command.

        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionadded:: 3.0
        """

        return await self._collection.delete_many(
            filter, collation, hint, session, let, comment
        )  # type: ignore

    async def find(
        self,
        filter: Mapping[str, Any] | None = None,
        projection: Any | None = None,
        skip: int = 0,
        limit: int = 0,
        no_cursor_timeout: bool = False,
        oplog_replay: bool = False,
        batch_size: int = 0,
        collation: Mapping[str, Any] | Collation | None = None,
        hint: str | Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        max_scan: Any | None = None,
        max_time_ms: int | None = None,
        max: int | None = None,
        min: int | None = None,
        return_key: bool = False,
        show_record_id: bool = False,
        snapshot: bool = False,
        comment: Any | None = None,
        session: ClientSession | None = None,
        allow_disk_use: Any | None = None,
    ):
        """Query the database.

        The `filter` argument is a query document that all results
        must match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `projection` argument is used to specify a subset
        of fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~pymongo.cursor.Cursor` corresponding to this query.

        The :meth:`find` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `filter` (optional): A query document that selects which documents
            to include in the result set. Can be an empty document to include
            all documents.
          - `projection` (optional): a list of field names that should be
            returned in the result set or a dict specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return. A limit of 0 (the default) is equivalent to setting no
            limit.
          - `no_cursor_timeout` (optional): if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
          - `cursor_type` (optional): the type of cursor to return. The valid
            options are defined by :class:`~pymongo.cursor.CursorType`:

            - :attr:`~pymongo.cursor.CursorType.NON_TAILABLE` - the result of
              this find call will return a standard cursor over the result set.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE` - the result of this
              find call will be a tailable cursor - tailable cursors are only
              for use with capped collections. They are not closed when the
              last data is retrieved but are kept open and the cursor location
              marks the final document position. If more data is received
              iteration of the cursor will continue from the last document
              received. For details, see the `tailable cursor documentation
              <https://www.mongodb.com/docs/manual/core/tailable-cursors/>`_.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` - the result
              of this find call will be a tailable cursor with the await flag
              set. The server will wait for a few seconds after returning the
              full result set so that it can capture and return additional data
              added during the query.
            - :attr:`~pymongo.cursor.CursorType.EXHAUST` - the result of this
              find call will be an exhaust cursor. MongoDB will stream batched
              results to the client without waiting for the client to request
              each batch, reducing latency. See notes on compatibility below.

          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `allow_partial_results` (optional): if True, mongos will return
            partial results if some shards are down instead of returning an
            error.
          - `oplog_replay` (optional): **DEPRECATED** - if True, set the
            oplogReplay query flag. Default: False.
          - `batch_size` (optional): Limits the number of documents returned in
            a single batch.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `return_key` (optional): If True, return only the index keys in
            each document.
          - `show_record_id` (optional): If True, adds a field ``$recordId`` in
            each document with the storage engine's internal record identifier.
          - `snapshot` (optional): **DEPRECATED** - If True, prevents the
            cursor from returning a document more than once because of an
            intervening write operation.
          - `hint` (optional): An index, in the same format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.hint` on the cursor to tell Mongo the
            proper index to use for the query.
          - `max_time_ms` (optional): Specifies a time limit for a query
            operation. If the specified time is exceeded, the operation will be
            aborted and :exc:`~pymongo.errors.ExecutionTimeout` is raised. Pass
            this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_time_ms` on the cursor.
          - `max_scan` (optional): **DEPRECATED** - The maximum number of
            documents to scan. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_scan` on the cursor.
          - `min` (optional): A list of field, limit pairs specifying the
            inclusive lower bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.min` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
          - `max` (optional): A list of field, limit pairs specifying the
            exclusive upper bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
          - `comment` (optional): A string to attach to the query to help
            interpret and trace the operation in the server logs and in profile
            data. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.comment` on the cursor.
          - `allow_disk_use` (optional): if True, MongoDB may use temporary
            disk files to store data exceeding the system memory limit while
            processing a blocking sort operation. The option has no effect if
            MongoDB can satisfy the specified sort using an index, or if the
            blocking sort requires less memory than the 100 MiB limit. This
            option is only supported on MongoDB 4.4 and above.

        .. note:: There are a number of caveats to using
          :attr:`~pymongo.cursor.CursorType.EXHAUST` as cursor_type:

          - The `limit` option can not be used with an exhaust cursor.

          - Exhaust cursors are not supported by mongos and can not be
            used with a sharded cluster.

          - A :class:`~pymongo.cursor.Cursor` instance created with the
            :attr:`~pymongo.cursor.CursorType.EXHAUST` cursor_type requires an
            exclusive :class:`~socket.socket` connection to MongoDB. If the
            :class:`~pymongo.cursor.Cursor` is discarded without being
            completely iterated the underlying :class:`~socket.socket`
            connection will be closed and discarded without being returned to
            the connection pool.

        .. versionchanged:: 4.0
           Removed the ``modifiers`` option.
           Empty projections (eg {} or []) are passed to the server as-is,
           rather than the previous behavior which substituted in a
           projection of ``{"_id": 1}``. This means that an empty projection
           will now return the entire document, not just the ``"_id"`` field.

        .. versionchanged:: 3.11
           Added the ``allow_disk_use`` option.
           Deprecated the ``oplog_replay`` option. Support for this option is
           deprecated in MongoDB 4.4. The query engine now automatically
           optimizes queries against the oplog without requiring this
           option to be set.

        .. versionchanged:: 3.7
           Deprecated the ``snapshot`` option, which is deprecated in MongoDB
           3.6 and removed in MongoDB 4.0.
           Deprecated the ``max_scan`` option. Support for this option is
           deprecated in MongoDB 4.0. Use ``max_time_ms`` instead to limit
           server-side execution time.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.5
           Added the options ``return_key``, ``show_record_id``, ``snapshot``,
           ``hint``, ``max_time_ms``, ``max_scan``, ``min``, ``max``, and
           ``comment``.
           Deprecated the ``modifiers`` option.

        .. versionchanged:: 3.4
           Added support for the ``collation`` option.

        .. versionchanged:: 3.0
           Changed the parameter names ``spec``, ``fields``, ``timeout``, and
           ``partial`` to ``filter``, ``projection``, ``no_cursor_timeout``,
           and ``allow_partial_results`` respectively.
           Added the ``cursor_type``, ``oplog_replay``, and ``modifiers``
           options.
           Removed the ``network_timeout``, ``read_preference``, ``tag_sets``,
           ``secondary_acceptable_latency_ms``, ``max_scan``, ``snapshot``,
           ``tailable``, ``await_data``, ``exhaust``, ``as_class``, and
           slave_okay parameters.
           Removed ``compile_re`` option: PyMongo now always
           represents BSON regular expressions as :class:`~bson.regex.Regex`
           objects. Use :meth:`~bson.regex.Regex.try_compile` to attempt to
           convert from a BSON regular expression to a Python regular
           expression object.
           Soft deprecated the ``manipulate`` option.

        .. seealso:: The MongoDB documentation on `find <https://dochub.mongodb.org/core/find>`_.
        """

        return await self._collection.find(
            filter,
            projection,
            skip,
            limit,
            no_cursor_timeout,
            oplog_replay,
            batch_size,
            collation,
            hint,
            max_scan,
            max_time_ms,
            max,
            min,
            return_key,
            show_record_id,
            snapshot,
            comment,
            session,
            allow_disk_use,
        )  # type: ignore

    async def find_one(
        self, filter: Mapping[str, Any] | None, *args: Any, **kwargs: Any
    ):
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        The :meth:`find_one` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:

          - `filter` (optional): a dictionary specifying
            the query to be performed OR any other type to be used as
            the value for a query for ``"_id"``.

          - `*args` (optional): any additional positional arguments
            are the same as the arguments to :meth:`find`.

          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to :meth:`find`.

              >>> collection.find_one(max_time_ms=100)
        """

        return await self._collection.find_one(filter, *args, **kwargs)  # type: ignore

    async def find_one_and_delete(
        self,
        filter: Mapping[str, Any],
        projection: Mapping[str, Any] | Iterable[str] | None = None,
        sort: Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        hint: str | Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> DocumentType:
        """Finds a single document and deletes it, returning the document.

          >>> db.test.count_documents({'x': 1})
          2
          >>> db.test.find_one_and_delete({'x': 1})
          {'x': 1, '_id': ObjectId('54f4e12bfba5220aa4d6dee8')}
          >>> db.test.count_documents({'x': 1})
          1

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'x': 1}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> db.test.find_one_and_delete(
          ...     {'x': 1}, sort=[('_id', pymongo.DESCENDING)])
          {'x': 1, '_id': 2}

        The *projection* option can be used to limit the fields returned.

          >>> db.test.find_one_and_delete({'x': 1}, projection={'_id': False})
          {'x': 1}

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `projection` (optional): a list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is deleted.
          - `hint` (optional): An index to use to support the query predicate
            specified either by its string name, or in the same format as
            passed to :meth:`~pymongo.collection.Collection.create_index`
            (e.g. ``[('field', ASCENDING)]``). This option is only supported
            on MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 4.1
           Added ``let`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionadded:: 3.0
        """

        return await self._collection.find_one_and_delete(
            filter, projection, sort, hint, session, let, comment, **kwargs
        )  # type: ignore

    async def find_one_and_replace(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        projection: Mapping[str, Any] | Iterable[str] | None = None,
        sort: Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        upsert: bool = False,
        return_document: ReturnDocument = ReturnDocument.BEFORE,
        hint: Sequence[tuple[str, int | str | Mapping[str, Any]]] | None = None,
        session: ClientSession | None = None,
        let: Mapping[str, Any] | None = None,
        comment: Any | None = None,
        **kwargs: Any,
    ) -> DocumentType:
        """Finds a single document and replaces it, returning either the
        original or the replaced document.

        The :meth:`find_one_and_replace` method differs from
        :meth:`find_one_and_update` by replacing the document matched by
        *filter*, rather than modifying the existing document.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> db.test.find_one_and_replace({'x': 1}, {'y': 1})
          {'x': 1, '_id': 0}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The replacement document.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is replaced.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was replaced, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the replaced
            or inserted document.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `let` (optional): Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 4.1
           Added ``let`` parameter.
        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
           Added the ``collation`` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """

        return await self._collection.find_one_and_replace(
            filter,
            replacement,
            projection,
            sort,
            upsert,
            return_document,  # type: ignore
            hint,
            session,
            let,
            comment,
            **kwargs,
        )  # type: ignore

    async def count_documents(
        self,
        filter: Mapping[str, Any],
        session: ClientSession | None = None,
        comment: Any | None = None,
        skip: int = 0,
        limit: int = 0,
        collation: Collation | None = None,
        **kwargs: Any,
    ) -> int:
        """Count the number of documents in this collection.

        .. note:: For a fast count of the total documents in a collection see
           :meth:`estimated_document_count`.

        The :meth:`count_documents` method is supported in a transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `skip` (int): The number of matching documents to skip before
            returning results.
          - `limit` (int): The maximum number of documents to count. Must be
            a positive integer. If not provided, no limit is imposed.
          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (string or list of tuples): The index to use. Specify either
            the index name as a string or the index specification as a list of
            tuples (e.g. [('a', pymongo.ASCENDING), ('b', pymongo.ASCENDING)]).

        The :meth:`count_documents` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        .. note:: When migrating from :meth:`count` to :meth:`count_documents`
           the following query operators must be replaced:

           +-------------+-------------------------------------+
           | Operator    | Replacement                         |
           +=============+=====================================+
           | $where      | `$expr`_                            |
           +-------------+-------------------------------------+
           | $near       | `$geoWithin`_ with `$center`_       |
           +-------------+-------------------------------------+
           | $nearSphere | `$geoWithin`_ with `$centerSphere`_ |
           +-------------+-------------------------------------+

        :Parameters:
          - `filter` (required): A query document that selects which documents
            to count in the collection. Can be an empty document to count all
            documents.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `**kwargs` (optional): See list of options above.


        .. versionadded:: 3.7

        .. _$expr: https://mongodb.com/docs/manual/reference/operator/query/expr/
        .. _$geoWithin: https://mongodb.com/docs/manual/reference/operator/query/geoWithin/
        .. _$center: https://mongodb.com/docs/manual/reference/operator/query/center/
        .. _$centerSphere: https://mongodb.com/docs/manual/reference/operator/query/centerSphere/
        """

        return await self._collection.count_documents(
            filter,
            session,
            comment,
            skip=skip,
            limit=limit,
            collation=collation,
            **kwargs,
        )  # type: ignore
