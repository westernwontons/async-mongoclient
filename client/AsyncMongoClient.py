"""
A thin wrapper around `AsyncIOMotorClient` that provides proper types
"""

import pymongo.database
import pymongo.client_session
import pymongo.read_preferences
import pymongo.write_concern
import pymongo.read_concern
import pymongo.typings
import pymongo.command_cursor
import pymongo.client_options
import pymongo.topology_description
import bson.codec_options
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Any, Mapping, NamedTuple, NoReturn, Sequence, TypeVar


CodecOptionsType = TypeVar("CodecOptionsType", bound=Mapping[str, Any])


class MongoAddress(NamedTuple):
    host: str
    port: str


class AsyncMongoClient:
    """
    Parameters
    ----------
    `url` `str` Connection string for a running MongoDB instance
    `io_loop` `Any | None` Special event loop instance to use instead of default.
    """

    # ? The `Any` here is temporary
    def __init__(self, url: str, io_loop: Any | None = None) -> None:
        if io_loop is not None:
            self._async_motor_client = AsyncIOMotorClient(url, io_loop=io_loop)
        self._async_motor_client = AsyncIOMotorClient(url)

    @property
    def HOST(self) -> str:
        """
        Create a new string object from the given object.
        If encoding or errors is specified, then the object must expose a data buffer that will be decoded using the given encoding and error handler.
        Otherwise, returns the result of `object.__str__()` (if defined) or `repr(object)`. encoding defaults to `sys.getdefaultencoding()`.
        errors defaults to `strict`.
        """

        return self._async_motor_client.HOST

    @property
    def PORT(self) -> int:
        """
        Convert a number or string to an integer, or return 0 if no arguments are given.
        If x is a number, return x.__int__().
        For floating point numbers, this truncates towards zero.
        If x is not a number or if base is given, then x must be a string, bytes, or bytearray instance representing an integer literal in the given base.
        The literal can be preceded by `+` or `-` and be surrounded by whitespace.
        The base defaults to 10. Valid bases are 0 and 2-36.
        Base 0 means to interpret the base from the string as an integer literal. >>> int('0b100', base=0) 4
        """

        return self._async_motor_client.PORT

    @property
    def address(self) -> MongoAddress | None:
        """
        `MongoAddress(host: str, port: str)` of the current standalone, primary, or mongos, or None.
        Accessing address raises `InvalidOperation` if the client is load-balancing among mongooses, since there is no single address. Use nodes instead.
        If the client is not connected, this will block until a connection is established or raise `ServerSelectionTimeoutError` if no server is available.
        """

        if self._async_motor_client.address is None:
            return None
        address = self._async_motor_client.address
        return MongoAddress(host=address[0], port=address[1])

    @property
    def arbiters(self) -> Sequence[MongoAddress]:
        """
        Arbiters in the replica set.
        A sequence of MongoAddress(host: str, port: str). Empty if this client is not connected to a replica set, there are no arbiters, or this client was created without the replicaSet option.
        """

        arbiters = self._async_motor_client.arbiters
        if len(arbiters) == 0:
            return []
        return list(
            map(
                lambda arbiter: MongoAddress(host=arbiter[0], port=arbiter[1]),
                self._async_motor_client.arbiters,
            )
        )

    @property
    def close(self) -> Any:  # TODO: remove Any
        """
        Cleanup client resources and disconnect from `MongoDB`.
        End all server sessions created by this client by sending one or more `endSessions` commands.
        Close all sockets in the connection pools and stop the monitor threads.

        Changed in version 4.0: Once closed, the client cannot be used again and any attempt will raise `InvalidOperation`.
        Changed in version 3.6: End all server sessions created by this client.
        """

        return self._async_motor_client.close

    @property
    def codec_options(self) -> bson.codec_options.CodecOptions[CodecOptionsType]:
        """
        Read only access to the CodecOptions of this instance.
        """

        return self._async_motor_client.codec_options

    @codec_options.setter
    def codec_options(self) -> NoReturn:
        raise AttributeError("codec_options is read-only")

    @property
    def is_mongos(self) -> Any:  # TODO: remove Any
        """
        If this client is connected to mongos.
        If the client is not connected, this will block until a connection is established or raise `ServerSelectionTimeoutError` if no server is available.
        """

        return self._async_motor_client.is_mongos

    @property
    def is_primary(self) -> bool:
        """
        If this client is connected to a server that can accept writes.
        True if the current server is a standalone, mongos, or the primary of a replica set.
        If the client is not connected, this will block until a connection is established or raise `ServerSelectionTimeoutError` if no server is available.
        """

        return self._async_motor_client.is_primary

    @property
    def nodes(self) -> Any:  # TODO: remove Any
        """
        Set of all currently connected servers.

        Warning
        -------
        When connected to a replica set the value of nodes can change over time as MongoClient's view of the replica set changes.
        nodes can also be an empty set when MongoClient is first instantiated and hasn't yet connected to any servers, or a network partition causes it to lose connection to all servers.
        """

        return self._async_motor_client.nodes

    @property
    def options(self) -> pymongo.client_options.ClientOptions:
        """
        The configuration options for this client.

        Returns
        -------
        An instance of `ClientOptions`.
        """

        return self._async_motor_client.options

    @property
    def read_concern(self) -> pymongo.read_concern.ReadConcern:
        """
        Read only access to the `ReadConcern` of this instance.
        """
        return self._async_motor_client.read_concern

    @read_concern.getter
    def read_concern(self) -> NoReturn:
        raise AttributeError("read_concern is read-only")

    @property
    def read_preference(self) -> pymongo.read_preferences.ReadPreference:
        """
        Read only access to the read preference of this instance.
        """

        return self._async_motor_client.read_preference

    @read_preference.setter
    def read_preference(self) -> NoReturn:
        raise AttributeError("read_preference is read_only")

    @property
    def secondaries(self) -> Sequence[MongoAddress]:
        """
        The secondary members known to this client.
        A sequence of MongoAddress(host: str, port: str).
        Empty if this client is not connected to a replica set, there are no visible secondaries, or this client was created without the `replicaSet` option.

        New in version 3.0: `MongoClient` gained this property in version 3.0.
        """

        secondaries = self._async_motor_client.secondaries
        if len(secondaries) == 0:
            return []
        return list(
            map(
                lambda secondary: MongoAddress(host=secondary[0], port=secondary[1]),
                self._async_motor_client.secondaries,
            )
        )

    @property
    def topology_description(
        self,
    ) -> pymongo.topology_description.TopologyDescription:
        """
        The description of the connected MongoDB deployment.

        Note that the description is periodically updated in the background but the returned object itself is immutable.
        Access this property again to get a more recent `TopologyDescription`.

        Returns
        -------
        An instance of `TopologyDescription`
        """

        return self._async_motor_client.topology_description

    @property
    def write_concern(self) -> pymongo.write_concern.WriteConcern:
        """
        Read only access to the WriteConcern of this instance.
        """

        return self._async_motor_client.write_concern

    @write_concern.setter
    def write_concern(self) -> NoReturn:
        raise AttributeError("write_concern is read-only")

    def get_db_name(self, db_name: str) -> str:
        """
        Get the `db_name` on `AsyncIOMotorClient` `client`

        Parameters
        ----------
        `db_name`: Name of the database

        Raise
        -----
        `InvalidName` if an invalid database name is used
        """

        return self._async_motor_client[db_name]

    async def drop_database(
        self,
        name_or_database: str
        | pymongo.database.Database[pymongo.typings._DocumentType],
        session: pymongo.client_session.ClientSession | None = None,
        comment: str | None = None,
    ) -> None:
        """
        Drop a database.

        Parameters
        ----------
        `name_or_database`: the name of a database to drop, or a Database instance representing the database to drop
        `session` (optional): a `ClientSession`
        `comment` (optional): A user-provided comment to attach to this command

        Raise
        -----
        Raises `TypeError` if `name_or_database` is not an instance of `str` or `Database`.

        Note: The `write_concern` of this client is automatically applied to this operation.
        """

        return await self._async_motor_client.drop_database(
            name_or_database, session, comment
        )

    def get_database(
        self,
        name: str | None = None,
        codec_options: bson.codec_options.CodecOptions[CodecOptionsType] | None = None,
        read_preference: pymongo.read_preferences._ServerMode | None = None,
        write_concern: pymongo.write_concern.WriteConcern | None = None,
        read_concern: pymongo.read_concern.ReadConcern | None = None,
    ) -> pymongo.database.Database[pymongo.typings._DocumentType]:
        """
        Get a `MotorDatabase` with the given name and options.

        Useful for creating a MotorDatabase with different codec options, read preference, and/or write concern from this `MotorClient`.

        Parameters
        ----------
        `name`: The name of the database - a string.
        `codec_options (optional)`: An instance of CodecOptions. If `None` (the default) the codec_options of this `MotorClient` is used.
        `read_preference (optional)`: The read preference to use. If `None` (the default) the read_preference of this `MotorClient` is used. See `read_preferences` for options.
        `write_concern (optional)`: An instance of `WriteConcern`. If `None` (the default) the write_concern of this `MotorClient` is used.
        """

        return self._async_motor_client.get_database(
            name, codec_options, read_preference, write_concern, read_concern
        )

    def get_default_database(
        self,
        default: str | None = None,
        codec_options: bson.codec_options.CodecOptions[CodecOptionsType] | None = None,
        read_preference: pymongo.read_preferences._ServerMode | None = None,
        write_concern: pymongo.write_concern.WriteConcern | None = None,
        read_concern: pymongo.read_concern.ReadConcern | None = None,
        comment: Any | None = None,
    ) -> pymongo.database.Database[pymongo.typings._DocumentType]:
        """
        Get the database named in the MongoDB connection URI.
        Useful in scripts where you want to choose which database to use based only on the URI in a configuration file.

        Parameters
        ----------
        `default (optional)`: the database name to use if no database name was provided in the URI.
        `codec_options (optional)`: An instance of CodecOptions. If None (the default) the codec_options of this `MotorClient` is used.
        `read_preference (optional)`: The read preference to use. If None (the default) the read_preference of this `MotorClient` is used. See read_preferences for options.
        `write_concern (optional)`: An instance of WriteConcern. If None (the default) the write_concern of this `MotorClient` is used.
        `read_concern (optional)`: An instance of ReadConcern. If None (the default) the read_concern of this `MotorClient` is used.
        `comment (optional)`: A user-provided comment to attach to this command.
        """

        return self._async_motor_client.get_default_database(
            default,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
            comment,
        )

    async def list_database_names(
        self,
        session: pymongo.client_session.ClientSession | None = None,
        comment: Any | None = None,
    ) -> list[str]:
        """
        Get a list of the names of all databases on the connected server.

        Parameters
        ----------
        `session (optional)`: a `ClientSession`.
        `comment (optional)`: A user-provided comment to attach to this command.
        """

        return await self._async_motor_client.list_database_names(session, comment)

    async def list_databases(
        self,
        session: pymongo.client_session.ClientSession | None = None,
        comment: Any | None = None,
        **kwargs: Any
    ) -> pymongo.command_cursor.CommandCursor[dict[str, Any]]:
        """
        Get a list of the names of all databases on the connected server.

        `session (optional)`: a `ClientSession`.
        `comment (optional)`: A user-provided comment to attach to this command.
        `**kwargs (optional)`: Optional parameters of the `listDatabases` command can be passed as keyword arguments to this method. The supported options differ by server version.

        Returns
        -------
        An instance of `CommandCursor`
        """

        return await self._async_motor_client.list_databases(session, comment, **kwargs)

    async def server_info(
        self,
        session: pymongo.client_session.ClientSession | None = None,
    ) -> dict[str, Any]:
        """
        Get information about the MongoDB server we're connected to.

        Parameters
        ----------
        `session (optional)`: a `ClientSession`
        """

        return await self._async_motor_client.server_info(session)

    async def start_session(
        self,
        causal_consistency: bool | None = None,
        default_transaction_options: pymongo.client_session.TransactionOptions
        | None = None,
        snapshot: bool | None = False,
    ) -> pymongo.client_session.ClientSession:
        """
        Start a logical session.

        This method takes the same parameters as `PyMongo's` `SessionOptions`. See the `client_session` module for details.
        This session is created uninitialized, use it in an await expression to initialize it, or an async with statement.

        Parameters
        ----------
        `causal_consistency (optional)`: If `True`, read operations are causally ordered within the session. Defaults to `True` when the snapshot option is `False`.
        `default_transaction_options (optional)`: The default `TransactionOptions` to use for transactions started on this session.
        `snapshot (optional)`: If `True`, then all reads performed using this session will read from the same snapshot. This option is incompatible with `causal_consistency=True`. Defaults to `False`.

        Returns
        -------
        An instance of `MotorClientSession`
        """

        return await self._async_motor_client.start_session(
            causal_consistency, default_transaction_options, snapshot
        )

    # ! Type signature in documentation doesn't have types
    # ! I'll get back to this later
    def watch(self):
        raise NotImplementedError()
