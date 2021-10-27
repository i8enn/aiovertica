# Copyright (c) 2018-2021 Micro Focus or one of its affiliates.
# Copyright (c) 2018 Uber Technologies, Inc.
# Copyright (c) 2021 Ivan Galin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


from __future__ import print_function, division, absolute_import

import asyncio
import base64
import logging
import socket
import ssl
import getpass
import types
import uuid
from struct import unpack
from collections import deque, namedtuple

# noinspection PyCompatibility,PyUnresolvedReferences
from typing import (
    Any, Awaitable, Callable, Coroutine, Dict, KeysView, Optional, Tuple, Type, Union,
    List, TYPE_CHECKING
)

from six import raise_from, string_types, integer_types, PY3

if not PY3:
    raise ValueError('Not supported python version. Supports only python3 and high.')

from urllib.parse import urlparse, parse_qs
import aiovertica
from aiovertica import errors, messages
from aiovertica.cursor import Cursor, CursorType
from aiovertica.messages.message import BackendMessage, FrontendMessage
from aiovertica.messages.frontend_messages import CancelRequest
from aiovertica.log import VerticaLogging

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 5433
DEFAULT_PASSWORD = ''
DEFAULT_AUTOCOMMIT = False
DEFAULT_BACKUP_SERVER_NODE = []
DEFAULT_KRB_SERVICE_NAME = 'vertica'
DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_PATH = 'aiovertica.log'
try:
    DEFAULT_USER = getpass.getuser()
except Exception as e:
    DEFAULT_USER = None
    print("WARN: Cannot get the login user name: {}".format(str(e)))


StreamRWs = Tuple[asyncio.StreamReader, asyncio.StreamWriter]


async def connect(**kwargs: KeysView[Any]) -> 'Connection':
    """Opens a new connection to a Vertica database."""
    conn = Connection(kwargs)
    return await conn()


def parse_dsn(dsn: str) -> Dict[str, Any]:
    """Parse connection string into a dictionary of keywords and values.
       Connection string format:
           vertica://<user>:<password>@<host>:<port>/<database>?k1=v1&k2=v2&...
    """
    url = urlparse(dsn)
    if url.scheme != 'vertica':
        raise ValueError("Only vertica:// scheme is supported.")

    # Ignore blank/invalid values
    result = {k: v for k, v in (
        ('host', url.hostname),
        ('port', url.port),
        ('user', url.username),
        ('password', url.password),
        ('database', url.path[1:])) if v
              }
    for key, values in parse_qs(url.query, keep_blank_values=True).items():
        # Try to get the last non-blank value in the list of values for each key
        for i in reversed(range(len(values))):
            value = values[i]
            if value != '':
                break

        if value == '' and key != 'log_path':
            # blank values are to be ignored
            continue
        elif key == 'backup_server_node':
            continue
        elif key in ('connection_load_balance', 'use_prepared_statements',
                     'disable_copy_local', 'ssl', 'autocommit'):
            lower = value.lower()
            if lower in ('true', 'on', '1'):
                result[key] = True
            elif lower in ('false', 'off', '0'):
                result[key] = False
        elif key == 'connection_timeout':
            result[key] = float(value)
        elif key == 'log_level' and value.isdigit():
            result[key] = int(value)
        else:
            result[key] = value

    return result

_AddressEntry = namedtuple('_AddressEntry', ['host', 'resolved', 'data'])

class _AddressList(object):
    def __init__(self, host, port, backup_nodes, logger):
        """Creates a new deque with the primary host first, followed by any backup hosts"""

        self._logger = logger

        # Items in address_deque are _AddressEntry values.
        #   host is the original hostname/ip, used by SSL option check_hostname
        #   - when resolved is False, data is port
        #   - when resolved is True, data is the 5-tuple from socket.getaddrinfo
        # This allows for lazy resolution. Seek peek() for more.
        self.address_deque = deque()

        # load primary host into address_deque
        self._append(host, port)

        # load backup nodes into address_deque
        if not isinstance(backup_nodes, list):
            err_msg = 'Connection option "backup_server_node" must be a list'
            self._logger.error(err_msg)
            raise TypeError(err_msg)

        # Each item in backup_nodes should be either
        # a host name or IP address string (using default port) or
        # a (host, port) tuple
        for node in backup_nodes:
            if isinstance(node, string_types):
                self._append(node, DEFAULT_PORT)
            elif isinstance(node, tuple) and len(node) == 2:
                self._append(node[0], node[1])
            else:
                err_msg = ('Each item of connection option "backup_server_node"'
                           ' must be a host string or a (host, port) tuple')
                self._logger.error(err_msg)
                raise TypeError(err_msg)

        self._logger.debug('Address list: {0}'.format(list(self.address_deque)))

    def _append(self, host, port):
        if not isinstance(host, string_types):
            err_msg = 'Host must be a string: invalid value: {0}'.format(host)
            self._logger.error(err_msg)
            raise TypeError(err_msg)

        if not isinstance(port, (string_types, integer_types)):
            err_msg = 'Port must be an integer or a string: invalid value: {0}'.format(port)
            self._logger.error(err_msg)
            raise TypeError(err_msg)
        elif isinstance(port, string_types):
            try:
                port = int(port)
            except ValueError as e:
                err_msg = 'Port "{0}" is not a valid string: {1}'.format(port, e)
                self._logger.error(err_msg)
                raise ValueError(err_msg)

        if port < 0 or port > 65535:
            err_msg = 'Invalid port number: {0}'.format(port)
            self._logger.error(err_msg)
            raise ValueError(err_msg)

        self.address_deque.append(_AddressEntry(host=host, resolved=False, data=port))

    def push(self, host, port):
        self.address_deque.appendleft(_AddressEntry(host=host, resolved=False, data=port))

    def pop(self):
        self.address_deque.popleft()

    def peek(self):
        # do lazy DNS resolution, returning the leftmost socket.getaddrinfo result
        if len(self.address_deque) == 0:
            return None

        while len(self.address_deque) > 0:
            self._logger.debug('Peek at address list: {0}'.format(list(self.address_deque)))
            entry = self.address_deque[0]
            if entry.resolved:
                # return a resolved sockaddrinfo
                return entry.data
            else:
                # DNS resolve a single host name to multiple IP addresses
                self.address_deque.popleft()
                host, port = entry.host, entry.data
                try:
                    resolved_hosts = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
                except Exception as e:
                    self._logger.warning('Error resolving host "{0}" on port {1}: {2}'.format(host, port, e))
                    continue

                # add resolved addrinfo (AF_INET and AF_INET6 only) to deque
                for addrinfo in reversed(resolved_hosts):
                    if addrinfo[0] in (socket.AF_INET, socket.AF_INET6):
                        self.address_deque.appendleft(_AddressEntry(
                            host=host, resolved=True, data=addrinfo))
        return None

    def peek_host(self):
        # returning the leftmost host result
        self._logger.debug('Peek host at address list: {0}'.format(list(self.address_deque)))
        if len(self.address_deque) == 0:
            return None
        return self.address_deque[0].host


def _generate_session_label() -> str:
    """Return generated session label"""
    return f'vertica-python-{aiovertica.__version__}-{uuid.uuid1()}'


class Connection:
    def __init__(self, options: Optional[Dict[str, Any]] = None):
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

        options = options or {}
        self.options = parse_dsn(options['dsn']) if 'dsn' in options else {}
        self.options.update({key: value for key, value in options.items() \
                             if key == 'log_path' or (key != 'dsn' and value is not None)})

        # Set up connection logger
        logger_name = f'vertica_{id(self)}_{uuid.uuid4()}'  # must be a unique value
        self._logger = logging.getLogger(logger_name)

        if 'log_level' not in self.options and 'log_path' not in self.options:
            # logger is disabled by default
            self._logger.disabled = True
        else:
            self.options.setdefault('log_level', DEFAULT_LOG_LEVEL)
            self.options.setdefault('log_path', DEFAULT_LOG_PATH)
            VerticaLogging.setup_logging(logger_name, self.options['log_path'],
                self.options['log_level'], id(self))

        self.options.setdefault('host', DEFAULT_HOST)
        self.options.setdefault('port', DEFAULT_PORT)
        if 'user' not in self.options:
            if DEFAULT_USER:
                self.options['user'] = DEFAULT_USER
            else:
                msg = 'Connection option "user" is required'
                self._logger.error(msg)
                raise KeyError(msg)
        self.options.setdefault('database', self.options['user'])
        self.options.setdefault('password', DEFAULT_PASSWORD)
        self.options.setdefault('autocommit', DEFAULT_AUTOCOMMIT)
        self.options.setdefault('session_label', _generate_session_label())
        self.options.setdefault('backup_server_node', DEFAULT_BACKUP_SERVER_NODE)
        self.options.setdefault('kerberos_service_name', DEFAULT_KRB_SERVICE_NAME)
        # Kerberos authentication hostname defaults to the host value here so
        # the correct value cannot be overwritten by load balancing or failover
        self.options.setdefault('kerberos_host_name', self.options['host'])

        self.address_list = _AddressList(self.options['host'], self.options['port'],
            self.options['backup_server_node'], self._logger)

        # we only support one cursor per connection
        self.options.setdefault('unicode_error', None)
        self._cursor = Cursor(self, self._logger, cursor_type=None,
            unicode_error=self.options['unicode_error'])

        # knob for using server-side prepared statements
        self.options.setdefault('use_prepared_statements', False)
        is_connection_prepared_statements = (
            'enabled' if self.options['use_prepared_statements'] else 'disabled'
        )
        self._logger.debug(
            f'Connection prepared statements is {is_connection_prepared_statements}'
        )

        # knob for disabling COPY LOCAL operations
        self.options.setdefault('disable_copy_local', False)
        is_disable_copy_local = (
            'disabled' if self.options['disable_copy_local'] else 'enabled'
        )
        self._logger.debug(f'COPY LOCAL operation is {is_disable_copy_local}')

        # Initially, for a new session, autocommit is off
        if self.options['autocommit']:
            self.autocommit = True

    async def __call__(self) -> 'Connection':
        self._logger.info(
            f'Connecting as user "{self.options["user"]}" '
            f'to database "{self.options["database"]}" '
            f'on host "{self.options["host"]}" with port "{self.options["port"]}"'
        )

        await self.startup_connection()

        self._logger.info('Connection is ready')
        return self

    #############################################
    # supporting `async with` statements
    #############################################
    async def __aenter__(self) -> 'Connection':
        return await self.__call__()

    async def __aexit__(
        self,
        type_: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[types.TracebackType]
    ) -> None:
        await self.close()

    #############################################
    # dbapi methods
    #############################################
    async def close(self) -> None:
        self._logger.info('Close the connection')
        try:
            await self.write(messages.Terminate())
        finally:
            await self.close_socket()

    async def commit(self) -> None:
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        cur = self.cursor()
        await cur.execute('COMMIT;')

    async def rollback(self) -> None:
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        cur = self.cursor()
        await cur.execute('ROLLBACK;')

    def cursor(
        self, cursor_type: Optional[CursorType] = None
    ) -> Cursor:
        if self.closed():
            raise errors.ConnectionError('Connection is closed')

        if self._cursor.closed():
            self._cursor._closed = False

        # let user change type if they want?
        self._cursor.cursor_type = cursor_type
        return self._cursor

    #############################################
    # non-dbapi methods
    #############################################
    @property
    def autocommit(self):
        """Read the connection's AUTOCOMMIT setting from cache"""
        return self.parameters.get('auto_commit', 'off') == 'on'

    @autocommit.setter
    def autocommit(self, value):
        """Change the connection's AUTOCOMMIT setting"""
        if self.autocommit is value:
            return
        val = 'on' if value else 'off'
        cur = self.cursor()
        cur.execute(
            f'SET SESSION AUTOCOMMIT TO {val}',
            use_prepared_statements=False
        )
        cur.fetchall()   # check for errors and update the cache

    async def cancel(self) -> None:
        """Cancel the current database operation. This can be called from a
           different thread than the one currently executing a database operation.
        """
        if self.closed():
            raise errors.ConnectionError('Connection is closed')
        self._logger.info('Canceling the current database operation')
        # Must create a new socket connection to the server
        _, writer = await self.establish_socket_connection(
            self.address_list
        )
        await self.write(CancelRequest(self.backend_pid, self.backend_key), writer)
        writer.close()
        await writer.wait_closed()

        self._logger.info('Cancel request issued')

    def opened(self) -> bool:
        return (self.writer is not None
                and self.backend_pid is not None
                and self.transaction_status is not None)

    def closed(self) -> bool:
        return not self.opened()

    def __str__(self) -> str:
        safe_options = {
            key: value
            for key, value in self.options.items()
            if key != 'password'
        }

        return f'<AIOVertica.Connection:{id(self)} {self.parameters=}' \
               f' {self.backend_pid=}, {self.backend_key=},' \
               f' {self.transaction_status=},' \
               f' socket={self._socket()}, options={safe_options}>'

    #############################################
    # internal
    #############################################
    def _socket(self) -> Optional[socket.socket]:
        if not self.writer:
            return None
        _socket: socket.socket = self.writer.get_extra_info("socket")
        return _socket

    def reset_values(self) -> None:
        self.parameters = {}
        self.session_id = None
        self.backend_pid = None
        self.backend_key = None
        self.transaction_status = None
        self.reader = None
        self.writer = None
        self.address_list = _AddressList(self.options['host'], self.options['port'],
            self.options['backup_server_node'], self._logger)

    async def _writer(self) -> asyncio.StreamWriter:
        if self.writer:
            return self.writer

        # the initial establishment of the client connection
        reader, writer = await self.establish_socket_connection(self.address_list)

        # enable load balancing
        load_balance_options = self.options.get('connection_load_balance')
        is_load_balance = 'enabled' if load_balance_options else 'disabled'
        self._logger.debug(f'Connection load balance option is {is_load_balance}')
        if load_balance_options:
            reader, writer = await self.balance_load(reader, writer)

        # enable SSL
        ssl_options = self.options.get('ssl')
        is_ssl_enabled = 'enabled' if ssl_options else 'disabled'
        self._logger.debug(f'SSL option is {is_ssl_enabled}')
        if ssl_options:
            reader, writer = await self.enable_ssl(reader, writer, ssl_options)

        self.reader = reader
        self.writer = writer

        return self.writer

    async def balance_load(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> StreamRWs:
        # Send load balance request and read server response
        self._logger.debug(f'=> {messages.LoadBalanceRequest()}')

        writer.write(messages.LoadBalanceRequest().get_message())
        await writer.drain()

        response = await reader.read(1)

        if response == b'Y':
            size = unpack('!I', await reader.read(4))[0]
            if size < 4:
                err_msg = f"Bad message size: {size}"
                self._logger.error(err_msg)
                raise errors.MessageError(err_msg)
            res = BackendMessage.from_type(
                type_=response, data=await reader.read(size - 4)
            )
            self._logger.debug(f'<= {res}')
            host = res.get_host()
            port = res.get_port()
            self._logger.info(
                f'Load balancing to host "{host}" on port {port}'
            )

            peer = writer.transport.get_extra_info('peername')
            if peer:
                socket_host, socket_port = peer[0], peer[1]
                if host == socket_host and port == socket_port:
                    self._logger.info(
                        f'Already connecting to host "{host}" on port {port}.'
                        f' Ignore load balancing.'
                    )
                    return reader, writer

            # Push the new host onto the address list before connecting again. Note that this
            # will leave the originally-specified host as the first failover possibility.
            self.address_list.push(host, port)
            writer.close()
            await writer.wait_closed()
            reader, writer = self.establish_socket_connection(self.address_list)
        else:
            self._logger.debug(f'<= LoadBalanceResponse: {response}')
            self._logger.warning("Load balancing requested but not supported by server")

        return reader, writer

    async def enable_ssl(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        ssl_options: Union[Dict[str, Any], ssl.SSLContext],
    ) -> StreamRWs:
        # Send SSL request and read server response
        self._logger.debug('=> %s', messages.SslRequest())
        writer.write(messages.SslRequest().get_message())
        await writer.drain()
        response = await reader.read(1)
        self._logger.debug(f'<= SslResponse: {response}')
        if response == b'S':
            self._logger.info('Enabling SSL')
            try:
                loop = asyncio.get_running_loop()
                if isinstance(ssl_options, ssl.SSLContext):
                    server_host = self.address_list.peek_host()
                    if server_host is None:   # This should not happen
                        msg = 'Cannot get the connected server host while enabling SSL'
                        self._logger.error(msg)
                        raise errors.ConnectionError(msg)
                    transport = await loop.start_tls(
                        writer.transport,
                        writer.transport.get_protocol(),
                        sslcontext=ssl_options,
                        server_hostname=server_host,
                    )
                    writer._transport = transport
                    reader.set_transport(transport)
                else:
                    transport = await loop.start_tls(
                        writer.transport,
                        writer.transport.get_protocol(),
                        sslcontext=ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                    )
                    writer._transport = transport
                    reader.set_transport(transport)
            except ssl.CertificateError as e:
                raise_from(errors.ConnectionError(str(e)), e)
            except ssl.SSLError as e:
                raise_from(errors.ConnectionError(str(e)), e)
        else:
            err_msg = "SSL requested but not supported by server"
            self._logger.error(err_msg)
            raise errors.SSLNotSupported(err_msg)

        self.writer = writer
        self.reader = reader
        return reader, writer

    async def establish_socket_connection(
        self,
        address_list: _AddressList
    ) -> StreamRWs:
        """
        Given a list of database node addresses, establish the socket
        connection to the database server. Return a connected socket object.
        """
        addrinfo = address_list.peek()
        writer = None
        last_exception = None

        # Failover: loop to try all addresses
        while addrinfo:
            (family, socktype, proto, canonname, sockaddr) = addrinfo
            last_exception = None

            # _AddressList filters all addrs to AF_INET and AF_INET6, which both
            # have host and port as values 0, 1 in the sockaddr tuple.
            host = sockaddr[0]
            port = sockaddr[1]

            self._logger.info(
                f'Establishing connection to host "{host}" on port {port}'
            )

            try:
                reader, writer = await asyncio.open_connection(
                    host=self.options['host'],
                    port=self.options['port'],
                )

                self.reader = reader
                self.writer = writer

                break
            except Exception as e:
                self._logger.info(
                    f'Failed to connect to host "{host}" on port {port}: {e}'
                )
                last_exception = e
                address_list.pop()
                addrinfo = address_list.peek()
                if writer:
                    writer.close()
                    await writer.wait_closed()

        # all of the addresses failed
        if (self.reader is None or self.writer is None) or last_exception:
            err_msg = 'Failed to establish a connection to the primary server or any backup address.'
            self._logger.error(err_msg)
            raise errors.ConnectionError(err_msg)

        return self.reader, self.writer

    def ssl(self) -> bool:
        return self.writer.get_extra_info('ssl_object') is not None

    async def write(
        self,
        message: FrontendMessage,
        writer: Optional[asyncio.StreamWriter] = None
    ) -> None:
        if not isinstance(message, FrontendMessage):
            raise TypeError(f"invalid message: ({message})")
        if writer is None:
            writer = await self._writer()

        self._logger.debug(f'=> {message}')
        try:
            for data in message.fetch_message():
                try:
                    writer.write(data)
                    await writer.drain()
                except Exception:
                    self._logger.error("couldn't send message")
                    raise

        except Exception as e:
            self.writer.close()
            await self.writer.wait_closed()
            self._logger.error(str(e))
            if isinstance(e, IOError):
                raise_from(errors.ConnectionError(str(e)), e)
            else:
                raise

    async def close_socket(self) -> None:
        try:
            if self.writer is not None:
                self.writer.close()
                await self.writer.wait_closed()
        finally:
            self.reset_values()

    async def reset_connection(self) -> None:
        await self.close_socket()
        await self.startup_connection()

    @staticmethod
    def is_asynchronous_message(
        message: Union[BackendMessage, FrontendMessage]
    ) -> bool:
        # Check if it is an asynchronous response message
        # Note: ErrorResponse is a subclass of NoticeResponse
        return (
            isinstance(message, messages.ParameterStatus)
            or (
                isinstance(message, messages.NoticeResponse)
                and not isinstance(message, messages.ErrorResponse)
            )
        )

    def handle_asynchronous_message(
        self, message: Union[BackendMessage, FrontendMessage]
    ) -> None:
        if isinstance(message, messages.ParameterStatus):
            if message.name == 'protocol_version':
                message.value = int(message.value)
            self.parameters[message.name] = message.value
        elif (isinstance(message, messages.NoticeResponse) and
              not isinstance(message, messages.ErrorResponse)):
            if getattr(self, 'notice_handler', None) is not None:
                self.notice_handler(message)
            else:
                self._logger.warning(message.error_message())

    async def read_string(self) -> bytearray:
        s = bytearray()
        while True:
            char = await self.read_bytes(1)
            if char == b'\x00':
                break
            s.extend(char)
        return s

    async def read_message(self) -> BackendMessage:
        while True:
            try:
                type_ = await self.read_bytes(1)
                size = unpack('!I', await self.read_bytes(4))[0]
                if size < 4:
                    raise errors.MessageError(f"Bad message size: {size}")
                if type_ == messages.WriteFile.message_id:
                    # The whole WriteFile message may not be read at here.
                    # Instead, only the file name and file length is read.
                    # This is because the message could be too large to read all at once.
                    f = await self.read_string()
                    filename = f.decode('utf-8')
                    file_length = unpack('!I', await self.read_bytes(4))[0]
                    size -= 4 + len(f) + 1 + 4
                    if size != file_length:
                        raise errors.MessageError(f"Bad message size: {size}")
                    if filename == '':
                        # If there is no filename, then this is really RETURNREJECTED data, not a rejected file
                        if file_length % 8 != 0:
                            raise errors.MessageError(
                                f"Bad RETURNREJECTED data size: {file_length}"
                            )
                        data = await self.read_bytes(file_length)
                        message = messages.WriteFile(filename, file_length, data)
                    else:
                        # The rest of the message is read later with write_to_disk()
                        message = messages.WriteFile(filename, file_length)
                else:
                    message = BackendMessage.from_type(
                        type_, await self.read_bytes(size - 4)
                    )
                self._logger.debug(f'<= {message}')
                self.handle_asynchronous_message(message)
                # handle transaction status
                if isinstance(message, messages.ReadyForQuery):
                    self.transaction_status = message.transaction_status
            except (SystemError, IOError) as e:
                await self.close_socket()
                # noinspection PyTypeChecker
                self._logger.error(e)
                raise_from(errors.ConnectionError(str(e)), e)
            if not self.is_asynchronous_message(message):
                break
        return message

    async def read_expected_message(
        self,
        expected_types: Union[messages.BindComplete, Tuple[messages.BindComplete]],
        error_handler: Optional[
            Callable[[Any], Coroutine[Any, Any, None]]
        ] = None
    ) -> None:
        # Reads a message and does some basic error handling.
        # expected_types must be a class (e.g. messages.BindComplete) or a tuple of classes
        message = self.read_message()
        if isinstance(message, expected_types):
            return message
        elif isinstance(message, messages.ErrorResponse):
            if error_handler is not None:
                await error_handler(message)
            else:
                raise errors.DatabaseError(message.error_message())
        else:
            msg = f'Received unexpected message type: {type(message)}.'
            if isinstance(expected_types, tuple):
                stringify_expected_types = ", ".join(
                    [t.__name__ for t in expected_types]
                )
                msg += f'Expected types: {stringify_expected_types}'
            else:
                msg += f'Expected type: {expected_types.__name__}'
            self._logger.error(msg)
            raise errors.MessageError(msg)

    async def read_bytes(self, n: int) -> bytes:
        if n == 1:
            result = await self.reader.read(1)
            if not result:
                raise errors.ConnectionError("Connection closed by Vertica")
        else:
            result = await self.reader.readexactly(n)
            if len(result) == 0:
                raise errors.ConnectionError("Connection closed by Vertica")
        return result

    async def send_GSS_response_and_receive_challenge(self, response):
        # Send the GSS response data to the vertica server
        token = base64.b64decode(response)
        await self.write(messages.Password(token, messages.Authentication.GSS))
        # Receive the challenge from the vertica server
        message = self.read_expected_message(messages.Authentication)
        if message.code != messages.Authentication.GSS_CONTINUE:
            msg = ('Received unexpected message type: Authentication(type={}).'
                   ' Expected type: Authentication(type={})'.format(
                message.code, messages.Authentication.GSS_CONTINUE))
            self._logger.error(msg)
            raise errors.MessageError(msg)
        return message.auth_data

    def make_GSS_authentication(self):
        try:
            import kerberos
        except ImportError as e:
            raise errors.ConnectionError("{}\nCannot make a Kerberos "
                                         "authentication because no Kerberos package is installed. "
                                         "Get it with 'pip install kerberos'.".format(str(e)))

        # Set GSS flags
        gssflag = (kerberos.GSS_C_DELEG_FLAG | kerberos.GSS_C_MUTUAL_FLAG |
                   kerberos.GSS_C_SEQUENCE_FLAG | kerberos.GSS_C_REPLAY_FLAG)

        # Generate the GSS-style service principal name
        service_principal = "{}@{}".format(self.options['kerberos_service_name'],
            self.options['kerberos_host_name'])

        # Initializes a context object with a service principal
        self._logger.info('Initializing a context for GSSAPI client-side '
                          'authentication with service principal {}'.format(service_principal))
        try:
            result, context = kerberos.authGSSClientInit(service_principal, gssflags=gssflag)
        except kerberos.GSSError as err:
            msg = "GSSAPI initialization error: {}".format(str(err))
            self._logger.error(msg)
            raise errors.KerberosError(msg)
        if result != kerberos.AUTH_GSS_COMPLETE:
            msg = ('Failed to initialize a context for GSSAPI client-side '
                   'authentication with service principal {}'.format(service_principal))
            self._logger.error(msg)
            raise errors.KerberosError(msg)

        # Processes GSSAPI client-side steps
        try:
            challenge = b''
            while True:
                self._logger.info('Processing a single GSSAPI client-side step')
                challenge = base64.b64encode(challenge).decode("utf-8")
                result = kerberos.authGSSClientStep(context, challenge)

                if result == kerberos.AUTH_GSS_COMPLETE:
                    self._logger.info('Result: GSSAPI step complete')
                    break
                elif result == kerberos.AUTH_GSS_CONTINUE:
                    self._logger.info('Result: GSSAPI step continuation')
                    # Get the response from the last successful GSSAPI client-side step
                    response = kerberos.authGSSClientResponse(context)
                    challenge = self.send_GSS_response_and_receive_challenge(response)
                else:
                    msg = "GSSAPI client-side step error status {}".format(result)
                    self._logger.error(msg)
                    raise errors.KerberosError(msg)
        except kerberos.GSSError as err:
            msg = "GSSAPI client-side step error: {}".format(str(err))
            self._logger.error(msg)
            raise errors.KerberosError(msg)

    async def startup_connection(self) -> None:
        user = self.options['user']
        database = self.options['database']
        session_label = self.options['session_label']
        os_user_name = DEFAULT_USER if DEFAULT_USER else ''
        password = self.options['password']

        await self.write(messages.Startup(user, database, session_label, os_user_name))

        while True:
            message = await self.read_message()

            if isinstance(message, messages.Authentication):
                if message.code == messages.Authentication.OK:
                    self._logger.info(
                        f"User {self.options['user']} successfully authenticated"
                    )
                elif message.code == messages.Authentication.CHANGE_PASSWORD:
                    msg = f"The password for user {self.options['user']} has expired"
                    self._logger.error(msg)
                    raise errors.ConnectionError(msg)
                elif message.code == messages.Authentication.PASSWORD_GRACE:
                    self._logger.warning(
                        f"The password for user {self.options['user']}"
                        f" will expire soon. Please consider changing it."
                    )
                elif message.code == messages.Authentication.GSS:
                    self.make_GSS_authentication()
                else:
                    await self.write(
                        messages.Password(
                            password,
                            message.code,
                            {
                                'user': user,
                                'salt': getattr(message, 'salt', None),
                                'usersalt': getattr(message, 'usersalt', None)
                            }
                        )
                    )
            elif isinstance(message, messages.BackendKeyData):
                self.backend_pid = message.pid
                self.backend_key = message.key
            elif isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.ErrorResponse):
                self._logger.error(message.error_message())
                raise errors.ConnectionError(message.error_message())
            else:
                msg = f"Received unexpected startup message: {message}"
                self._logger.error(msg)
                raise errors.MessageError(msg)
