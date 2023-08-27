from mimetypes import init
from typing import Type, Callable, Collection, Generic, Optional
from uuid import UUID, uuid4
import aio_pika
from aio_pika import ExchangeType
import logging
from enum import Enum
from pydantic import BaseModel
import multiprocessing as mp
import asyncio
from ardennes.serial import SeralizationHandler
from inspect import signature

log = logging.getLogger(__name__)


class HandlerType(Enum):
    SCATTER = "SCATTER"
    GATHER = "GATHER"
    TRANSFORM = "TRANSFORM"
    SUBSCRIBE = "SUBSCRIBE"
    CONSUME = "CONSUME"


HANDLER_EXCHANGE_TYPE_MAP = {
    HandlerType.SCATTER: ExchangeType.DIRECT,
    HandlerType.GATHER: ExchangeType.DIRECT,
    HandlerType.TRANSFORM: ExchangeType.DIRECT,
    HandlerType.SUBSCRIBE: ExchangeType.FANOUT,
    HandlerType.CONSUME: ExchangeType.DIRECT,
}


def get_model_queue_name(model: Type[BaseModel]):
    return model.__name__


def get_exchange_name(model: Type[BaseModel], exchange_type: ExchangeType):
    return f"{get_model_queue_name(model)}_{exchange_type.value}"


def get_exchange_names(model: Type[BaseModel]):
    result = {}
    exchange_types = [ExchangeType.DIRECT, ExchangeType.FANOUT]
    for exchange_type in exchange_types:
        result[exchange_type] = get_exchange_name(model, exchange_type)
    return result


class Handler:
    def __init__(
        self,
        input_model: Type[BaseModel],
        output_model: Optional[Type[BaseModel]],
        callback: Callable,
        parent: "Ardennes",
    ):
        self.handler_id = uuid4()
        self.input_model = input_model
        self.output_model = output_model
        self.callback = callback
        self.parent = parent
        log.debug(f"Created {self._pretty}.")

    async def invoke_callback(self, input_data):
        result = self.callback(input_data)
        log.debug(f"Result: {type(result).__name__}({result})")
        return result

    async def start(self):
        log.debug(f"Starting {self._pretty}")
        await self._initialize()
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    input_data = self.parent.serialization_handler.deserialize(
                        message.body
                    )
                    await self.invoke_callback(input_data)

    @property
    def _pretty(self) -> str:
        input_name = self.input_model.__name__
        try:
            output_name = self.output_model.__name__
        except AttributeError:
            output_name = "None"

        return f"{type(self).__name__} {self.handler_id}: {input_name} -> {output_name}"

    async def _initialize(self):
        log.debug(f"Initializing {self._pretty}")
        await self._ensure_exchange()
        await self._ensure_queue()
        await self._bind()

    async def _bind(self):
        log.debug(f"Binding queue {self.queue} to exchange {self.exchange}.")
        await self.queue.bind(self.exchange, "*")

    def get_exchange_type(self) -> ExchangeType:
        return ExchangeType.DIRECT

    async def _ensure_exchange(self):
        exchange_type = self.get_exchange_type()
        exchange_name = get_exchange_name(self.input_model, exchange_type)
        log.debug(f"Declaring exchange {exchange_name} for {self._pretty}")
        self.exchange = await self.parent.channel.declare_exchange(
            exchange_name, exchange_type
        )

    async def _ensure_queue(self):
        queue_name = get_model_queue_name(self.input_model)
        log.debug(f"Declaring queue {queue_name} for {self._pretty}")
        self.queue = await self.parent.channel.declare_queue(
            queue_name, auto_delete=True
        )


class ScatterHandler(Handler):
    async def invoke_callback(self, input_data):
        results = await super().invoke_callback(input_data)
        for result in results:
            await self.parent.produce(result)
        return results


class GatherHandler(Handler):
    async def invoke_callback(self, input_data):
        result = await super().invoke_callback(input_data)
        await self.parent.produce(result)
        return result

    async def start(self):
        raise NotImplementedError()
        log.debug(f"Starting {self._pretty}")
        await self._initialize()
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    input_data = self.parent.serialization_handler.deserialize(
                        message.body
                    )
                    await self.invoke_callback(input_data)


class TransformHandler(Handler):
    async def invoke_callback(self, input_data):
        result = await super().invoke_callback(input_data)
        await self.parent.produce(result)
        return result


class SubscribeHandler(Handler):
    def get_exchange_type(self) -> ExchangeType:
        return ExchangeType.DIRECT

    async def invoke_callback(self, input_data):
        result = await super().invoke_callback(input_data)
        return result


class ConsumeHandler(Handler):
    async def invoke_callback(self, input_data):
        result = await super().invoke_callback(input_data)
        return result


class Ardennes:
    def __init__(
        self,
        concurrency: Optional[int] = None,
        serialization_handler: Optional[SeralizationHandler] = None,
    ):
        self.concurrency = concurrency
        self.handlers: Collection[Handler] = []
        if serialization_handler is None:
            serialization_handler = SeralizationHandler()
        self.serialization_handler = serialization_handler
        self.initialized = False

    async def _open_connection(self):
        log.debug(f"Opening connection.")
        self.connection = await aio_pika.connect_robust()
        await self._on_connection_opened()

    async def _on_connection_opened(self):
        log.debug(f"Connection opened.")
        await self._open_channel()
        await self._on_channel_opened()

    async def _open_channel(self):
        log.debug(f"Opening channel.")
        self.channel = await self.connection.channel()

    async def _on_channel_opened(self):
        log.debug(f"Channel opened.")

    async def produce(self, message: BaseModel):
        """
        Generate a novel message.

        Input: n/a
        Output: one
        Consumes input: n/a
        """
        await self.initialize()
        async with self.connection:
            message_type = type(message).__name__
            log.debug(f"Producing {message_type}.")
            exchange_names = get_exchange_names(type(message))
            num_published = 0
            for exchange_type, exchange_name in exchange_names.items():
                log.debug(f"Trying exchange {exchange_name} for {message_type}.")
                try:
                    exchange = await self.channel.declare_exchange(
                        exchange_name, exchange_type
                    )
                    serial = self.serialization_handler.serialize(message)
                    await exchange.publish(aio_pika.Message(body=serial), "*")
                    log.debug(
                        f"Successfully published {message_type} to exchange {exchange_name}."
                    )
                    num_published += 1
                except (
                    aio_pika.exceptions.ChannelClosed,
                    aio_pika.exceptions.ChannelInvalidStateError,
                    asyncio.exceptions.CancelledError,
                ):
                    log.debug(
                        f"Exchange {exchange_name} does not exist or no queues bound; skipping."
                    )

        log.debug(f"Published {message_type} to {num_published} exchanges.")

    def scatter(self, input_model: Type[BaseModel], output_model: Type[BaseModel]):
        """
        Scatter an input message into a set of output messages.

        Input: one
        Output: many
        Consumes input: yes
        """

        def inner(callback: Callable[[Type[BaseModel]], Collection[Type[BaseModel]]]):
            handler = ScatterHandler(input_model, output_model, callback, self)
            self.handlers.append(handler)

        return inner

    def gather(self, input_model: Type[BaseModel], output_model: Type[BaseModel]):
        """
        Gather a set of input messages based on a

        Input: many
        Output: zero or one
        Consumes input: yes
        """

        def inner(callback: Callable[[Collection[Type[BaseModel]]], Type[BaseModel]]):
            handler = GatherHandler(input_model, output_model, callback, self)
            self.handlers.append(handler)

        return inner

    def transform(self, input_model: Type[BaseModel], output_model: Type[BaseModel]):
        """
        Input: one
        Output: one
        Consumes input: yes
        """

        def inner(callback: Callable[[Type[BaseModel]], Type[BaseModel]]):
            handler = TransformHandler(input_model, output_model, callback, self)
            self.handlers.append(handler)

        return inner

    def subscribe(self, input_model: Type[BaseModel]):
        """
        Input: one
        Output: zero
        Consumes input: no
        """

        def inner(callback: Callable[[Type[BaseModel]], None]):
            handler = SubscribeHandler(input_model, None, callback, self)
            self.handlers.append(handler)

        return inner

    def consume(self, input_model: Type[BaseModel]):
        """
        Input: one
        Output: zero
        Consumes input: yes
        """

        def inner(callback: Callable[[Type[BaseModel]], None]):
            handler = ConsumeHandler(input_model, None, callback, self)
            self.handlers.append(handler)

        return inner

    async def _start_handlers(self):
        log.debug(f"Starting handlers.")
        coros = [handler.start() for handler in self.handlers]
        await asyncio.gather(*coros)

    async def initialize(self):
        if self.initialized:
            return
        else:
            await self._open_connection()
            self.initialized = True

    async def _start(self):
        await self.initialize()
        async with self.connection:
            await self._start_handlers()

    async def start(self):
        log.debug(f"Starting app.")
        await self._start()
