import asyncio
import unittest


from simplemp.brokers import RequestResponseBroker


class AsyncMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_event_loop()

    def new_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        return self._loop

    def run_coroutine(self, coro):
        self._loop.run_until_complete(coro)


class RequestMessage(AsyncMixin, unittest.TestCase):
    topic = 'test topic'
    content = {'key': 'value'}
    expected_message = {
        'type': 'request',
        'topic': 'test topic',
    }

    expected_message_with_content = {
        'type': 'request',
        'topic': 'test topic',
        'content': content,
    }

    def define_send(self):
        future = self._loop.create_future()

        async def send(message):
            future.set_result(message)

        return send, future

    def verify_message(self, message, expected_message):
        self.assertTrue(isinstance(message, dict))
        self.assertTrue('sequence' in message)
        message.pop('sequence')
        self.assertDictEqual(message, expected_message)

    def test_request_message(self):
        loop = self.new_loop()
        send, fut = self.define_send()

        broker = RequestResponseBroker(send, loop)
        self.run_coroutine(broker.request(self.topic, lambda resp: None))
        self.assertTrue(fut.done())
        self.verify_message(fut.result(), self.expected_message)

    def test_request_message_with_content(self):
        loop = self.new_loop()
        send, fut = self.define_send()

        broker = RequestResponseBroker(send, loop)
        self.run_coroutine(broker.request(self.topic, lambda resp: None,
                                          content=self.content))
        self.assertTrue(fut.done())
        self.verify_message(fut.result(), self.expected_message_with_content)
