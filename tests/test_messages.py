import unittest

from simplemp.messages import (create_publish_message, create_register_message,
                               create_request_message,
                               create_response_message,
                               create_subscribe_message,
                               create_unregister_message,
                               create_unsubscribe_message)


class PublishMessage(unittest.TestCase):
    topic = 'test topic'
    content = ['test content']
    expected_message = {
        'type': 'publication',
        'topic': topic,
    }
    expected_message_with_content = {
        'type': 'publication',
        'topic': topic,
        'content': content,
    }

    def test_create(self):
        message = create_publish_message(self.topic)
        self.assertDictEqual(message, self.expected_message)

    def test_create_with_content(self):
        message = create_publish_message(self.topic, content=self.content)
        self.assertDictEqual(message, self.expected_message_with_content)


class RegisterMessage(unittest.TestCase):
    topic = 'test topic'
    expected_message = {
        'type': 'registration',
        'topic': topic,
    }

    def test_create(self):
        message = create_register_message(self.topic)
        self.assertDictEqual(message, self.expected_message)


class RequestMessage(unittest.TestCase):
    topic = 'test topic'
    sequence = 'test sequence'
    content = ['test content']
    expected_message = {
        'type': 'request',
        'topic': topic,
        'sequence': sequence,
    }
    expected_message_with_content = {
        'type': 'request',
        'topic': topic,
        'sequence': sequence,
        'content': content,
    }

    def test_create(self):
        message = create_request_message(self.topic, self.sequence)
        self.assertDictEqual(message, self.expected_message)

    def test_create_with_content(self):
        message = create_request_message(self.topic, self.sequence,
                                         content=self.content)
        self.assertDictEqual(message, self.expected_message_with_content)


class ResponseMessage(unittest.TestCase):
    topic = 'test topic'
    sequence = 'test sequence'
    content = ['test content']
    expected_message = {
        'type': 'response',
        'topic': topic,
        'sequence': sequence,
    }
    expected_message_with_content = {
        'type': 'response',
        'topic': topic,
        'sequence': sequence,
        'content': content,
    }

    def test_create(self):
        message = create_response_message(self.topic, self.sequence)
        self.assertDictEqual(message, self.expected_message)

    def test_create_with_content(self):
        message = create_response_message(self.topic, self.sequence,
                                          content=self.content)
        self.assertDictEqual(message, self.expected_message_with_content)


class SubscribeMessage(unittest.TestCase):
    topic = 'test topic'
    expected_message = {
        'type': 'subscription',
        'topic': topic,
    }

    def test_create(self):
        message = create_subscribe_message(self.topic)
        self.assertDictEqual(message, self.expected_message)


class UnregisterMessage(unittest.TestCase):
    topic = 'test topic'
    expected_message = {
        'type': 'unregistration',
        'topic': topic,
    }

    def test_create(self):
        message = create_unregister_message(self.topic)
        self.assertDictEqual(message, self.expected_message)


class UnsubscribeMessage(unittest.TestCase):
    topic = 'test topic'
    expected_message = {
        'type': 'unsubscription',
        'topic': topic,
    }

    def test_create(self):
        message = create_unsubscribe_message(self.topic)
        self.assertDictEqual(message, self.expected_message)
