from uuid import uuid4


FIELD_TYPE = 'type'
FIELD_TOPIC = 'topic'
FIELD_SEQUENCE = 'sequence'
FIELD_CONTENT = 'content'

TYPE_REGISTRATION = 'registration'
TYPE_UNREGISTRATION = 'unregistration'
TYPE_REQUEST = 'request'
TYPE_RESPONSE = 'response'

TYPE_PUBLICATION = 'publication'
TYPE_SUBSCRIPTION = 'subscription'
TYPE_UNSUBSCRIPTION = 'unsubscription'


def create_message(type, topic, sequence=None, content=None):
    message = {
        FIELD_TYPE: type,
        FIELD_TOPIC: topic,
    }
    if sequence is not None:
        message[FIELD_SEQUENCE] = sequence
    if content is not None:
        message[FIELD_CONTENT] = content
    return message


def create_register_message(topic):
    return create_message(TYPE_REGISTRATION, topic)


def create_unregister_message(topic):
    return create_message(TYPE_UNREGISTRATION, topic)


def create_request_message(topic, sequence, content=None):
    return create_message(TYPE_REQUEST, topic, sequence=sequence,
                          content=content)


def create_response_message(topic, sequence, content=None):
    return create_message(TYPE_RESPONSE, topic, sequence=sequence,
                          content=content)


def create_publish_message(topic, content=None):
    return create_message(TYPE_PUBLICATION, topic, content=content)


def create_subscribe_message(topic):
    return create_message(TYPE_SUBSCRIPTION, topic)


def create_unsubscribe_message(topic):
    return create_message(TYPE_UNSUBSCRIPTION, topic)


def get_new_sequence():
    return str(uuid4())


def get_message_sequence(message):
    return message.get(FIELD_SEQUENCE)


def get_message_topic(message):
    return message.get(FIELD_TOPIC)


def get_message_type(message):
    return message.get(FIELD_TYPE)


def unpack_message(message):
    return (message.get(FIELD_TYPE), message.get(FIELD_TOPIC),
            message.get(FIELD_SEQUENCE), message.get(FIELD_CONTENT))
