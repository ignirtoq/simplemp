from argparse import ArgumentParser
from asyncio import Future, get_event_loop
from functools import partial
from logging import basicConfig, ERROR
from simplemp import connect


prog = 'request'
desc = 'Example request script'


def print_response(handled_future: Future, topic, message=None):
    print("'%s' response received: %s" % (topic, message))
    handled_future.set_result(None)


async def setup(url, topic, message, loop=None):
    loop = get_event_loop() if loop is None else loop

    handled_future = loop.create_future()
    response_handler = partial(print_response, handled_future)

    client = await connect(url, loop=loop)
    await client.request(topic, response_handler, message)
    loop.create_task(stop_on_complete(handled_future, client, loop))


async def stop_on_complete(future, client, loop):
    await future
    await client.disconnect()
    loop.stop()


def main(*, message, topic, url, verbose):
    setup_logging(verbose)
    loop = get_event_loop()

    loop.run_until_complete(setup(url, topic, message, loop=loop))
    try:
        loop.run_forever()
    finally:
        loop.close()


def setup_logging(verbosity):
    if verbosity is not None:
        level = ERROR - verbosity*10
        form = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
        basicConfig(level=level, format=form)


def parse_arguments():
    p = ArgumentParser(prog, description=desc)
    p.add_argument('-u', '--url', required=True,
                   help='url to host server (e.g. tcp://host:port)')
    p.add_argument('-t', '--topic', help='message topic', default='test')
    p.add_argument('-m', '--message', help='message content string',
                   default='content')
    p.add_argument('-v', '--verbose', action='count',
                   help='verbose output; specify more times for more verbosity')

    return vars(p.parse_args())


if __name__ == '__main__':
    main(**parse_arguments())
