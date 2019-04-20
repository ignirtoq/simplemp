from argparse import ArgumentParser
from asyncio import get_event_loop
from logging import basicConfig, ERROR
from simplemp import Connection


prog = 'request'
desc = 'Example request script'


async def amain(url, topic, message, loop=None):
    loop = get_event_loop() if loop is None else loop
    response_received = False

    async with Connection(url, loop=loop) as client:
        async for response in await client.request(topic, message):
            print("'%s' response received: %s" % (topic, response))
            response_received = True

    if not response_received:
        print("No responders for '%s'" % topic)

    loop.stop()


def main(*, message, topic, url, verbose):
    setup_logging(verbose)
    loop = get_event_loop()
    loop.create_task(amain(url, topic, message))

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
