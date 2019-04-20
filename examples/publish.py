from argparse import ArgumentParser
from asyncio import get_event_loop
from logging import basicConfig, ERROR
from simplemp import Connection

prog = 'publish'
desc = 'Example publishing script'


async def publish(url, topic, message):
    with Connection(url) as client:
        await client.publish(topic, message)


def main(*, url, topic, message, verbose):
    setup_logging(verbose)
    get_event_loop().run_until_complete(publish(url, topic, message))


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
