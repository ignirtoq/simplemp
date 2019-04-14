from argparse import ArgumentParser
from asyncio import get_event_loop
from logging import basicConfig, ERROR
from simplemp import connect


prog = 'respond'
desc = 'Example request responder script'


def handle_request(topic, content=None):
    print("'%s' request received: %s" % (topic, content))
    return '%s successfully processed' % topic


def main(*, topic, url, verbose):
    setup_logging(verbose)
    loop = get_event_loop()
    client = loop.run_until_complete(connect(url, loop=loop))
    loop.run_until_complete(client.register(topic, handle_request))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(client.disconnect())


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
    p.add_argument('-v', '--verbose', action='count',
                   help='verbose output; specify more times for more verbosity')

    return vars(p.parse_args())


if __name__ == '__main__':
    main(**parse_arguments())
