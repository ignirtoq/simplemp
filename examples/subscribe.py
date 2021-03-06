from argparse import ArgumentParser
from asyncio import get_event_loop
from logging import basicConfig, ERROR
from simplemp import connect


prog = 'subscribe'
desc = 'Example subscription script'


async def setup(url, loop):
    return await connect(url, loop=loop)


async def amain(client, topic):
    async for message in await client.subscribe(topic):
        print("received '%s' message '%s'" % (topic, message))


async def shutdown(client, topic):
    await client.unsubscribe(topic)
    await client.disconnect()


def main(*, topic, url, verbose):
    setup_logging(verbose)
    loop = get_event_loop()
    client = loop.run_until_complete(setup(url, loop))

    try:
        loop.run_until_complete(amain(client, topic))
    except KeyboardInterrupt:
        loop.run_until_complete(shutdown(client, topic))
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
    p.add_argument('-v', '--verbose', action='count',
                   help='verbose output; specify more times for more verbosity')

    return vars(p.parse_args())


if __name__ == '__main__':
    main(**parse_arguments())
