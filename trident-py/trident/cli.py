#!/usr/bin/env python3

import asyncio
import sys

import fire
from termcolor import colored

from trident import TridentClient


async def client_cli(base_url: str = 'http://127.0.0.1', timeout: float = 3.0):
    try:
        client = TridentClient(base_url=base_url, timeout=timeout)
        await client.start()
        print(f"{colored('SERVER_RESPONDED', 'green')}:", file=sys.stderr)
        return client.get_interface()
    except asyncio.exceptions.TimeoutError:
        # ToDo: process exception through fire.core.FireError
        print(f"{colored('ERROR', 'red')}: {base_url} timeout", file=sys.stderr)
        sys.exit(1)


def main():
    fire.Fire(client_cli, name='trident-client')


if __name__ == '__main__':
    main()
