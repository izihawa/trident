import typing
from typing import AsyncGenerator

from aiobaseclient import BaseClient
from aiobaseclient.exceptions import ExternalServiceError


class TridentClient(BaseClient):
    async def response_processor(self, response):
        if response.status == 404:
            return None
        elif response.status != 200:
            data = await response.read()
            if hasattr(response, "request"):
                raise ExternalServiceError(response.request.url, response.status, data)
            else:
                raise ExternalServiceError(None, response.status, data)
        return response

    async def sinks_ls(self) -> dict:
        response = await self.get(f"/sinks/")
        return await response.json()

    async def sinks_create(self, sink: str, config: dict) -> dict:
        response = await self.post(f"/sinks/{sink}/", json=config)
        return await response.read()

    async def tables_ls(self) -> dict:
        response = await self.get(f"/tables/")
        return await response.json()

    async def tables_create(self, table: str, storage: str) -> bytes:
        url = f"/tables/{table}/"
        response = await self.post(url, params={'storage': storage})
        return await response.read()

    async def tables_exists(self, table: str) -> bool:
        url = f"/tables/{table}/exists/"
        response = await self.get(url)
        if response is None:
            return False
        response = await response.json()
        return response["exists"]

    async def tables_import(
        self,
        table: str,
        ticket: str,
        storage: str,
        download_policy: dict | None = None,
        sinks: tuple | list = tuple(),
        keep_blob: bool = True,
    ) -> bytes:
        url = f"/tables/{table}/import/"
        response = await self.post(
            url,
            json={
                'ticket': ticket,
                'storage': storage,
                'download_policy': download_policy,
                'sinks': sinks,
                'keep_blob': keep_blob,
            },
        )
        return await response.read()

    async def tables_drop(self, table: str) -> bytes:
        url = f"/tables/{table}/"
        response = await self.delete(url)
        return await response.read()

    async def table_insert(self, table: str, key: str, value: bytes) -> bytes:
        url = f"/tables/{table}/{key}/"
        response = await self.put(url, data=value)
        return await response.read()

    async def table_share(self, table: str) -> dict:
        url = f"/tables/{table}/share/"
        response = await self.get(url)
        return await response.json()

    async def table_delete(self, table: str, key: str):
        url = f"/tables/{table}/{key}/"
        await self.delete(url)

    async def table_foreign_insert(self, from_table: str, from_key: str, to_table: str, to_key: str) -> bytes:
        url = f"/tables/foreign_insert/"
        response = await self.post(url, params={
            'from_table': from_table,
            'from_key': from_key,
            'to_table': to_table,
            'to_key': to_key,
        })
        return await response.read()

    async def table_get(self, table: str, key: str, timeout: float = None) -> bytes | None:
        url = f"/tables/{table}/{key}/"
        response = await self.get(url, timeout=timeout)
        if response is None:
            return None
        return await response.read()

    async def table_get_chunks(self, table: str, key: str, timeout: float = None) -> AsyncGenerator[bytes, None]:
        url = f"/tables/{table}/{key}/"
        response = await self.get(url, timeout=timeout)
        async for data, _ in response.content.iter_chunks():
            yield data

    async def table_ls(self, table) -> typing.AsyncGenerator[str, None]:
        response = await self.get(f"/tables/{table}/")
        async for line in response.content:
            yield line.decode()[:-1]

    async def table_exists(self, table: str, key: str) -> bool:
        url = f"/tables/{table}/{key}/exists/"
        response = await self.get(url)
        if response is None:
            return False
        response = await response.json()
        return response["exists"]
