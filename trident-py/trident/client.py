import json
import os
import typing
from typing import AsyncGenerator

import aiofiles
from aiobaseclient import BaseClient
from aiobaseclient.exceptions import ExternalServiceError
from izihawa_utils.common import filter_none


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

    async def blobs_get(self, hash_: str, timeout: float = None) -> bytes | None:
        url = f"/blobs/{hash_}"
        response = await self.get(url, timeout=timeout)
        if response is None:
            return None
        return await response.read()

    async def blobs_get_chunks(self, hash_: str, timeout: float = None) -> AsyncGenerator[bytes, None]:
        url = f"/blobs/{hash_}"
        response = await self.get(url, timeout=timeout)
        if response is None:
            return
        async for data, _ in response.content.iter_chunks():
            yield data

    async def tables_ls(self) -> dict:
        response = await self.get(f"/tables/")
        return await response.json()

    async def tables_create(self, table: str, storage: str | None = None) -> bytes:
        url = f"/tables/{table}/"
        params = {}
        if storage:
            params['storage'] = storage
        response = await self.post(
            url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(filter_none(params)),
        )
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
    ) -> bytes:
        url = f"/tables/{table}/import/"
        response = await self.post(
            url,
            json={
                'ticket': ticket,
                'storage': storage,
                'download_policy': download_policy,
            },
        )
        return await response.read()

    async def tables_sync(
        self,
        table: str,
        download_policy: dict | None = None,
    ) -> bytes:
        url = f"/tables/{table}/sync/"
        response = await self.post(
            url,
            json={
                'download_policy': download_policy,
            },
        )
        return await response.read()

    async def tables_drop(self, table: str) -> bytes:
        url = f"/tables/{table}/"
        response = await self.delete(url)
        return await response.read()

    async def table_insert(self, table: str, key: str, value: bytes) -> str:
        url = f"/tables/{table}/{key}"
        response = await self.put(url, data=value)
        return response.headers['X-Iroh-Hash']

    async def table_share(self, table: str, mode: str = 'read') -> dict:
        url = f"/tables/{table}/share/{mode}/"
        response = await self.post(url)
        return await response.json()

    async def table_delete(self, table: str, key: str):
        url = f"/tables/{table}/{key}"
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

    async def table_get(self, table: str, key: str, timeout: float = None) -> dict | None:
        url = f"/tables/{table}/{key}"
        response = await self.get(url, timeout=timeout)
        if response is None:
            return None
        content = await response.read()
        return {
            'iroh_hash': response.headers['X-Iroh-Hash'],
            'size': response.headers['Content-Length'],
            'content': content
        }

    async def table_get_chunks(self, table: str, key: str, timeout: float = None) -> AsyncGenerator[bytes, None]:
        url = f"/tables/{table}/{key}"
        response = await self.get(url, timeout=timeout)
        async for data, _ in response.content.iter_chunks():
            yield data

    async def table_ls(self, table) -> typing.AsyncGenerator[str, None]:
        response = await self.get(f"/tables/{table}/")
        async for line in response.content:
            yield line.decode()[:-1]

    async def table_exists(self, table: str, key: str) -> dict | None:
        url = f"/tables/{table}/{key}"
        response = await self.head(url)
        if response is not None:
            return {
                'iroh_hash': response.headers['X-Iroh-Hash'],
                'size': int(response.headers['Content-Length']),
            }

    async def table_upload(self, table: str, directory: str):
        full_path = os.path.abspath(directory)
        for root, subdirs, files in os.walk(full_path):
            for file_path in files:
                async with aiofiles.open(root + '/' + file_path, mode='rb') as file:
                    rel_path = os.path.relpath(root + '/' + file_path, full_path)
                    await self.table_insert(table, rel_path, await file.read())

    async def table_copy_files(self, source_path: str, target_path: str):
        source_table, source_path = (source_path.rstrip('/') + '/').split('/', 1)
        target_table, target_path = (target_path.rstrip('/') + '/').split('/', 1)
        async for element in self.table_ls(source_table):
            if element.startswith(source_path):
                remaining_path = os.path.join(target_path, element.removeprefix(source_path).lstrip('/'))
                await self.table_foreign_insert(
                    source_table,
                    element,
                    target_table,
                    remaining_path,
                )

    async def _tables_ls_get_cli(self, table: str):
        elements = []
        async for line in self.table_ls(table):
            elements.append(line.strip())
        return elements

    def get_interface(self):
        return {
            'table-copy-files': self.table_copy_files,
            'table-get': self.table_get,
            'table-ls': self._tables_ls_get_cli,
            'table-upload': self.table_upload,
            'tables-create': self.tables_create,
            'tables-drop': self.tables_drop,
        }
