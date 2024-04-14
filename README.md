# 🔱 Trident 🔱

Key-value storage with torrent-like capabilities.

Let's say we have master node A and following nodes Bx

```
A: Insert key K1 with value V1 into table T1
... <magic happens> ...
B1: Received K1-V1 from A
B2: Received K1-V1 from B1
...
B10: Received K1-V1 from B5
```

Group of peers create swarm around the table and can leech data from each other.

# How it works

Trident is based on [Iroh](https://github.com/n0-computer/iroh), the library for
replicating key-value stream over open set of peers in the Internet.

## Concepts

- `author` - entity, identified by its key and allowed to read and/or write to table
- `table` - an ordered key-value storage that may be replicated
- `key` - any valid UTF8 string
- `value`/`file`/`blob` - any bytes
- `table ticket` - unique string allowing anyone in the Internet to start replication of table to its instance of Trident

## Steps

### Launch Trident

```bash 
mkdir trident
docker pull izihawa/trident:latest
docker run izihawa/trident:latest generate-config /trident > trident/config.yaml
docker run -it -p 7080:80 -v $(pwd)/trident:/trident izihawa/trident:latest serve --config-path /trident/config.yaml
```

### Import Document

```bash
curl -H "Content-Type: application/json" -X POST "http://127.0.0.1:7080/tables/document/import/" \
--data '{"ticket": "<doc_ticket>", "storage": "default", "download_policy": {"EverythingExcept": []}}'
```

### Force Syncing

Force syncing may be required to download the blobs that have been added by other nodes to the table before import.

```bash
curl -H "Content-Type: application/json" -X POST --data '{"download_policy": {"EverythingExcept": []}, "threads": 32}' "http://127.0.0.1:7080/tables/my_table/sync/"
```

### List Blobs

```bash
curl "http://127.0.0.1:7080/tables/my_table/"
```

### Read Blob

Force syncing may be required to download the blobs that have been added by other nodes to the table before import.

```bash
curl "http://127.0.0.1:7080/tables/my_table/<key_name>/"
```