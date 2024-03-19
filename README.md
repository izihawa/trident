# 🔱 Trident 🔱

Key-value storage with seeding capabilities.

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
- `sink` - extra endpoint where all replicated files may be additionally sent to
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

### Add Sinks

#### S3

```bash 
curl -H "Content-Type: application/json" "http://127.0.0.1:7080/sinks/my_fancy_s3/" \
--data '{"S3": {"region_name": "us-east1", "bucket_name": "my-fancy-bucket", "prefix": "my/fancy/dir", "credentials": {"aws_access_key_id": "...", "aws_secret_access_key": "..."}}}'
```

#### IPFS

**Important Note!** Kubo requires data to be inside the IPFS_PATH directory
to be importable in-place. Therefore, you will need to organize your files
in a non-standard manner by placing the Trident data (or more precisely, the shard path) 
directory inside the IPFS_PATH.

```bash 
curl -H "Content-Type: application/json" "http://127.0.0.1:7080/sinks/my_fancy_ipfs/" \
--data '{"Ipfs": {"api_base_url": "http://127.0.0.1:5001", "in_place": true}}'
```


### Import Document

```bash
curl -H "Content-Type: application/json" "http://127.0.0.1:7080/tables/document/import/" \
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