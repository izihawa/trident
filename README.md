# 🔱 Trident 🔱

Key-value storage with seeding capabilities.

Let's say we have master node A and following nodes Bx

A: Insert key K1 with value V1 into table T1

... magic

B1: Received K1-V1 from A
B2: Received K1-V1 from B1

...

B10: Received K1-V1 from B5

# How it works

Trident is based on [Iroh](https://github.com/n0-computer/iroh), the library for
replicating key-value stream over open set of peers in the Internet.

## Concepts

`author` - entity, identified by its key and allowed to read and/or write to table
`table` - an ordered key-value storage that may be replicated
`key` - any valid UTF8 string
`value` - any bytes
`table ticket` - unique string allowing anyone in the Internet to start replication of table to its instance of Trident

