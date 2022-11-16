# MyNoSQL

The MyNoSQL Module provides a distributed no-sql database library that is intended to be embedded into python applications. It allows fast local reading and writing of records, taking care of data replication for you regardless of the reliability of your network connection.

By using a MyNoSQL database you are freed from needing to worry about ensuring data gets to where it need to go or handling errors related to bad network connections.
- You write to the database locally knowing MyNoSQL will take care of ensuring eventually it will be replicated to the mirror in the background, you don't need to wait for that to happen in your application.
- You make a local read request and MyNoSQL will return the document as quickly as possible, taking care of caching and only requesting the document from the remote mirror if it's changed

The MyNoSQL database will be respectful of your machine resources and will not waste them unnecessarily:
- Records are purged from ram when no longer in use
- All data stored on disk is compressed with LZMA compression to minimise disk usage
-Records are not stored as individual files but in a shard of files, allowing each shard to grow and use a whole block on a disk, reducing the number of partially used blocks (wasted space on a disk).
- Network traffic is minimised by only transferring records when they change. This is done via a record index that tracks the revisions of all documents, MyNoSQL databases request each other's indexes and update their own index when there is a newer record. This allows MyNoSQL to know if the local copy of a record is current and can be returned immediately or if a newer version need to be downloaded.

While there is no limit to applications that could use MyNoSQL, here are some example applications where MyNoSQL's features could be valuable:
- IoT data logging with central data storage, each data logger would be a peer, with your central location having 1 or more mirrors. The data logging application would write to the local peer database allowing your data logging application to continue with it's data logging activities and the MyNoSQL will replicate the logged data to the mirror(s) in the background when connectivity is available
- Multiplayer games, implementing MyNoSQL in a multiplayer game would allow the game developer to significantly reduce the load on their game servers as game clients running a peer replica of the game database would only need to download game resources as they change and would only poll the game server for the a single index update rather than polling every resource to determine if the resource has changed or worse yet downloading every resource if caching is not implemented. Additionally implementing MyNoSQL allows the player to continue playing and save gameplay state even when their internet connection is lost without the game developer needing to handle these scenarios, MyNoSQL will update the save states to the game server when the internet connection is restored.
- Block chains, MyNoSQL could make development of a block chain simpler, the block chain application only need to worry about reading, writing and validating block records.

## Using this module

First import the module and create a database instance
```python
import MyNoSQL

db = MyNoSQL.MyNoSQL()
db.opendb("YourDatabseName")
```

### Changing database mode
The default mode for a MyNoSQL is peer, you will need at least one mirror, typically this is your application server. When your MyNoSQL instance is it's mode changed, the new mode is remembered, so this only needs to be called once, not every time the application is launched.
```python
db.setdbmode("Mirror")  # optional
```

### Connecting to a database mirror
In order for database replication to happen, either remote storage of records or retrieving of records created you will need to connect to a mirror, you don't need to connect to multiple mirrors though as once connected to the first mirror a list of all available mirrors is retrieved and stored locally, so this only needs to be called once, not every time the application is launched.
```python
db.addpeer("http://mirrorhostname:8800")
```

### Saving a record
MyNoSQL records are simply a python dictionary object, anything that can be stored in a python dictionary can be stored in a MyNoSQL record.

The following are reserved dictionary keys:
- id: the record id, you can set this to any value you wish, if you do not provide an id then one will be generated on first save. If you change the record id then it will be treated as a new record.
- rev: the recored revision, this will be created if not provided or in an invalid format, and incremented if one is provided, however the record will not be saved if there is a later revision for the same record id. You should always check you have the latest revision of a record before updating it.

Saving a record is as simple as:
```python
myrecord = db.savedoc(myrecord)
```
The returned record contains the generated id if you did not provide one and the updated record revision.

### Reading a record
Reading a record is as simple as:
```python
myrecord = db.readdoc(myrecordid)
```

- If the record's latest revision is available locally it will be returned immediately
- if the record's latest revision is not available locally, it would be downloaded then returned



## DB Modes

MyNoSQL has modes of operation, Peer (Default) and Mirror

### Peer

A Peer, will cache documents requested or written by the local application, it will not keep copies of documents the the local local application has not interacted with

When a MyNoSQL database in peer mode:
- Writing a new record MyNoSQL will:
  - write locally
  - contact a mirror and replicate to that mirror
- when reading a record MyNoSQL will:
  - check if the most recent version of the record is available locally
  - request the most recent version of the record from a mirror if not available locally
  - return the most recent version of the record
- periodically poll a mirror to push any local records that are newer then the record version on the mirror

### Mirror

A mirror, will attempt to keep copies of all documents.

When a MyNoSQL database in mirror mode:
- Receive records from peers and mirrors
  - write the received record
- Receive requests for records from peers and mirrors
  - read the record and return to the requestor
- Writing a new record MyNoSQL will:
  - write locally
  - attempt to contact all mirrors and replicate to those mirrors
- When reading a record MyNoSQL will:
	- check if the most recent version of the record is available locally
	- if not available locally request the most recent version of the record from each mirror until a mirror provides the record requested (This should rarely happen as mirrors should already have received the record when it was written on another mirror, or through periodical polling)
	- return the most recent version of the record
- periodically poll all other mirrors to check the versions of records and request updates to any records that are out of date

## Installing

```
pip3 install -U MyNoSQL
```
