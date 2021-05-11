# mynosql
mynosql is a distributed nosql style database written completely in python as a python library.

The intention is that mynosql would be embeded in a python application as a client mode and on an application server in mirror mode, this way your client application has fast cached access to records that were created locally, the files get mirrored to the application server and if the client application needs to access files created on another client can quickly pull that record (or an update) from the application server and then access it from the local cache.

Examples where this might be useful:
1. You are developing a mobile game, your game needs records from the game server (user accounts, level maps etc) the problem is when game users have flaky, poor or no internet connection then the game fails to load or stutters as these resources need to be downloaded every time unless you have a way to cache the resources and then check them for updates. mynosql will take care of all this for you, all you need to do is write the data to the local mynosql instance and mynosql will version the record, when you request to read the record mynosql will check for an update version on the server, if the locally cached version is current or the server can't be reached the local cached version is returned, otherwise the server version is downloaded and returned and the cache updated.
2. You have a network IOT data loggers, where the network access is intermitant, running mynosql in client mode on the data loggers allows you to log the data to the local mynosql instance and mynosql will take care of ensureing the mirror server is updated as soon as a connection is availalbe, if the connection is not up long enough to send all records mynosql will keep track and continue upadting the server when the connection is next available.

mynosql stors all records in compressed files to minimise the disk space requirements, it also keeps regularly accessed records in memory but is proactive in removing stale records from memory to minimise the memory consumption.

At the moment there is no documentation other than the library code itself, and the examples in the testing directory, ftest5.py being a client that generates records with random data and ftest6.py being a mirror (server) that pulls new and updated records from ftest5.py.

I created this for a project I'm planning in the future, as I wasn't happy with how couchdb worked with replicas and there was no better nosql or sql database solution. Specifically none of the other databases I cam accross allowed a client to have a partial replica where the contents of the partial replica are base on the clients usage and can be different partial replicas on each client.

If you think this might be useful to you, please let me know and i'll prioritise some documentation.
