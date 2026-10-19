# SFTP test keys

The host and client keys here exist only for the live SFTP tests: `tests/live/compose.live.yaml` mounts them into the test
server, and the tests pin its host keys. They protect nothing and must never be used anywhere else.
