"""The library's NATS shell (``synadia_durable/_nats/`` at ``7456c75``), copied with its imports
re-pointed (``synadia_durable._nats.`` → ``shell.``), for the integration suites' own staging and
reading of the streams: a test that writes a journal by hand writes exactly what a library worker
would have left behind (F13, F18). Never imported by ``src/``."""
