# Graphite Migration too

graphite_migrate.py is a graphite migration tool that exports the data and imports onto another graphite instance.
You can add as many destinations as you want.

## CMDLINE

```
Usage: graphite_migrate.py [options] [ -i FILE | -t ] graphite_destination1[:port1] [ graphite_destination2[:port2] [ ... ] ]

Manages graphite migration

Options:
  -h, --help            show this help message and exit
  -b BATCHSIZE, --batch-size=BATCHSIZE
                        Batch size when sending data to Graphite
  -r RECONNECT_INTERVAL, --reconnect-interval=RECONNECT_INTERVAL
                        Batch size when sending data to Graphite
  -m MAX_ENQUEUES, --max-enqueued=MAX_ENQUEUES
                        Maximum queue size for the graphite threads
  -p ABSOLUTE_PATH, --path=ABSOLUTE_PATH
                        absolute path where the whisper file relate from
  -f INPUT_FILE, --file=INPUT_FILE
                        The input file that contains the list of all the
                        whisper files to browse
  -s, --simulate        Testing the remote graphite by sending dummy data
  -d, --debug           Toggles DEBUG logging
  -E BEFORE_TIMESTAMP, --before=BEFORE_TIMESTAMP
                        upper timestamp limit
  -B AFTER_TIMESTAMP, --after=AFTER_TIMESTAMP
                        lower timestamp limit
```

## Options

### Simulation (-s)

Sends dummy data to all graphite destinations.
Allows you to test graphite connectivity and remote servers.

### INPUT FILE and PATH

INPUT_FILE (in normal mode) contains a list of files to migrate.
Can be composed of text files (.txt) and whisper files (.wsp).

ABSOLUTE_PATH is the root path for all files described in INPUT_FILE.

####Example:
INPUT_FILE
```
path/to/file.txt
```
ABSOLUTE_PATH
```
/root/to/files
```
will point to:
```
/root/to/files/path/to/file.txt
```

### Timestamps (-B and -E)

Allows you to define a start and end timestamp for the normal mode.
Defaults to now for BEFORE_TIMESTAMP and 7 days ago for AFTER_TIMESTAMP

### Tunables

#### Batchsize

Number of metrics sent in one go. Defaults to 1000.

#### Reconnect Interval

Number of metrics sent to trigger a reconnect. Defaults to 100000.

#### Max enqueued

Maximum number of metrics queued in graphite above which loading will be suspended. Defaults to 100,000,000.
