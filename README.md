Jut Statsd Graphite Backend
===========================

Statsd backend that sends metrics to Jut using the graphite carbon protocol.

At present this is just a clone of the builtin statsd graphite backend that uses
different configuration variables for the destination host and port.

Installation
-------------

Install the plugin alongside statsd:

`npm install statsd-jut-graphite`

Configuration
--------------

Add '../statsd-jut-graphite-backend/jutgraphite' to the backends list and set
`jutHost` and `jutPort` in the configuration.

For example:
```
{
    backends: ['../statsd-jut-graphite-backend/jutgraphite'],
    port: 8125,
    jutHost: '10.0.0.1',
    jutPort: 3125
}
```
