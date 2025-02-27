# bytewax-s2
<div>
  <p>
    <!-- PyPI -->
    <a href="https://pypi.org/project/bytewax-s2/"><img src="https://img.shields.io/pypi/v/bytewax-s2" /></a>
    <!-- Discord -->
    <a href="https://discord.gg/vTCs7kMkAf"><img src="https://img.shields.io/discord/1209937852528599092?logo=discord" /></a>
    <!-- LICENSE -->
    <a href="https://github.com/s2-streamstore/bytewax-s2/blob/main/LICENSE"><img src="https://img.shields.io/github/license/s2-streamstore/bytewax-s2" /></a>
  </p>
</div>

`bytewax-s2` is the Python package that provides [Bytewax](https://bytewax.io/) connector for [S2](https://s2.dev/), which can be used in your bytewax pipelines if you want to read from or append to S2 streams.

## API overview

The package exposes the following types:
- `S2Source` - used for reading from S2 streams.
- `S2Sink` - used for appending to S2 streams.
- `S2Config` - used when initializing `S2Source` and `S2Sink`.
- `S2SourceRecord` - items read from S2 streams will be of this type.
- `S2SinkRecord` - items to be appended to S2 streams must be of this type.
- `S2SinkPartitionFn` - used for routing the items to appropriate S2 streams.

Please refer to the docstrings of the abovementioned types to know more.

## Installation

You can install the package from the [Python Package Index](https://pypi.org/project/bytewax-s2) using the package manager of your choice. E.g., with `pip`:

```bash
pip install bytewax-s2
```

## Examples

`examples/` directory in the [repo](https://github.com/s2-streamstore/bytewax-s2/tree/main/examples/) contain two bytewax pipelines:
- `sink.py` - where firehose of events from Bluesky is processed and appended to S2 streams.
- `source.py` - where processed data is read from S2 streams and insights are plotted in terminal.

If you are interested in knowing more about these pipelines, take a look at this [walkthrough](https://s2.dev/docs/integrations/bytewax#real-time-insights-from-bluesky-firehose-data).

