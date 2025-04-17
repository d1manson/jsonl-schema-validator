# Samples

We don't store the large file directly in this repo, but you can download them from various places on the web. Note we aim to use real data
to make benchmarking realistic (though these files are not yet used in a 'proper' criterion benchmark):

* `a-data.jsonl` can be download from [here](https://github.com/json-iterator/test-data/blob/master/large-file.json); though that isn't a proper jsonl file, so it needs to be convert via `jq -c '.[]' samples/large-file.json > samples/a-data.jsonl`.


