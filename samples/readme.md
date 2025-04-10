# Samples

The `large-file.jsonl` itself is not included here, but you can download it from [here](https://github.com/json-iterator/test-data/blob/master/large-file.json); though that isn't a proper jsonl file, so it needs to be convert via `jq -c '.[]' samples/large-file.json > samples/large-file.jsonl.jsonl`.