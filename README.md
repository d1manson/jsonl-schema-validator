# json-schema-validator

A command line utility written in Rust to validate json-newline files against a schema, fast.

This tool **does not validate that the json is spec-compliant**, rather it validates that the json matches a custom schema.

Define a schema in a json file like this:

```json
[
    {
      "name": "id",
      "type": "STRING",
      "mode": "REQUIRED"
    },
    ...
]
```

and then validate a `.jsonl` against that schema using the tool:

```bash
jsv -f mydata.jsonl -s mydata-schema.json
```

The schema format borrows from the BigQuery format [here](https://cloud.google.com/bigquery/docs/schemas#creating_a_JSON_schema_file), basically you
need to supply at least a `name` and `type` for each field. The supported modes are:

-  `NULLABLE` (default) - field is not required in the json, and if provided it can be `null`.
-  `REQUIRED` - field must be present in the json and cannot be `null`.
-  `REPEATED` - similar to `NULLABLE`, but if the field is non-`null` it must be a json array of the given type.

The supported types are:

- `STRING` - a json string
- `BOOL` - basic json `true`/`false`
- `FLOAT64` - any json number (details not validated).
- `INT64` - a json number without any exponent, between int64 min and max.
- `DECIMAL_29_9` - a json number without an exponent, with up to 29 digits before the decimal point, and up to 9 after (aka BigQuery `NUMERIC`, with default decimal point position)
- `DATE`* - date as a string, without a timezone
- `DATETIME`* - date and time as a string, without a timezone
- `TIME`* - time as a string, without a timezone
- `STRUCT` - a sub schema. In this case you need to provide a `"fields": [...]` property in the schema definition, with a list of sub fields. You can nest arbitrarily deeply, and/or use `REPEATED` mode if needed.
- `ANY` - a unspecified blob of json (could be a scalar json value or a json array/object with arbitray depth).


*the datetime types aren't 100% watertight validators, e.g. `27:30` is treated as valid time, though `37:30` is not.


## Benchmarks

The exact performance will depend a lot on the schema, the data, and the CPU arch, so it's hard to give a really useful benchmark. But as an initial guide, on an M4 Mac, it will validate a 26MB / 11.3k line 
[file](https://github.com/json-iterator/test-data/blob/master/large-file.json) at ~1ms per MB / ~2.4µs per line.
Currently it only supports single threaded excutation (multi threading is hopefully coming soon).

## Internals - what makes it fast?

- It's Rust, so there's no copying by default (and indeed I believe there is indeed no copying).
- It uses SIMD for most of the supported types, including the `ANY` type (which was most interesting to implement - I plan to 
  explain that properly in due course).
- It avoids unpredictable branches on the happy path as much as possible, even for `null`s, it avoids branching because a
  `null` is still valid (if the schema does not specify `REQUIRED`), and if the data is a random mix of `null`s and real values
  the CPU won't be able to predict what's coming up.
- It avoids recursion - the nested structs are implement with a `Vec` stack rather than recursion. Not sure how much this helps,
  but presumably to some extent.
- For schemas with a small number of keys it uses an optimised field lookup map. Here "small" applies to the top level schema and
  any nested structs separately. Small generally means having no more keys than the number of byte lanes in your CPU's SIMD 
  registers.  The most customised thing here was spun out into a spearate repo 
  [here](https://github.com/d1manson/rust-simd-psmap), though note that the version here is even more tuned for the specific 
  usecase of parsing json.


## Why?

I wanted to learn Rust and get stuck into some hyper optimisation fun after having worked much higher up in the stack for a 
few years.  If you find the tool useful, or any of the ideas interesting, do let me know - I've licenced it permissively but 
it's always nice to hear from people who like your work ;)!

## TODOs

- [ ] Add tests on the `validate.rs` module itself.
- [ ] Explore more optimisations at the level of the `validate` function itself (to date optmisation has been mostly at lower 
      levels). This will presumably require implementing benchmarks for the function too.
- [ ] Threading.
- [ ] Document some of the SIMD tricks as they were kind of interesting and I'm not sure if anything is novel.
- [ ] Provide some proper benchmarks using other tools.
- [ ] Make sure x86 is sensibly optimised (so far focus has been on Arm Macs / Neon, though it should be ok on x86).
- [ ] Publish it somewhere, to encourage people to actually use it for real.
