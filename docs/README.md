
# Build duckdb-geography documentation

Install the requirements:

```shell
pip install -r requirements.txt
```

Build `duckdb` with the extension statically linked or install it
into some existing `duckdb` environment:

```shell
make debug
```

Run `duckdoc.py`:

```shell
python duckdoc.py \
    --extension geography \
    --output function-reference.md \
    --run-examples
```

This will update the rendered documentation (which is currently just checked in
as a `function-reference.md` in this directory).
