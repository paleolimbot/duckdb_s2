import json
from pathlib import Path
import subprocess

from jinja2 import Environment, FileSystemLoader


def main(extension_name, duckdb_path=None, output_path=None):
    if not duckdb_path:
        duckdb_path = find_duckdb(extension_name)

    functions = query_functions(duckdb_path, extension_name)
    parse_functions(functions)
    context = generate_context(functions)
    render_all(context, output_path)


def render_all(context, output_path):
    this_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(this_dir))

    template = env.get_template("function-reference.md.jinja")
    with open(output_path, "w") as out:
        out.write(template.render(context))


def generate_context(functions):
    category_names = set(fun["category"] for fun in functions)
    categories = [
        {
            "name": category,
            "functions": [fun for fun in functions if fun["category"] == category],
        }
        for category in sorted(category_names)
    ]

    return {"categories": categories}


def parse_functions(functions):
    for func in functions:
        if "category" in func["tags"]:
            func["category"] = func["tags"]["category"]
        else:
            func["category"] = "-".join(["other", func["type"], "functions"])

        if "description" in func and func["description"]:
            desc_lines = func["description"].splitlines()
            func["summary"] = desc_lines[0]
            func["description"] = "\n".join(desc_lines[1:])


def query_functions(duckdb_path, extension_name):
    sql = FUNCTION_DEF_SQL.replace("$EXTENSION_NAME$", extension_name)
    proc = subprocess.run(
        [
            duckdb_path,
            "-noheader",
            "-list",
            "-c",
            f"INSTALL json; LOAD json; LOAD {extension_name};" + sql,
        ],
        capture_output=True,
    )

    if proc.returncode != 0:
        raise ValueError("Function query failed\n---\n" + proc.stderr.decode())
    elif not proc.stdout.strip():
        raise ValueError("Function query returned zero functions")

    return [json.loads(line) for line in proc.stdout.splitlines()]


def find_duckdb(extension_name):
    this_dir = Path(__file__).parent
    build_dir = this_dir.parent / "build"
    for possible in [
        build_dir / "duckdb",
        build_dir / "debug" / "duckdb",
        "duckdb",
    ]:

        if subprocess.run(
            [possible, "-c", f"LOAD {extension_name};"], capture_output=True
        ):
            return possible

    raise ValueError(f"Can't find duckdb that can load extension '{extension_name}'")


def generate_test_functions():
    return [
        {
            "name": "s2_data_city",
            "type": "scalar",
            "summary": "Return a city from the example data or error if no such city exists.",
            "description": "An extended summary of the city",
            "example": "SELECT s2_data_city('Toronto');",
            "signatures": [
                {"return": "GEOGRAPHY", "params": []},
                {
                    "return": "GEOGRAPHY",
                    "params": [{"name": "city", "type": "VARCHAR"}],
                },
                {
                    "return": "GEOGRAPHY",
                    "params": [
                        {"name": "city", "type": "VARCHAR"},
                        {"name": "foofy", "type": "INTEGER"},
                    ],
                },
            ],
            "category": "example-data",
        },
        {
            "name": "s2_data_country",
            "summary": "Return a country from the example data or error if no such country exists.",
            "signatures": [
                {
                    "return": "GEOGRAPHY",
                    "params": [{"name": "country", "type": "VARCHAR"}],
                },
            ],
            "category": "example-data",
        },
        {
            "name": "s2_x",
            "summary": "Return the longitude of a point geography or NaN if none exist.s",
            "signatures": [
                {
                    "return": "DOUBLE",
                    "params": [{"name": "geog", "type": "GEOGRAPHY"}],
                },
            ],
            "category": "accessors",
        },
    ]


FUNCTION_DEF_SQL = """
SELECT
    json({
        name: function_name,
        type: function_type,
        signatures: signatures,
        tags: func_tags,
        description: description,
        example: example
    })
FROM (
    SELECT
        function_type,
        function_name,
        list({
            return: return_type,
            params: list_zip(parameters, parameter_types)::STRUCT(name VARCHAR, type VARCHAR)[]
        }) as signatures,
        any_value(tags) AS func_tags,
        any_value(description) AS description,
        any_value(example) AS example
    FROM duckdb_functions() as funcs
    GROUP BY function_name, function_type
    HAVING func_tags['ext'] = ['$EXTENSION_NAME$']
    ORDER BY function_name
);
"""


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description="Render function documentation for a DuckDB extension",
    )
    parser.add_argument(
        "--extension",
        help="The name of the extension for which reference should be rendered",
        default="geography",
    )
    parser.add_argument(
        "--duckdb",
        help=(
            "The path to the DuckDB executable used to load the "
            "desired version of the extension"
        ),
        default="",
    )
    parser.add_argument(
        "-o", "--output", help="The output file path", default="function-reference.md"
    )

    args = parser.parse_args(sys.argv[1:])
    main(args.extension, args.duckdb, args.output)
