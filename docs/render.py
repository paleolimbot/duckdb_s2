from pathlib import Path

from jinja2 import Environment, FileSystemLoader


def main():
    functions = generate_functions()
    categories = generate_categories(functions)
    render_all(categories)


def render_all(categories):
    this_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(this_dir))

    context = {
        "categories": categories,
    }

    template = env.get_template("function-reference.md.jinja")
    with open(this_dir / "function-reference.md", "w") as out:
        out.write(template.render(context))


def generate_categories(functions):
    category_names = set(fun["category"] for fun in functions)
    return [
        {
            "name": category,
            "functions": [fun for fun in functions if fun["category"] == category],
        }
        for category in sorted(category_names)
    ]


def generate_functions():
    return [
        {
            "name": "s2_data_city",
            "summary": "Return a city from the example data or error if no such city exists.",
            "description": "An extended summary of the city",
            "example": "SELECT s2_data_city('Toronto');",
            "signatures": [
                {"return_type": "GEOGRAPHY", "params": []},
                {
                    "return_type": "GEOGRAPHY",
                    "params": [{"name": "city", "type": "VARCHAR"}],
                },
                {
                    "return_type": "GEOGRAPHY",
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
                    "return_type": "GEOGRAPHY",
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
                    "return_type": "DOUBLE",
                    "params": [{"name": "geog", "type": "GEOGRAPHY"}],
                },
            ],
            "category": "accessors",
        },
    ]


if __name__ == "__main__":
    main()
