from pathlib import Path

from jinja2 import Environment, FileSystemLoader


functions = [
    {
        "name": "s2_data_city",
        "summary": "Return a city from the example data or error if no such city exists",
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
    }
]

category_names = set(fun["category"] for fun in functions)
categories = [
    {
        "name": category,
        "functions": [fun for fun in functions if fun["category"] == category],
    }
    for category in sorted(category_names)
]


def render_all(categories):
    this_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(this_dir))

    context = {
        "categories": categories,
    }

    template = env.get_template("function-reference.md.jinja")
    with open(this_dir / "function-reference.md", "w") as out:
        out.write(template.render(context))


if __name__ == "__main__":
    render_all(categories)
