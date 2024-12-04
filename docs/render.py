from pathlib import Path

from jinja2 import Environment, FileSystemLoader


functions = [
    {
        "name": "s2_data_city",
        "summary": "Return a city from the example data or error if no such city exists",
        "signature": "s2_data_city(VARCHAR)",
        "category": "example_data"
    }
]

category_names = set(fun["category"] for fun in functions)
categories = [{"name": category, "functions": [fun for fun in functions if fun["category"] == category]} for category in category_names]

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
