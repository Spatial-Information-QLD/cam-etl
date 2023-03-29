from pathlib import Path

from rdflib import Graph


if __name__ == "__main__":
    graph = Graph()

    files = Path("output").glob("**/*.ttl")

    for file in files:
        graph.parse(file)

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    graph.serialize(output_dir / "output.compounded.ttl", format="longturtle")
