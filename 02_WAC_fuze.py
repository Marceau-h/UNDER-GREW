from pathlib import Path

import pandas


def rm_tree(pth: Path) -> None:
    """https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    Removes all content of a directory recursively anf then the directory itself"""
    pth = Path(pth)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)

    pth.rmdir()


maindir = Path("exports")
newdir = maindir / "WAC"
newdir.mkdir(exist_ok=True, parents=True)

dfs = {}

for dir in maindir.glob("WAC*"):
    if not dir.name[-1].isdigit():
        print(f"{dir.name} is not a number")
        continue

    print(f"Processing {dir.name}")

    for file in dir.glob("*.csv"):
        df = pandas.read_csv(file)
        if not file.stem in dfs:
            dfs[file.stem] = df
        else:
            dfs[file.stem] = pandas.concat([dfs[file.stem], df])

    rm_tree(dir)


for name, df in dfs.items():
    df.to_csv(newdir / f"{name}.csv", index=False)
    df.to_csv(newdir / f"{name}.tsv", sep="\t", index=False)
