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


maindir: Path = Path("exports")
newdir: Path = maindir / "WAC"

if newdir.exists():
    rm_tree(newdir)

newdir.mkdir()

dfs: dict[str, pandas.DataFrame] = {}

for dir in maindir.glob("WAC-*"):
    if not dir.name[-1].isdigit():
        print(f"{dir.name} has no number")
        continue

    print(f"Processing {dir.name}")

    for file in dir.glob("*.csv"):
        df: pandas.DataFrame = pandas.read_csv(file)
        # The stem contains information on which query was used
        if file.stem not in dfs:
            dfs[file.stem] = df
        else:
            dfs[file.stem] = pandas.concat([dfs[file.stem], df])

    rm_tree(dir)


for name, df in dfs.items():
    df.to_csv(newdir / f"{name}.csv", index=False)
    # df.to_csv(newdir / f"{name}.tsv", sep="\t", index=False)  # Commented because TSVs ar not very useful
