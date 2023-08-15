from pathlib import Path

import pandas

maindir = Path("exports")
newdir = maindir / "WAC"
newdir.mkdir(exist_ok=True, parents=True)

dfs = {}

for dir in maindir.glob("WAC*"):
    if not dir.name[-1].isdigit():
        print(f"{dir.name} is not a number")
        continue

    print(f"{dir.name}")

    for file in dir.glob("*.csv"):
        df = pandas.read_csv(file)
        if not file.stem in dfs:
            dfs[file.stem] = df
        else:
            dfs[file.stem] = pandas.concat([dfs[file.stem], df])

    dir.unlink()


for name, df in dfs.items():
    df.to_csv(newdir / f"{name}.csv", index=False)
    df.to_csv(newdir / f"{name}.tsv", sep="\t", index=False)
