from pathlib import Path
import pandas as pd
from tqdm.auto import tqdm

corpus_dir: Path
df: pd.DataFrame
per_verb: dict[str, pd.DataFrame]

filtres: Path = Path("filtres")


def rm_tree(pth: Path) -> None:
    """https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    Removes all content of a directory recursively but not the directory itself"""
    pth = Path(pth)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)


for to_split in filtres.glob("*-all.csv"):
    df = pd.read_csv(to_split).fillna("")

    if df.empty:
        print(f"{to_split} is empty")
        continue

    print(f"{to_split} has {len(df)} rows")

    per_verb = {}
    for verb in tqdm(df["LEMMA"].unique()):
        per_verb[verb] = df[df["LEMMA"] == verb]

    corpus_dir = to_split.parent / to_split.stem

    if corpus_dir.exists():
        rm_tree(corpus_dir)  # Delete if exists to clear previous results
    else:
        corpus_dir.mkdir()  # Create if not exists

    for verb, df_verb in per_verb.items():
        df_verb.to_csv(corpus_dir / f"{verb}.csv", index=False)
