from pathlib import Path
import pandas as pd
from tqdm.auto import tqdm

filtres = Path("filtres")


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
    print(to_split)
    df = pd.read_csv(to_split)
    df = df.fillna("")

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
