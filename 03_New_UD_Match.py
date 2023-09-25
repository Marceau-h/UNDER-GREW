import json
import re
from pathlib import Path
from io import StringIO
from multiprocessing import Pool, cpu_count

import pandas as pd
from tqdm.auto import tqdm
import jsonlines

import spacy

nlp: spacy.language
columns: list[str]
ud_dir: Path
exports_dir: Path
exports_extended_dir: Path


def try_int(s: str) -> int | str:
    try:
        return int(s)
    except ValueError:
        return s


def conllu_to_tuple(conllu: str) -> tuple[tuple[str]]:
    return tuple(tuple(l.split("\t")) for l in conllu.split("\n") if l != "" and not l.startswith("#"))


def last_name(doc: spacy.tokens.doc.Doc) -> int:
    for i, t in enumerate(reversed(doc)):
        if t.pos_ == "NOUN":
            return i

    return -1


def first_name(doc: spacy.tokens.doc.Doc) -> int:
    for i, t in enumerate(doc):
        if t.pos_ == "NOUN":
            return i

    return -1


def dist_name(right: spacy.tokens.doc.Doc, left: spacy.tokens.doc.Doc) -> int:
    return min(last_name(right), first_name(left))


def process_row(args: list[pd.Series | list[tuple[str]]]) -> list[str, int]:
    row, sent = args
    id_, sent = next(iter(sent.items()))

    left = row["left_context"]
    left = left if not (isinstance(left, float) or pd.isna(left)) else ""
    pivot = row["pivot"]
    pivot = pivot if not isinstance(pivot, float) else ""
    pivot = pivot.replace('""', '"')
    right = row["right_context"]
    right = right if not isinstance(right, float) else ""

    if pivot == "":
        print(f"{row['sent_id'] = }")
        # print(f"{subfolder.name = }")
        print(f"{left = }")
        print(f"{pivot = }")
        print("pivot is empty")
        return {}

    if left == "" and right == "":
        print(f"{row['sent_id'] = }")
        # print(f"{subfolder.name = }")
        print(f"{left = }")
        print(f"{pivot = }")
        print(f"{right = }")
        print("left and right are empty")
        return {}

    dist = dist_name(nlp(right), nlp(left))

    try:
        count = (sum(1 for e in re.findall(fr"\W{re.escape(pivot)}\W", right) if e)
                 + sum(1 for e in re.findall(fr"^{re.escape(pivot)}(\b)", left) if e)
                 + sum(1 for e in re.findall(fr"(\b){re.escape(pivot)}$", left) if e))
        count = count if count >= 0 else 0

    except:
        print(left)
        print(pivot)
        raise

    words = [e[1] for e in sent]

    try:
        token_nb = words.index(pivot, count)
    except ValueError:
        try:
            token_nb = words.index(pivot)
        except ValueError:
            print(row)
            print(f"{words = }")
            print(f"{pivot = }")
            print(f"{count = }")
            print(f"{id_ = }")
            raise

    pivot_data = sent[token_nb]
    pivot_data.append(dist)

    return pivot_data


def find_next(file: str | Path, ids: list[str | int]):
    # if isinstance(ids[0], int):
    #     ids = (str(i) for i in ids)
    if isinstance(ids[0], str):
        ids = (try_int(i) for i in ids)

    previ = None
    obj = None
    with jsonlines.open(file, mode="r") as f:
        # while (i := next(ids, None)):
        for i in ids:
            if previ == i:
                yield obj
                continue

            previ = i

            while True:
                obj = f.read()
                id_ = try_int(next(iter(obj)))
                if i == id_:
                    yield obj
                    break


if __name__ == "__main__":
    ud_dir = Path("UD")
    exports_dir = Path("exports")
    exports_extended_dir = Path("Xports")
    exports_extended_dir.mkdir(exist_ok=True, parents=True)

    columns = ["ID", "FORM", "LEMMA", "UPOS", "XPOS", "FEATS", "HEAD", "DEPREL", "DEPS", "MISC", "dist"]

    nlp = spacy.load("fr_core_news_sm")

    for subfolder in (list(ud_dir.iterdir())):
        print(f"{subfolder.name}")

        # if not subfolder.name == "WAC":
        #     continue

        if not subfolder.is_dir():
            print(f"{subfolder.name} is not a folder")
            continue

        if subfolder.name[-1].isdigit():
            print(f"{subfolder.name} is a number")
            continue

        with StringIO() as all_txt:
            for connlu in subfolder.glob("*.conllu"):
                # print(connlu)
                with open(connlu, "r", encoding="utf-8") as f:
                    all_txt.write(f.read())

            all_txt.seek(0)
            all_txt = all_txt.getvalue().split("\n\n")

            sents = (s.split("# sent_id = ")[-1] for s in all_txt if s != "")

        sents = (s.split("\n", 1) for s in sents)

        try:
            sents = [(int(s[0]), conllu_to_tuple(s[1])) for s in sents]
        except ValueError:
            sents = [(str(s[0]), conllu_to_tuple(s[1])) for s in sents]

        sents = sorted(sents, key=lambda x: x[0])

        ids_ = tuple(s[0] for s in sents)

        sents = [{s[0]: s[1]} for s in sents]

        with jsonlines.open("sents.jsonl", mode="w") as f:
            f.write_all(sents)

        del sents

        exports_sub = exports_dir / subfolder.name

        first = False

        for export in exports_sub.glob("*.csv"):
            if first:
                first = False
                continue

            df = pd.read_csv(export, index_col=None, low_memory=False).fillna("")  # , low_memory=False)
            print(df.memory_usage(deep=True).sum() / (1024 ** 2))
            for column in df:
                try:
                    df[column] = pd.to_numeric(df[column], downcast="unsigned")
                except:
                    df[column] = df[column].astype(pd.StringDtype())

            print(df.memory_usage(deep=True).sum() / (1024 ** 2))

            # Après le sort, on a les lignes dans l'ordre des sent_id
            # cependant, le dataframe est indexé par les lignes d'origine.
            # On reset alors l'index pour avoir un index qui correspond au nouveau tri,
            # sinon, le join se fait sur les index d'origine, sachant que le nouveau
            # dataframe `df_pivot` à un index qui correspond au tri des lignes
            df = df.sort_values(by="sent_id")
            df = df.reset_index(drop=True)

            sent_ids = df["sent_id"].to_list()

            args = ((row, sent) for (_, row), sent in zip(df.iterrows(), find_next("sents.jsonl", sent_ids)))

            pivot_datas = []
            with Pool(cpu_count() - 2) as p:
                pbar = tqdm(p.imap(process_row, args), total=len(df))
                for res in pbar:
                    pivot_datas.append(res)

            df_pivot = pd.DataFrame(pivot_datas, columns=columns)

            del pivot_datas

            df = df.join(df_pivot)

            del df_pivot

            Xport = exports_extended_dir / subfolder.name
            Xport.mkdir(exist_ok=True, parents=True)
            Xport = Xport / export.name

            df.to_csv(Xport.with_suffix('.csv'), index=False)

            try:
                df.to_excel(Xport.with_suffix('.xlsx'), index=False)
            except ValueError:
                print(f"{subfolder.name = }")
                print(f"{export.name = }")
                pass
