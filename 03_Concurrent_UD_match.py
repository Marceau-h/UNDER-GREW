import json
import re
from pathlib import Path
from io import StringIO
from multiprocessing import Pool, cpu_count

import pandas as pd
from tqdm.auto import tqdm

import spacy

from ConnluSent import ConnluSent
from ConnluLine import ConnluLine

subfolder: Path

nlp: spacy.language = spacy.load("fr_core_news_sm")
columns: list[str] = ["ID", "FORM", "LEMMA", "UPOS", "XPOS", "FEATS", "HEAD", "DEPREL", "DEPS", "MISC"]
ud_dir: Path = Path("UD")
exports_dir: Path = Path("exports")
exports_extended_dir: Path = Path("Xports")
exports_extended_dir.mkdir(exist_ok=True, parents=True)





def last_name(s: str) -> int:
    for i, t in enumerate(reversed(nlp(s))):
        if t.pos_ == "NOUN":
            return i

    return -1


def first_name(s: str) -> int:
    for i, t in enumerate(nlp(s)):
        if t.pos_ == "NOUN":
            return i

    return -1


def dist_name(right: str, left: str) -> int:
    return min(last_name(right), first_name(left))

def ids(sents: tuple[ConnluSent]) -> tuple[str]:
    return tuple(sent.id for sent in sents)

def find_by_id(id: str, sents: tuple[ConnluSent], ids_: tuple[str] = ()) -> ConnluSent:
    if not ids_:
        ids_ = ids(sents)

    try:
        return sents[ids_.index(id)]
    except ValueError:
        print(f"{id = }")
        # print(f"{(set(ids_).difference(set(range(1_000_000)))) = }")
        # print(f"{sents = }")
        print(f"{subfolder.name = }")
        raise



def process_row(args: tuple[pd.Series, list[dict[str, str]]]) -> dict[str, str | int]:
    row, sent = args
    # sent = ConnluSent.fromPickle(sent)

    left = row["left_context"]
    left = left if not (isinstance(left, float) or pd.isna(left)) else ""
    pivot = row["pivot"]
    pivot = pivot if not isinstance(pivot, float) else ""
    pivot = pivot.replace('""', '"')
    right = row["right_context"]
    right = right if not isinstance(right, float) else ""

    dist = dist_name(right, left)

    if pivot == "":
        print(f"{row['sent_id'] = }")
        print(f"{subfolder.name = }")
        print(f"{left = }")
        print(f"{pivot = }")
        print("pivot is empty")
        return {}

    try:
        count = (sum(1 for e in re.findall(fr"\W{re.escape(pivot)}\W", right) if e)
                 + sum(1 for e in re.findall(fr"^{re.escape(pivot)}(\b)", left) if e)
                 + sum(1 for e in re.findall(fr"(\b){re.escape(pivot)}$", left) if e))
        count = count if count >= 0 else 0

    except:
        print(left)
        print(pivot)
        raise

    words = [e["FORM"] for e in sent]
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
            print(f"{subfolder.name = }")
            raise

    pivot_data = sent[token_nb]
    pivot_data["dist"] = dist

    return pivot_data.toJson()







avoid_firsts = 4
if __name__ == '__main__':
    for subfolder in (list(ud_dir.iterdir())):
        print(f"{subfolder.name}")

        if not subfolder.name == "WAC":
            continue

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

            sents = tuple(s.split("# sent_id = ")[-1] for s in all_txt.getvalue().split("\n\n") if s != "")

            all_txt.close()

        sents = tuple(ConnluSent(s) for s in sents)
        ids_ = ids(sents)

        with open(subfolder / "sents.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(sents, default=lambda o: o.toJson(), ensure_ascii=False, indent=4))

        exports_sub = exports_dir / subfolder.name

        for export in exports_sub.glob("*.csv"):
            # if avoid_firsts:
            #     avoid_firsts -= 1
            #     continue

            df = pd.read_csv(export, index_col=None, low_memory=False).fillna("")  # , low_memory=False)
            print(df.memory_usage(deep=True).sum() / (1024 ** 2))
            for column in df:
                try:
                    df[column] = pd.to_numeric(df[column], downcast="unsigned")
                except:
                    df[column] = df[column].astype(pd.StringDtype())

            print(df.memory_usage(deep=True).sum() / (1024 ** 2))

            pivot_datas = []

            if "sents" not in locals() and "sents" not in globals():
                with open(subfolder / "sents.json", "r", encoding="utf-8") as f:
                    sents = json.loads(f.read(), object_hook=lambda o: ConnluSent().fromJson(o))

            # args = tuple((row, sents[str(row["sent_id"])]) for _, row in df.iterrows())

            # args = [(row, find_by_id(str(row["sent_id"]), sents, ids_).toPickle()) for _, row in df.iterrows()]
            args = [(row, find_by_id(str(row["sent_id"]), sents, ids_)) for _, row in df.iterrows()]


            # del sents  # Free memory
            del df  # Free memory

            with Pool(cpu_count() - 4) as p:
                pbar = tqdm(p.imap(process_row, args), total=len(args))
                # pbar = p.imap(process_row, args)
                # pbar = tqdm(p.imap_unordered(process_row, args), total=len(args))
                for res in pbar:
                    pivot_datas.append(res)

            df_pivot = pd.DataFrame(pivot_datas)

            del pivot_datas  # Free memory

            df = pd.read_csv(export, index_col=None, low_memory=False).fillna("")

            for column in df:
                try:
                    df[column] = pd.to_numeric(df[column], downcast="unsigned")
                except:
                    df[column] = df[column].astype(pd.StringDtype())

            df = df.join(df_pivot)

            del df_pivot  # Free memory

            Xport = exports_extended_dir / subfolder.name
            Xport.mkdir(exist_ok=True, parents=True)
            Xport = Xport / export.name

            df.to_csv(Xport.with_suffix('.csv'), index=False)
            # df.to_csv(Xport.with_suffix('.tsv'), sep="\t", index=False)
            try:
                df.to_excel(Xport.with_suffix('.xlsx'), index=False)
            except ValueError:
                print(f"{subfolder.name = }")
                print(f"{export.name = }")
                pass
            # df.to_pickle(Xport.with_suffix('.pkl'))
            # df.to_json(Xport.with_suffix('.jsonl'), orient='records', lines=True)
            # df.to_json(Xport.with_suffix('.json'), orient='records')
