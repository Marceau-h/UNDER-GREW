import json
import re
from pathlib import Path
from io import StringIO
from multiprocessing import Pool, cpu_count

import pandas as pd
from tqdm.auto import tqdm

import spacy

subfolder: Path

nlp: spacy.language = spacy.load("fr_core_news_sm")
columns: list[str] = ["ID", "FORM", "LEMMA", "UPOS", "XPOS", "FEATS", "HEAD", "DEPREL", "DEPS", "MISC"]
ud_dir: Path = Path("UD")
exports_dir: Path = Path("exports")
exports_extended_dir: Path = Path("Xports")
exports_extended_dir.mkdir(exist_ok=True, parents=True)


class ConnluLine:
    columns: tuple[str] = ("ID", "FORM", "LEMMA", "UPOS", "XPOS", "FEATS", "HEAD", "DEPREL", "DEPS", "MISC")
    len_col = len(columns)

    def __init__(self, line: str):
        self.ID, self.FORM, self.LEMMA, self.UPOS, self.XPOS, self.FEATS, self.HEAD, self.DEPREL, self.DEPS, self.MISC = line.split(
            "\t")

    def __repr__(self):
        return f"""ConnluLine({" ".join(f"{col}: {getattr(self, col)}" for col in self.columns)})"""

    def __str__(self):
        return "\t".join(getattr(self, col) for col in self.columns)

    def __eq__(self, other):
        return all(getattr(self, col) == getattr(other, col) for col in self.columns)

    def __hash__(self):
        return hash(str(self))

    def __getitem__(self, key):
        if isinstance(key, int):
            return getattr(self, self.columns[key])
        elif isinstance(key, str):
            return getattr(self, key)
        else:
            raise TypeError(f"ConnluLine indices must be integers or strings, not {type(key)}")

    def __setitem__(self, key, value):
        if isinstance(key, int):
            setattr(self, self.columns[key], value)
        elif isinstance(key, str):
            setattr(self, key, value)
        else:
            raise TypeError(f"ConnluLine indices must be integers or strings, not {type(key)}")

    def __iter__(self):
        for col in self.columns:
            yield getattr(self, col)

    def __len__(self):
        return self.len_col


class ConnluSent:

    def __init__(self, string: str):
        decoupe = [line.strip() for line in string.split("\n") if line != "" and not line.startswith("#")]
        self.id = decoupe[0]
        self.lines = tuple(ConnluLine(line) for line in decoupe[1:])

    def __repr__(self):
        return f"""ConnluSent({self.id}, {self.lines})"""

    def __str__(self):
        return "\n".join((self.id, *map(str, self.lines)))

    def __eq__(self, other):
        return self.id == other.id and self.lines == other.lines

    def __hash__(self):
        return hash(str(self))

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.lines[key]
        elif isinstance(key, str):
            return tuple(line[key] for line in self.lines)
        else:
            raise TypeError(f"ConnluSent indices must be integers or strings, not {type(key)}")


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


def process_row(args: tuple[pd.Series, list[dict[str, str]]]) -> dict[str, str | int]:
    row, sent = args

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

    # pivot_data = sent[token_nb]
    # pivot_data["dist"] = dist

    # del left, pivot, right, count, token_nb  # Free memory

    return {**sent[token_nb], "dist": dist}

avoid_firsts = 4
if __name__ == '__main__':
    for subfolder in (list(ud_dir.iterdir())):
        print(f"{subfolder.name}")

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

        with open(subfolder / "sents.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(sents))

        exports_sub = exports_dir / subfolder.name

        for export in exports_sub.glob("*.csv"):
            if avoid_firsts:
                avoid_firsts -= 1
                continue

            df = pd.read_csv(export, index_col=None, low_memory=False).fillna("")  # , low_memory=False)
            print(df.memory_usage(deep=True).sum() / (1024**2))
            for column in df:
                try:
                    df[column] = pd.to_numeric(df[column], downcast="unsigned")
                except:
                    df[column] = df[column].astype(pd.StringDtype())

            print(df.memory_usage(deep=True).sum() / (1024**2))

            pivot_datas = []

            if "sents" not in locals() and "sents" not in globals():
                with open(subfolder / "sents.json", "r", encoding="utf-8") as f:
                    sents = json.loads(f.read())

            args = tuple((row, sents[str(row["sent_id"])]) for _, row in df.iterrows())

            del sents  # Free memory
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
