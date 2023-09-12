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


def conllu_to_dict(conllu: str) -> list[dict[str, str]]:
    conllu = conllu.split("\n")
    conllu = [l.split("\t") for l in conllu if l != "" and not l.startswith("#")]
    conllu = [dict(zip(columns, l)) for l in conllu]
    return conllu


def last_name(s: str) -> int:
    s = nlp(s)
    for i, t in enumerate(reversed(s)):
        if t.pos_ == "NOUN":
            return i

    return -1


def first_name(s: str) -> int:
    s = nlp(s)
    for i, t in enumerate(s):
        if t.pos_ == "NOUN":
            return i

    return -1


def dist_name(right: str, left: str) -> int:
    return min(last_name(right), first_name(left))


def process_row(args: tuple[pd.Series, list[dict[str, str]]]) -> dict[str, str]:
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

    pivot_data = sent[token_nb]
    pivot_data["dist"] = dist

    return pivot_data


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

        sents = [s.split("# sent_id = ")[-1] for s in all_txt.getvalue().split("\n\n") if s != ""]

    sents = [s.split("\n", 1) for s in sents]
    sents = {s[0]: conllu_to_dict(s[1]) for s in sents}

    exports_sub = exports_dir / subfolder.name

    for export in exports_sub.glob("*.csv"):
        df = pd.read_csv(export, low_memory=False)
        pivot_datas = []
        args = []
        for _, row in df.iterrows():
            try:
                sent = sents[str(row["sent_id"])]
            except Exception as e:
                print(f"{row['sent_id'] = }")
                print(f"{subfolder.name = }")
                print(sents)
                raise

            args.append((row, sent))

        with Pool(cpu_count()) as p:
            pbar = tqdm(p.imap(process_row, args), total=len(args))
            # pbar =p.imap(process_row, args)
            for res in pbar:
                pivot_datas.append(res)

        df_pivot = pd.DataFrame(pivot_datas)

        df = df.join(df_pivot)

        Xport = exports_extended_dir / subfolder.name
        Xport.mkdir(exist_ok=True, parents=True)
        Xport = Xport / export.name

        df.to_csv(Xport.with_suffix('.csv'), index=False)
        df.to_csv(Xport.with_suffix('.tsv'), sep="\t", index=False)
        try:
            df.to_excel(Xport.with_suffix('.xlsx'), index=False)
        except ValueError:
            print(f"{subfolder.name = }")
            print(f"{export.name = }")
            pass
        df.to_pickle(Xport.with_suffix('.pkl'))
        df.to_json(Xport.with_suffix('.jsonl'), orient='records', lines=True)
        df.to_json(Xport.with_suffix('.json'), orient='records')
