import re
from pathlib import Path
from io import StringIO

import pandas as pd
from tqdm.auto import tqdm

import spacy

nlp = spacy.load("fr_core_news_sm")

columns = "ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC".split()

ud_dir = Path("UD")
exports_dir = Path("exports")
expoets_extended_dir = Path("Xports")
expoets_extended_dir.mkdir(exist_ok=True, parents=True)


def conllu_to_dict(conllu):
    conllu = conllu.split("\n")
    conllu = [l.split("\t") for l in conllu if l != "" and not l.startswith("#")]
    conllu = [dict(zip(columns, l)) for l in conllu]
    # conllu = {l[0]: l for l in conllu}
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


for subfolder in tqdm(list(ud_dir.iterdir())):
    print(f"{subfolder.name}")
    # if subfolder.name == "WAC":
    #     continue

    if not subfolder.is_dir():
        print(f"{subfolder.name} is not a folder")
        continue

    if subfolder.name[-1].isdigit():
        print(f"{subfolder.name} is a number")
        continue

    if "WAC" != subfolder.name:
        continue

    all_txt = StringIO()
    for connlu in subfolder.glob("*.conllu"):
        # print(connlu)
        with open(connlu, "r", encoding="utf-8") as f:
            all_txt.write(f.read())

    sents = [s.split("# sent_id = ")[-1] for s in all_txt.getvalue().split("\n\n") if s != ""]
    sents = [s.split("\n", 1) for s in sents]
    sents = {s[0]: conllu_to_dict(s[1]) for s in sents}

    exports_sub = exports_dir / subfolder.name

    for export in exports_sub.glob("*.csv"):
        df = pd.read_csv(export, low_memory=False)  # .fillna("")

        # df["left_context"] = df["left_context"].fillna("")
        # df["right_context"] = df["right_context"].fillna("")

        pivot_datas = []
        for line, row in df.iterrows():
            try:
                sent = sents[str(row["sent_id"])]
            except Exception as e:
                print(f"{row['sent_id'] = }")
                print(f"{subfolder.name = }")
                print(sents)
                raise

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
                # raise Exception("pivot is empty")
                print("pivot is empty")
                continue
            try:
                # count = left.count(pivot) if left != "" else 0
                count = sum(1 for e in re.findall(fr"\W{re.escape(pivot)}\W", right) if e) + sum(1 for e in re.findall(fr"^{re.escape(pivot)}(\b)", left) if e) + sum(1 for e in re.findall(fr"(\b){re.escape(pivot)}$", left) if e)
                # print(count)
                # print(left)
                count = count if count >= 0 else 0



            except:
                print(left)
                print(pivot)
                raise

            # print([e[1] for e in sent])
            # print(pivot)

            words = [e["FORM"] for e in sent]
            try:
                token_nb = words.index(pivot, count)
            except:
                try:
                    token_nb = words.index(pivot)

                except:
                    print(row)
                    print(f"{words = }")
                    print(f"{pivot = }")
                    print(f"{count = }")
                    print(f"{subfolder.name = }")
                    raise

            # print(sent[token_nb])

            pivot_data = sent[token_nb]
            pivot_data["dist"] = dist

            pivot_datas.append(pivot_data)

        df_pivot = pd.DataFrame(pivot_datas)

        # print(df.head())
        # print(df_pivot.head())

        df = df.join(df_pivot)

        Xport = expoets_extended_dir / subfolder.name
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
