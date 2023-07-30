import re
from pathlib import Path
from io import StringIO
import pandas as pd

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


for subfolder in ud_dir.iterdir():
    # if subfolder.name == "WAC":
    #     continue

    if not subfolder.is_dir():
        print(f"{subfolder} is not a folder")
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
        df = pd.read_csv(export).fillna("")

        pivot_datas = []
        for line, row in df.iterrows():
            try:
                sent = sents[row["sent_id"]]
            except Exception as e:
                print(e)
                try:
                    sent = sents[str(row["sent_id"])]
                except:
                    print(f"{row['sent_id'] = }")
                    print(f"{subfolder.name = }")
                    print(sents)
                    raise

            left = row["left_context"]
            pivot = row["pivot"]
            try:
                # count = left.count(pivot) if left != "" else 0
                count = sum(1 for e in left if e == pivot) - sum(1 for e in re.findall(fr"\w*{pivot}\w*", left) if e)
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
                print(f"{words = }")
                print(f"{pivot = }")
                print(f"{count = }")
                raise
            # print(sent[token_nb])

            pivot_data = sent[token_nb]

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
        df.to_excel(Xport.with_suffix('.xlsx'), index=False)
        df.to_pickle(Xport.with_suffix('.pkl'))
        df.to_json(Xport.with_suffix('.jsonl'), orient='records', lines=True)
        df.to_json(Xport.with_suffix('.json'), orient='records')
