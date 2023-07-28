from pathlib import Path
import pandas as pd
from collections import Counter


def save_to_sheet(df, sheet, writer):
    df.to_excel(writer, sheet_name=sheet, index=True, header=True)


main = Path("Xports")
stats_folder_main = Path("stats")

for mode in ("pivot", "LEMMA"):
    stats_folder = stats_folder_main / mode
    jsons = list(main.glob("*/*.json"))
    nb_feuilles = 10

    stats = {}

    for json in jsons:
        corpus = json.parent.name
        query = json.stem

        stats_file = stats_folder / corpus
        stats_file.mkdir(parents=True, exist_ok=True)
        stats_file = stats_file / (query + ".xlsx")
        # print(f"{stats_file = }")

        if corpus not in stats:
            stats[corpus] = {}
        stats[corpus][query] = {}

        df = pd.read_json(json)

        if len(df) == 0:
            print(f"{json = }")
            print(f"{df = }")
            continue

        verbes = df[mode]
        verbes_freq = Counter([e.lower() for e in verbes])
        occurences = len(verbes)

        # print(df.head())

        tuplverbes = sorted([(v, verbes_freq[v]) for v in verbes_freq], key=lambda x: x[1], reverse=True)
        tuplverbes = {v: c for v, c in tuplverbes}

        temp = {
            "nb_occurences": occurences,
            "nb_verbes": len(verbes.unique()),
            **tuplverbes,
        }

        feuilles = {}

        for verbe, count in verbes_freq.most_common(nb_feuilles):
            phrases = df[df[mode] == verbe]
            feuilles[verbe] = {}
            feuilles[verbe]["stats"] = {
                "nb_occurences": count,
                "freq": count / occurences,
                "nb_phrases": len(phrases),
            }

            # [(sent_id, left, pivot, right) for sent_id, left, pivot, right in phrases[["sent_id", "left_context", "pivot", "right_context"]].values]
            feuilles[verbe]["phrases"] = phrases.values.tolist()

        with pd.ExcelWriter(stats_file, engine='xlsxwriter') as writer:

            tempdf = pd.DataFrame(temp, index=["", ])
            tempdf = tempdf.T

            save_to_sheet(tempdf, "Globales", writer)

            for verbe, content in feuilles.items():
                tempdf = pd.DataFrame(content["stats"], index=[0])
                save_to_sheet(tempdf, f"{verbe}_stats", writer)

                tempdf = pd.DataFrame(content["phrases"], columns=phrases.columns)
                save_to_sheet(tempdf, f"{verbe}_phrases", writer)


        temp["feuilles"] = feuilles

        stats[corpus][query] = temp

    with pd.ExcelWriter(stats_folder / "stats.xlsx", engine='xlsxwriter') as writer:
        for corpus, content in stats.items():
            tempdf = pd.DataFrame(content).T
            save_to_sheet(tempdf, corpus, writer)

# Round 2
for mode in ("pivot", "LEMMA"):
    stats["all"] = {}

    for corpus in stats:
        if corpus == "all":
            continue
        for k, v in stats[corpus]["VERB"].items():
            if k == "feuilles":
                if k not in stats["all"]:
                    stats["all"][k] = {}
                for verbe, content3 in v.items():
                    if verbe not in stats["all"][k]:
                        stats["all"][k][verbe] = {}
                        stats["all"][k][verbe]["stats"] = {}
                        stats["all"][k][verbe]["phrases"] = []

                    for k2, v2 in content3["stats"].items():
                        if k2 not in stats["all"][k][verbe]["stats"]:
                            stats["all"][k][verbe]["stats"][k2] = 0

                        stats["all"][k][verbe]["stats"][k2] += v2

                    stats["all"][k][verbe]["phrases"] += content3["phrases"]

            else:
                if k not in stats["all"]:
                    stats["all"][k] = 0

                stats["all"][k] += v

    temp = stats["all"].copy()
    temp.pop("feuilles")
    stats_file = stats_folder_main / mode / "all.xlsx"

    with pd.ExcelWriter(stats_file, engine='xlsxwriter') as writer:

        tempdf = pd.DataFrame(temp, index=["", ])
        tempdf = tempdf.T

        tempdf = tempdf.sort_values(tempdf.columns[0], ascending=False)

        save_to_sheet(tempdf, "Globales", writer)

        for verbe, content in stats["all"]["feuilles"].items():
            tempdf = pd.DataFrame(content["stats"], index=[0])
            save_to_sheet(tempdf, f"{verbe}_stats", writer)

            tempdf = pd.DataFrame(content["phrases"], columns=phrases.columns)
            save_to_sheet(tempdf, f"{verbe}_phrases", writer)
