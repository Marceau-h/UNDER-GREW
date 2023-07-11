from pathlib import Path
import pandas as pd
from collections import Counter

main = Path("exports")
stats_folder = Path("stats")
jsons = list(main.glob("*/*.json"))
nb_feuilles = 10

stats = {}

for json in jsons:
    corpus = json.parent.name
    query = json.stem

    stats_file = stats_folder / corpus
    stats_file.mkdir(parents=True, exist_ok=True)
    stats_file = stats_file / (query + ".xlsx")

    if corpus not in stats:
        stats[corpus] = {}
    stats[corpus][query] = {}

    df = pd.read_json(json)
    verbes = df["pivot"]
    verbes_freq = Counter(verbes)
    occurences = len(verbes)

    # print(df.head())

    temp = {
        "nb_occurences": occurences,
        "nb_verbes": len(verbes.unique()),

        "verbes_freq": ", ".join(sorted((f"{v}:{verbes_freq[v]}" for v in verbes_freq), key=lambda x: x[1], reverse=True)),
    }

    feuilles = {}

    for verbe, count in verbes_freq.most_common(nb_feuilles):
        phrases = df[df["pivot"] == verbe]
        feuilles[verbe] = {}
        feuilles[verbe]["stats"] = {
            "nb_occurences": count,
            "freq": count / occurences,
            "nb_phrases": len(phrases),
        }

        # [(sent_id, left, pivot, right) for sent_id, left, pivot, right in phrases[["sent_id", "left_context", "pivot", "right_context"]].values]
        feuilles[verbe]["phrases"] = phrases[["sent_id", "left_context", "pivot", "right_context"]].values.tolist()

    with pd.ExcelWriter(stats_file, engine='xlsxwriter') as writer:

        tempdf = pd.DataFrame(temp, index=[0])
        tempdf.to_excel(writer, index=False, sheet_name="Globales")

        for verbe, content in feuilles.items():
            tempdf = pd.DataFrame(content["stats"], index=[0])
            tempdf.to_excel(writer, index=False, sheet_name=f"{verbe}_stats")

            tempdf = pd.DataFrame(content["phrases"], columns=["sent_id", "left_context", "pivot", "right_context"])
            tempdf.to_excel(writer, index=False, sheet_name=f"{verbe}_phrases")


    temp["feuilles"] = feuilles


    stats[corpus][query] = temp




