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
        print(f"{stats_file = }")

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
            #
            # tempdf.to_excel(writer, sheet_name="Globales", index=True, header=False)
            # for column in df:
            #     column_width = max(df[column].astype(str).map(len).max(), len(column))
            #     col_idx = df.columns.get_loc(column)
            #     writer.sheets['my_analysis'].set_column(col_idx, col_idx, column_width)

            save_to_sheet(tempdf, "Globales", writer)

            for verbe, content in feuilles.items():
                tempdf = pd.DataFrame(content["stats"], index=[0])
                # tempdf.to_excel(writer, index=False, sheet_name=f"{verbe}_stats")
                save_to_sheet(tempdf, f"{verbe}_stats", writer)

                tempdf = pd.DataFrame(content["phrases"], columns=phrases.columns)
                # tempdf.to_excel(writer, index=False, sheet_name=f"{verbe}_phrases")
                save_to_sheet(tempdf, f"{verbe}_phrases", writer)

        # fix_worksheet = columns.XLSXAutoFitColumns(stats_file)
        # fix_worksheet.process_all_worksheets()

        temp["feuilles"] = feuilles


        stats[corpus][query] = temp




