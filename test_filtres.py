import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
from upsetplot import from_contents, UpSet, plot


def stats(df, dfall, dfallpristine):
    return {
        "# sent": len(df),
        "# sent successif": len(dfall),
        "% sent": len(df) / len(dfallpristine),
        "% sent successif": len(dfall) / len(dfallpristine),
    }


def is_not_pron(x):
    return not x.endswith("se ") and not x.endswith("s'")


def atleast(i: int, df: pd.DataFrame, col: str, val: str):
    return len(df[df[col] == val]) >= i


main = Path("Xports")
filtres = Path("filtres")
filtres.mkdir(exist_ok=True)


for subdir in main.iterdir():

    if not subdir.is_dir():
        continue

    print(subdir)

    for_stats = {}

    # test_all = Path("Xports/UD_French-GSD/VERB.csv")
    # test_direct = Path("Xports/UD_French-GSD/VERB-direct-obj.csv")
    # test_no_direct = Path("Xports/UD_French-GSD/VERB-no-direct-obj.csv")

    file_all = subdir / "VERB.csv"
    file_direct = subdir / "VERB-direct-obj.csv"
    file_no_obj = subdir / "VERB-no-obj.csv"


    df_all = pd.read_csv(file_all).fillna("")
    df_all_pristine = pd.read_csv(file_all).fillna("")
    df_direct = pd.read_csv(file_direct).fillna("")
    df_no_obj = pd.read_csv(file_no_obj).fillna("")

    for_stats["all"] = stats(df_all, df_all, df_all_pristine)

    df_all = df_no_obj

    for_stats["no_obj"] = stats(df_no_obj, df_all, df_all_pristine)

    lst_lemmas_all = df_all["LEMMA"].tolist()
    lst_lemmas_direct = df_direct["LEMMA"].tolist()
    lst_lemmas_no_direct = df_no_obj["LEMMA"].tolist()

    in_both = set(lst_lemmas_direct).intersection(lst_lemmas_no_direct)

    df_in_both = df_all_pristine[df_all_pristine["LEMMA"].isin(in_both)]
    df_all = df_all[df_all["LEMMA"].isin(in_both)]

    for_stats["in_both"] = stats(df_in_both, df_all, df_all_pristine)

    ten_or_more = [x for x in lst_lemmas_all if lst_lemmas_all.count(x) >= 10]

    df_ten_or_more = df_all_pristine[df_all_pristine["LEMMA"].isin(ten_or_more)]
    df_all = df_all[df_all["LEMMA"].isin(ten_or_more)]

    for_stats["ten_or_more"] = stats(df_ten_or_more, df_all, df_all_pristine)

    lst_feats_all = df_all_pristine["FEATS"].tolist()

    no_pass = [x for x in lst_feats_all if "Voice=Pass" not in x]  # and "VerbForm=Part" not in x]
    no_pass = set(no_pass)

    df_no_pass = df_all_pristine[df_all_pristine["FEATS"].isin(no_pass)]
    df_all = df_all[df_all["FEATS"].isin(no_pass)]

    for_stats["no_pass"] = stats(df_no_pass, df_all, df_all_pristine)

    df_no_pron = df_all_pristine[df_all_pristine["left_context"].apply(is_not_pron)]
    df_all = df_all[df_all["left_context"].apply(is_not_pron)]

    for_stats["no_pron"] = stats(df_no_pron, df_all, df_all_pristine)

    # for k, v in for_stats.items():
    #     print(k)
    #     for k2, v2 in v.items():
    #         print(f"\t{k2}: {v2}")

    df_all.to_csv(filtres / f"{subdir.name}-all.csv")
    df = pd.DataFrame(for_stats).T
    df.to_csv(filtres / f"{subdir.name}.csv")

    upset_data = {
        "all": {s for s in df_all_pristine["sent_id"].tolist()},
        "no_obj": {s for s in df_no_obj["sent_id"].tolist()},
        "in_both": {s for s in df_in_both["sent_id"].tolist()},
        "ten_or_more": {s for s in df_ten_or_more["sent_id"].tolist()},
        "no_pass": {s for s in df_no_pass["sent_id"].tolist()},
        "no_pron": {s for s in df_no_pron["sent_id"].tolist()},
    }

    upset_data = from_contents(upset_data)

    upset = UpSet(
        upset_data,
        orientation="vertical",
        sort_by="cardinality",
        show_percentages=True,
    )

    upset.style_subsets(
        present=["all", "in_both", "ten_or_more", "no_pass", "no_pron"],
        facecolor="gray",
    )

    upset.style_subsets(
        present=["all", "in_both", "ten_or_more", "no_pron"],
        absent=["no_pass"],
        facecolor="blue",
    )

    upset.style_subsets(
        present=["all", "ten_or_more", "no_pass", "no_pron"],
        absent=["in_both"],
        facecolor="green",
    )

    upset.style_subsets(
        present=["all", "in_both", "no_pass", "no_pron"],
        absent=["ten_or_more"],
        facecolor="red",
    )

    upset.style_subsets(
        present=["all", "in_both", "ten_or_more", "no_pass"],
        absent=["no_pron"],
        facecolor="yellow",
    )

    fig = plt.figure()
    fig.figsize = (20, 40)
    fig.legend(loc=7)

    upset.make_grid(
        fig
    )

    upset.plot(fig=fig)

    plt.savefig(f"filtres/{subdir.name}.png")
