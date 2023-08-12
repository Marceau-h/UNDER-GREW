import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
from upsetplot import from_contents, UpSet, plot
from random import randint


def make_percent(a: int, b: int):
    return round(a / b * 100, 2)


def get_one_random(df: pd.DataFrame):
    if len(df) == 0:
        return None

    if len(df) == 1:
        return df.iloc[0]

    return df.iloc[randint(0, len(df) - 1)]


def get_one_random_not_in(df: pd.DataFrame, dfallpristine: pd.DataFrame):
    df = dfallpristine[~dfallpristine["sent_id"].isin(df["sent_id"])]
    return get_one_random(df)


def row_to_str(row):
    return f"{row['left_context']};;{row['pivot']};;{row['right_context']}"


def random_not_in_str(df: pd.DataFrame, dfallpristine: pd.DataFrame):
    row = get_one_random_not_in(df, dfallpristine)
    if row is None:
        return None
    return row_to_str(row)


def stats(df, dfall, dfallpristine):
    return {
        "# sent": len(df),
        "# sent successif": len(dfall),
        "% sent": make_percent(len(df), len(dfallpristine)),
        "% sent successif": make_percent(len(dfall), len(dfallpristine)),
        "filtered": random_not_in_str(df, dfallpristine),
    }


def is_not_pron(x):
    return not x.endswith("se ") and not x.endswith("s'")


def atleast(i: int, df: pd.DataFrame, col: str, val: str):
    return len(df[df[col] == val]) >= i


def too_close(dist: int, max_dist: int) -> bool:
    return 0 < dist <= max_dist if dist else False


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
    file_no_nothing = subdir / "VERB-no-nothing.csv"
    file_in_idiom = subdir / "Verb_in_idiom.csv"
    file_fixed = subdir / "fixed-VERB.csv"

    df_all = pd.read_csv(file_all).fillna("")
    df_all_pristine = pd.read_csv(file_all).fillna("")
    df_direct = pd.read_csv(file_direct).fillna("")
    df_no_obj = pd.read_csv(file_no_obj).fillna("")
    df_no_nothing = pd.read_csv(file_no_nothing).fillna("")
    df_in_idiom = pd.read_csv(file_in_idiom).fillna("")
    df_fixed = pd.read_csv(file_fixed).fillna("")

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

    df_out_idiom = df_all_pristine[~df_all_pristine["sent_id"].isin(df_in_idiom["sent_id"])]
    df_all = df_all[~df_all["sent_id"].isin(df_in_idiom["sent_id"])]

    for_stats["out_idiom"] = stats(df_out_idiom, df_all, df_all_pristine)

    df_something = df_all_pristine[~df_all_pristine["sent_id"].isin(df_no_nothing["sent_id"])]
    df_all = df_all[~df_all["sent_id"].isin(df_no_nothing["sent_id"])]

    for_stats["something"] = stats(df_something, df_all, df_all_pristine)

    df_not_fixed = df_all_pristine[~df_all_pristine["sent_id"].isin(df_fixed["sent_id"])]
    df_all = df_all[~df_all["sent_id"].isin(df_in_idiom["sent_id"])]

    for_stats["not_fixed"] = stats(df_not_fixed, df_all, df_all_pristine)


    df_too_close = df_all_pristine[df_all_pristine["dist"].apply(lambda x: too_close(x, 3))]
    df_all = df_all[df_all["dist"].apply(lambda x: too_close(x, 3))]

    for_stats["too_close"] = stats(df_too_close, df_all, df_all_pristine)

    # five_or_more = [x for x in lst_lemmas_all if lst_lemmas_all.count(x) >= 5]
    #
    # df_five_or_more = df_all_pristine[df_all_pristine["LEMMA"].isin(five_or_more)]
    # df_all = df_all[df_all["LEMMA"].isin(five_or_more)]
    #
    # for_stats["five_or_more"] = stats(df_five_or_more, df_all, df_all_pristine)

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
        "out_idiom": {s for s in df_out_idiom["sent_id"].tolist()},
        "something": {s for s in df_something["sent_id"].tolist()},
        "not_fixed": {s for s in df_not_fixed["sent_id"].tolist()},
        "too_close": {s for s in df_too_close["sent_id"].tolist()},
        # "five_or_more": {s for s in df_five_or_more["sent_id"].tolist()},
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

    set_labels = {
        "all",
        "no_obj",
        "in_both",
        "out_idiom",
        "something",
        "not_fixed",
        "too_close",
        # "five_or_more",
        "no_pass",
        "no_pron",
    }
    colors = ["#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00", "#a65628", "#f781bf", "#999999", "#000000"]

    for s, c in zip(set_labels, colors):
        upset.style_subsets(
            present=set_labels - {s},
            absent={s},
            facecolor=c,
        )

    fig = plt.figure()
    fig.figsize = (20, 40)
    # fig.legend(loc=7)

    upset.make_grid(
        fig
    )

    upset.plot(fig=fig)

    plt.savefig(f"filtres/{subdir.name}.png")
