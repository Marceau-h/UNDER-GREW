import json
import pickle


class ConnluLine:
    columns: tuple[str] = ("ID", "FORM", "LEMMA", "UPOS", "XPOS", "FEATS", "HEAD", "DEPREL", "DEPS", "MISC")
    len_col = len(columns)

    def __init__(self, line: str | dict[str, str] = ""):
        if isinstance(line, dict):
            for col in self.columns:
                setattr(self, col, line[col])
            return

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

    def toJson(self):
        return {col: getattr(self, col) for col in self.columns}

    @classmethod
    def fromJson(cls, json_obj: str | dict[str, str]):
        if isinstance(json_obj, str):
            json_obj = json.loads(json)

        return cls(**json_obj)


    def toPickled(self):
        return pickle.dumps(self)
