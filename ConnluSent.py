import json
import pickle

from ConnluLine import ConnluLine


class ConnluSent:

    def __init__(self, string: str = "", *args, id: str = "", lines: tuple[ConnluLine] = ()):
        if string:
            decoupe = [line.strip() for line in string.split("\n") if line != "" and not line.startswith("#")]
            self.id = decoupe[0]
            self.lines = tuple(ConnluLine(line) for line in decoupe[1:])

        elif id and lines:
            self.id = id
            self.lines = lines

        else:
            raise ValueError("ConnluSent must be initialized with a string or an id and a tuple of ConnluLine")

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

    def toJson(self):
        return {"id": self.id, "lines": tuple(line.toJson() for line in self.lines)}

    @classmethod
    def fromJson(cls, json_obj: str | dict[str, str]):
        if isinstance(json_obj, str):
            json_obj = json.loads(json)

        return cls(id=json_obj["id"], lines=tuple(ConnluLine.fromJson(line) for line in json_obj["lines"]))

    def toPickle(self):
        return pickle.dumps(self)

    @classmethod
    def fromPickle(cls, pickle_obj: bytes):
        return pickle.loads(pickle_obj)

