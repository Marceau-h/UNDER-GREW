# Utilisation de Grew

## Accès en ligne

Grew est accessible en ligne à l'adresse suivante : [https://grew.marceau-h.fr/](https://grew.marceau-h.fr/)


## Requêtes

### Requêtes classiques

Les requêtes classiques sont des requêtes sur les graphes de dépendance ainsi que sur les étiquettes des tokens.

```grew
pattern { N [upos="NUM"] }
```

Cette requête renvoie les tokens qui ont le POS tag `NUM`.

```grew
pattern {
V [upos="VERB"];
N [];
V -[obj]-> N
}
```

Cette requête renvoie les tokens qui ont le POS tag `VERB` et qui ont un objet.

```grew
pattern {
V [upos="VERB"];
N [lemma="chat"];
V -[obj]-> N
}
```

Cette requête renvoie les tokens qui ont le POS tag `VERB` et qui ont un objet dont le lemme est `chat`.


### Faire une requête sur l'identifiant d'une phrase

```grew
global { sent_id = "ID_DE_LA_PHRASE" }
```

La requête renvoie le graphe de dépendance de la phrase.
On peut combiner cette requête avec d'autres requêtes classiques.

```grew
global { sent_id = "ID_DE_LA_PHRASE" }
pattern { N [upos="NUM"] }
```

Cette requête renvoie les tokens de la phrase qui ont le POS tag `NUM`.

Le sent_id peut aussi être une expression régulière.

```grew
global { sent_id = re"FQB.*" }
```

Cette requête renvoie les phrases dont l'identifiant commence par `FQB`.

### Faire une requête sur le texte d'une phrase

```grew
global { text = "TEXTE_DE_LA_PHRASE" }
```

La requête renvoie le graphe de dépendance de la phrase.
On peut combiner cette requête avec d'autres requêtes classiques.

```grew
global { text = "TEXTE_DE_LA_PHRASE" }
pattern { N [upos="NUM"] }
```

Cette requête renvoie les tokens de la phrase qui ont le POS tag `NUM`.

Le texte peut aussi être une expression régulière.

```grew
global { text = re"[0-9]+$" }
```

Cette requête renvoie les phrases dont le texte se termine par un nombre.



