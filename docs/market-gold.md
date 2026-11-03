# Résultats Gold de la surveillance de marché

Les trois jobs de `config/market/` produisent les mêmes données métier.
`indicators` contient une ligne par symbole et minute observée ; `alerts` possède
le même schéma et conserve seulement les lignes où `alert = true`.
Le couple `(symbol, minute_start)` identifie une ligne dans chaque résultat.

## Stockage

| Résultat | pandas / Polars | Spark |
| --- | --- | --- |
| Indicateurs | `${GOLD}/market/<engine>/indicators.parquet` | `local.market.indicators` |
| Alertes | `${GOLD}/market/<engine>/alerts.parquet` | `local.market.alerts` |

`<engine>` vaut `pandas` ou `polars`. Les sorties sont remplacées à chaque
exécution (`overwrite`). Les loaders créent les fichiers ou les tables Iceberg
depuis les DataFrames : aucune table PostgreSQL ni initialisation SQL n’est requise.
Les transactions nettoyées constituent la sortie Silver, décrite dans le README.

## Schéma commun aux indicateurs et aux alertes

Les types ci-dessous décrivent les données logiques ; leur représentation native
dépend du moteur. Les timestamps Parquet pandas/Polars sont sans fuseau explicite
et doivent être interprétés en UTC. La session Spark utilise UTC.

| Colonne | Type | Signification |
| --- | --- | --- |
| `symbol` | Texte | Instrument, par exemple `BTCUSDT` |
| `minute_start` | Timestamp UTC | Début de la minute observée |
| `open` | Float64 | Premier prix, selon l’ordre timestamp puis identifiant |
| `high` | Float64 | Prix maximal de la minute |
| `low` | Float64 | Prix minimal de la minute |
| `close` | Float64 | Dernier prix de la minute |
| `volume` | Float64 | Somme des quantités échangées |
| `notional` | Float64 | Somme de `price × quantity` |
| `trade_count` | Int64 | Nombre de transactions de la minute |
| `vwap` | Float64 | Prix moyen pondéré : `notional / volume` |
| `return_1m` | Float64, nullable | `close(t) / close(t − 1 minute) − 1` |
| `history_count` | Int64 | Nombre de minutes observées dans la fenêtre passée |
| `volume_mean` | Float64, nullable | Moyenne des volumes dans cette fenêtre |
| `volatility` | Float64, nullable | Écart-type des rendements passés, avec `ddof=1` |
| `history_ready` | Booléen | Historique suffisant pour appliquer les seuils |
| `volume_ratio` | Float64, nullable | `volume / volume_mean` |
| `alert` | Booléen | Historique suffisant et dépassement des deux seuils |

Par défaut, la fenêtre couvre les 60 minutes précédentes, minute courante exclue,
et exige 60 minutes observées. Les minutes manquantes ne sont pas ajoutées.
`return_1m` est manquant si la minute précédente manque ; `volatility` est
manquante avec moins de deux rendements disponibles. Une condition d’alerte
manquante donne `false`.

## Exemple de résultat de la démonstration

Le générateur produit 452 lignes brutes, dont 450 sont conservées après nettoyage.
L’agrégation donne 150 lignes Gold dans `indicators`, et le filtrage donne
2 lignes dans `alerts`. Voici un extrait des colonnes de ces deux alertes :

| symbol | minute_start (UTC) | close | volume | volume_mean | volume_ratio | return_1m | alert |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BTCUSDT | 2025-01-01 01:05:00 | 103000.0 | 15.0 | 3.0 | 5.0 | 0.03 | true |
| ETHUSDT | 2025-01-01 01:05:00 | 3090.0 | 15.0 | 3.0 | 5.0 | 0.03 | true |

Ces deux lignes ont `history_count = 60` et `history_ready = true`.
Le volume dépasse 3 fois sa moyenne et la variation absolue du prix dépasse 2 %.
Les résultats de données Binance dépendent des transactions téléchargées.

Après préparation et exécution du job Polars, consulter le résultat complet :

```bash
uv run python -c 'import polars as pl; print(pl.read_parquet("datalake/gold/market/polars/alerts.parquet"))'
```
