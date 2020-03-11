# Helidon DB Client Example with M:N relation processing

Imagibne that there is an entity _Pokemon_ with some name. Each of the pokemons may be of several types (e.g. Pidgey which is type _normal_ and _flying_).

Let's create corresponding model in a database:

**Pokemons**
- id INTEGER NOT NULL PRIMARY KEY
- name VARCHAR(...) NOT NULL

**Types**
- id INTEGER NOT NULL PRIMARY KEY
- name VARCHAR(...) NOT NULL

**PokemonTypes** (M:N relation between Pokemons and Types)
- pid INTEGER NOT NULL REFERENCES Pokemons(id)
- tid INTEGER NOT NULL REFERENCES Types(id)
- UNIQUE(pid,tid)

## Sample use-case

This example shows, how to build single JSON structure from data combined from all tables.

*JSON Document Structure*

Target JSON structure is list of Pokemon objects, but each of those object contains _type_ parameter with an array of this pokemon types names.
```
[
  {
    "id": <Pokemon_ID>,
    "name": <Pokemon_Name>,
    "type": [<Type1_NAme>, <Type1_NAme>, ...]
  }
]
```

## Sample code

Sample code in _PokemonService_ class shows how to read all necessary data from database using Helidon DB Client API and how to build target JSON structure from them.

Use-case entry point is `PokemonService#listPokemons(ServerRequest,ServerResponse)` method which executes primary SQL statement to read all pokemons from _Pokemons_ table.
Statement result set (_DbRows_) is processed by `Flow.Subscriber<DbRow>` interface implementation class `ReadPokemonRows`.
