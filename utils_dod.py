# %%
def validate_aos(aos):
    aos = iter(aos)
    first = set(next(aos).keys())
    uniform = all(set(s.keys())==first for s in aos)
    return uniform

def validate_soa(soa):
    lengths = [len(v) for v in soa.values()]
    uniform = all(l==lengths[0] for l in lengths[1:])
    return uniform

def validate_tab(tab):
    info, data = tab
    number_of_columns = len(info["columns"])
    uniform = all(len(item)==number_of_columns for item in data)
    return uniform

# %%

def aos2soa(aos):
    keys = next(iter(aos)).keys()
    soa = {k: [] for k in keys}
    for struct in aos:
        for key in keys:
            soa[key].append(struct[key])
    return soa

def soa2aos(soa):
    keys = list(soa.keys())
    structs = list(zip(*soa.values()))
    aos = [dict(zip(keys, struct)) for struct in structs]
    return aos

def soa2tab(soa):
    keys = list(soa.keys())
    info = {"columns": keys}
    data = list(zip(*soa.values()))
    return (info, data)

def aos2tab(aos):
    keys = next(iter(aos)).keys()
    info = {"columns": list(keys)}
    data = [tuple(s.values()) for s in aos]
    return (info, data)

def tab2soa(tab):
    keys = tab[0]["columns"]
    data = table[1]
    soa = dict(zip(keys, map(list, zip(*data))))
    return soa

def tab2aos(tab):
    keys = tab[0]["columns"]
    data = table[1]
    aos = [dict(zip(keys, line)) for line in data]
    return aos

# %%
array_of_structs = [
    {"nome": "enrico", "cognome": "giampieri"},
    {"nome": "nico", "cognome": "curti"},
]

struct_of_array = {
    "nome": ["enrico", "nico"],
    "cognome": ["giampieri", "curti"],
}

table = (
    {"columns": ["nome", "cognome"]},
    [
        ("enrico", "giampieri"),
        ("nico", "curti"),
    ],
)

assert aos2soa(array_of_structs) == struct_of_array
assert aos2tab(array_of_structs) == table
assert soa2aos(struct_of_array) == array_of_structs
assert soa2tab(struct_of_array) == table
assert tab2soa(table) == struct_of_array
assert tab2aos(table) == array_of_structs

assert table == soa2tab(aos2soa(tab2aos(table)))
assert table == aos2tab(soa2aos(tab2soa(table)))
# %%
import json

def tab2jsonlines(tab):
    yield json.dumps(tab[0])
    for line in tab[1]:
        yield json.dumps(line)

def soa2jsonlines(soa):
    for key, value in soa.items():
        yield json.dumps((key, value))

def aos2jsonlines(aos):
    for struct in aos:
        yield json.dumps(struct)

