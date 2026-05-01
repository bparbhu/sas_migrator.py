from __future__ import annotations
import re

MACRO_DEF_RE = re.compile(r'%macro\s+([A-Za-z_][A-Za-z0-9_]*)(?:\((.*?)\))?\s*;(.*?)%mend\s*(?:\1)?\s*;', re.IGNORECASE | re.DOTALL)
MACRO_CALL_RE = re.compile(r'%([A-Za-z_][A-Za-z0-9_]*)\((.*?)\)\s*;', re.IGNORECASE | re.DOTALL)
LET_RE = re.compile(r'%let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*;', re.IGNORECASE)
SIMPLE_IF_RE = re.compile(r'%if\s+(.+?)\s+%then\s+%do;(.*?)%end;\s*(?:%else\s+%do;(.*?)%end;)?', re.IGNORECASE | re.DOTALL)
SIMPLE_DO_RE = re.compile(r'%do\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\d+)\s+%to\s+(\d+)\s*;(.*?)%end;', re.IGNORECASE | re.DOTALL)

def remove_macro_definitions(text: str) -> str:
    return MACRO_DEF_RE.sub("", text)

def parse_kv_args(raw: str) -> dict[str, str]:
    out = {}
    for part in [p.strip() for p in raw.split(",") if p.strip()]:
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip().lower()] = v.strip()
    return out

def substitute_vars(text: str, variables: dict[str, str]) -> str:
    for key, value in variables.items():
        text = re.sub(rf"&{re.escape(key)}\.?\b", lambda _: value, text, flags=re.IGNORECASE)
    return text

def eval_simple_condition(cond: str, variables: dict[str, str]):
    cond = substitute_vars(cond.strip(), variables).strip()
    m = re.match(r'("?[^"]+"?|\'.*?\'|[A-Za-z0-9_.-]+)\s*(=|ne|eq|gt|lt|ge|le)\s*("?[^"]+"?|\'.*?\'|[A-Za-z0-9_.-]+)', cond, re.IGNORECASE)
    if not m:
        return None
    left, op, right = m.group(1).strip("\"'"), m.group(2).lower(), m.group(3).strip("\"'")
    try:
        left_val, right_val = float(left), float(right)
    except Exception:
        left_val, right_val = left, right
    return {
        "=": left_val == right_val,
        "eq": left_val == right_val,
        "ne": left_val != right_val,
        "gt": left_val > right_val,
        "lt": left_val < right_val,
        "ge": left_val >= right_val,
        "le": left_val <= right_val,
    }[op]

def expand_control_flow(text: str, variables: dict[str, str], unsupported: list[str]) -> str:
    changed = True
    while changed:
        changed = False

        def if_repl(m):
            nonlocal changed
            result = eval_simple_condition(m.group(1), variables)
            if result is None:
                unsupported.append(f"Unsupported macro %if condition: {m.group(1)}")
                return m.group(0)
            changed = True
            return m.group(2) if result else (m.group(3) or "")

        text = SIMPLE_IF_RE.sub(if_repl, text)

        def do_repl(m):
            nonlocal changed
            var, start, end, body = m.group(1).lower(), int(m.group(2)), int(m.group(3)), m.group(4)
            pieces = []
            for i in range(start, end + 1):
                local_vars = dict(variables)
                local_vars[var] = str(i)
                pieces.append(substitute_vars(body, local_vars))
            changed = True
            return "".join(pieces)

        text = SIMPLE_DO_RE.sub(do_repl, text)
    return text

def expand(text: str, manifest: dict, initial_let_vars: dict | None = None):
    unsupported = []
    macros = manifest.get("macros", {})
    variables = dict(manifest.get("global_lets", {}))
    if initial_let_vars:
        variables.update(initial_let_vars)

    for m in LET_RE.finditer(text):
        variables[m.group(1).lower()] = substitute_vars(m.group(2).strip(), variables)
    text = LET_RE.sub("", text)
    text = remove_macro_definitions(text)
    text = substitute_vars(text, variables)

    def macro_repl(m):
        name = m.group(1).lower()
        spec = macros.get(name)
        if not spec:
            unsupported.append(f"Unknown macro: %{name}")
            return m.group(0)
        args = parse_kv_args(m.group(2) or "")
        local_vars = dict(variables)
        body = spec["body"]
        for p in spec["params"]:
            if p.lower() not in args:
                unsupported.append(f"Missing arg {p} in %{name}")
                return m.group(0)
            local_vars[p.lower()] = substitute_vars(args[p.lower()], local_vars)
        body = substitute_vars(body, local_vars)
        return expand_control_flow(body, local_vars, unsupported)

    changed = True
    while changed:
        changed = False
        new_text = MACRO_CALL_RE.sub(macro_repl, text)
        if new_text != text:
            changed = True
            text = new_text
            text = substitute_vars(text, variables)
            text = expand_control_flow(text, variables, unsupported)

    text = substitute_vars(text, variables)
    text = expand_control_flow(text, variables, unsupported)
    return text, sorted(set(unsupported))
