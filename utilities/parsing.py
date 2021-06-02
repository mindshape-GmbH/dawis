import operator
import re

COMPERASION_OPERATORS = {
    '==': operator.eq,
    '!=': operator.ne,
    '>': operator.gt,
    '<': operator.lt,
    '>=': operator.ge,
    '<=': operator.le,
}

COMPERASION_PLACEHOLDERS = {
    '_empty': ''
}

COMPERASION_VALUES = {
    'null': None,
    'none': None
}


def parse_comparison(comparison: str, variables: dict) -> bool:
    try:
        comparison: list = comparison.format(**{**variables, **COMPERASION_PLACEHOLDERS}).split()

        for key in ([0] if 1 == len(comparison) else [0, 2]):
            if comparison[key] in COMPERASION_VALUES:
                comparison[key] = COMPERASION_VALUES[comparison[key].lower()]

            try:
                comparison[key] = int(comparison[key])
                continue
            except ValueError:
                pass

            try:
                comparison[key] = float(comparison[key])
                continue
            except ValueError:
                pass

            if 'true' == comparison[key]:
                comparison[key] = True
            elif 'false' == comparison[key]:
                comparison[key] = False

        if 1 == len(comparison) and type(comparison[0]) is bool:
            return comparison[0]
        elif 3 != len(comparison):
            raise KeyError()

        return COMPERASION_OPERATORS.get(comparison[1])(comparison[0], comparison[2])
    except KeyError:
        raise SyntaxError
