from typing import Optional, Dict

import pandas as pd

from psyl.lisp import (
    parse,
    serialize
)
from tshistory.util import extend
from tshistory.api import (
    altsources,
    mainsource
)
from tshistory_formula import (
    types,
    interpreter
)


NONETYPE = type(None)


@extend(mainsource)
def register_formula(self,
                     name: str,
                     formula: str,
                     reject_unknown: bool=True) -> NONETYPE:
    """Define a series as a named formula.

    .. highlight:: python
    .. code-block:: python

      tsa.register_formula('sales.eu', '(add (series "sales.fr") (series "sales.be"))')
    """

    self.tsh.register_formula(
        self.engine,
        name,
        formula,
        reject_unknown=reject_unknown
    )


@extend(mainsource)
def eval_formula(self,
                 formula: str,
                 revision_date: pd.Timestamp=None,
                 from_value_date: pd.Timestamp=None,
                 to_value_date: pd.Timestamp=None) -> pd.Series:
    """Execute a formula on the spot.

    .. highlight:: python
    .. code-block:: python

      tsa.eval_formula('(add (series "sales.fr") (series "sales.be"))')
    """

    # basic syntax check
    tree = parse(formula)
    # this normalizes the formula
    formula = serialize(tree)

    with self.engine.begin() as cn:
        # type checking
        i = interpreter.Interpreter(cn, self, {})
        rtype = types.typecheck(tree, env=i.env)
        if not types.sametype(rtype, pd.Series):
            raise TypeError(
                f'formula `{formula}` must return a `Series`, not `{rtype.__name__}`'
            )

        return self.tsh.eval_formula(
            cn,
            formula,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date
        )


@extend(mainsource)
def formula(self,
            name: str,
            display: bool=False,
            expanded: bool=False,
            level: int=-1) -> Optional[str]:
    """Get the formula associated with a name.

    .. highlight:: python
    .. code-block:: python

      tsa.formula('sales.eu')
      ...
      '(add (series "sales.fr") (series "sales.be"))')

    The expanded parameter takes an integer between 0 and n and will
    proceed n successive expansions of the formula series expressions.

    Expanding means replacing all `series` expressions that are
    formulas with the formula contents.

    It can be all-or-nothing with the expanded parameter or asked for
    a defined level (stopping the expansion process).

    The maximum level can be obtained through the `formula_depth` api
    call.

    """
    expanded = expanded or level >= 0

    form = self.tsh.formula(self.engine, name)
    if form:
        if not expanded:
            return form

        tree = self.tsh._expanded_formula(
            self.engine,
            form,
            level=level,
            qargs=None if display else {}
        )
        if tree:
            return serialize(tree)

    return self.othersources.formula(
        name,
        display=display,
        expanded=expanded
    )


@extend(altsources)
def formula(self,  # noqa: F811
            name: str,
            display: bool=False,
            expanded: int=-1) -> Optional[str]:
    source = self._findsourcefor(name)
    if source is None:
        return
    return source.tsa.formula(
        name,
        display=display,
        expanded=expanded
    )


@extend(mainsource)
def formula_depth(self, name: str):
    """Compute the depth of a formula.

    The depth is the maximum number of formula series sub expressions
    that have to be traversed to get to the bottom.
    """
    depth = self.tsh.depth(self.engine, name)
    if depth is None:
        return self.othersources.formula_depth(name)

    return depth


@extend(altsources)
def formula_depth(self, name: str):  # noqa
    source = self._findsourcefor(name)
    if source is None:
        return
    return source.tsa.formula_depth(name)


@extend(mainsource)
def formula_components(self,
                       name: str,
                       expanded: bool=False) -> Optional[Dict[str, list]]:
    """Compute a mapping from series name (defined as formulas) to the
    names of the component series.

    If `expanded` is true, it will expand the formula before computing
    the components. Hence only "ground" series (stored or autotrophic
    formulas) will show up in the leaves.

    >>> tsa.formula_components('my-series')
    {'my-series': ['component-a', 'component-b']}

    >>> tsa.formula_components('my-series-2', expanded=True)
    {'my-series-2': [{'sub-component-1': ['component-a', 'component-b']}, 'component-b']}

    """
    form = self.formula(name)

    if form is None:
        if not self.tsh.exists(self.engine, name):
            return self.othersources.formula_components(
                name,
                expanded=expanded
            )
        return

    parsed = parse(form)
    names = list(
        self.tsh.find_series(self.engine, parsed)
    )

    # compute expansion of the remotely defined formula
    remotes = [
        name for name in names
        if not self.tsh.exists(self.engine, name)
        and self.formula(name)
    ]
    if remotes:
        # remote names will be replaced with their expansion
        rnames = []
        for rname in names:
            if rname in remotes:
                rnames.append(
                    self.othersources.formula_components(rname, expanded)
                )
            else:
                rnames.append(rname)
        names = rnames

    if expanded:
        # pass through some formula walls
        # where expansion > formula expansion
        subnames = []
        for cname in names:
            if not isinstance(cname, str) or not self.formula(cname):
                subnames.append(cname)
                continue
            subnames.append(
                self.formula_components(cname, expanded)
            )
        names = subnames

    return {name: names}


@extend(altsources)
def formula_components(self,  # noqa: F811
                       name: str,
                       expanded: bool=False) -> Optional[Dict[str, str]]:
    source = self._findsourcefor(name)
    if source is None:
        return {}
    return source.tsa.formula_components(name, expanded=expanded)


# groups

@extend(mainsource)
def register_group_formula(self, name: str, formula: str) -> NONETYPE:
    """Define a group as a named formula.

    You can use any operator (including those working on series)
    provided the top-level expression is a group.

    """
    with self.engine.begin() as cn:
        self.tsh.register_group_formula(
            cn, name, formula
        )


@extend(mainsource)
def group_formula(self, name: str, expanded: bool=False) -> Optional[str]:
    """Get the group formula associated with a name.

    """
    # NOTE: implement expanded
    with self.engine.begin() as cn:
        return self.tsh.group_formula(cn, name)


@extend(mainsource)
def register_formula_bindings(self,
                              groupname: str,
                              formulaname: str,
                              bindings: pd.DataFrame) -> NONETYPE:
    """Define a group by association of an existing series formula
    and a `bindings` object.

    The designated series formula will be then interpreted as a group
    formula.

    And the bindings object provides mappings that tell which
    components of the formula are to be interpreted as groups.

    Given a formula named "form1":

    `(add (series "foo") (series "bar") (series "quux"))`

    ... were one wants to treat "foo" and "bar" as groups.
    The binding is expressed as a dataframe:

        binding = pd.DataFrame(
        [
            ['foo', 'foo-group', 'group'],
            ['bar', 'bar-group', 'group'],
        ],
        columns=('series', 'group', 'family')
    )

    The complete registration looks like:

    register_formula_bindings(
        'groupname',
        'form1',
        pd.DataFrame(
        [
            ['foo', 'foo-group', 'group'],
            ['bar', 'bar-group', 'group'],
        ],
        columns=('series', 'group', 'family')
    ))

    Within a given family, all groups must have the same number of
    members (series) and the member roles are considered equivalent
    (e.g. meteorological scenarios).

    """
    with self.engine.begin() as cn:
        return self.tsh.register_formula_bindings(
            cn,
            groupname,
            formulaname,
            bindings
        )

@extend(mainsource)
def bindings_for(self, name: str):
    with self.engine.begin() as cn:
        return self.tsh.bindings_for(cn, name)
