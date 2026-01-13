from typing import Optional, Dict, List, Tuple

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
                     reject_unknown: bool=True,
                     user: str='no-user') -> NONETYPE:
    """Define a series as a named formula.

    .. highlight:: python
    .. code-block:: python

      tsa.register_formula('sales.eu', '(add (series "sales.fr") (series "sales.be"))')
    """

    self.othersources.forbidden(
        name,
        'not allowed to register a formula on a secondary source'
    )

    with self.engine.begin() as cn:
        self.tsh.register_formula(
            cn,
            name,
            formula,
            reject_unknown=reject_unknown,
            user=user
        )


@extend(mainsource)
def eval_formula(self,
                 formula: str,
                 revision_date: pd.Timestamp=None,
                 from_value_date: pd.Timestamp=None,
                 to_value_date: pd.Timestamp=None,
                 tz=None) -> pd.Series:
    """Execute a formula on the spot.

    .. highlight:: python
    .. code-block:: python

      tsa.eval_formula('(add (series "sales.fr") (series "sales.be"))')
    """

    # basic syntax check
    tree = parse(formula)

    with self.engine.begin() as cn:
        # type checking
        i = interpreter.Interpreter(cn, self, {})
        rtype = types.typecheck(tree, env=i.env)
        if not types.sametype(rtype, pd.Series):
            # this normalizes the formula
            formula = serialize(tree)
            raise TypeError(
                f'formula `{formula}` must return a `Series`, not `{rtype.__name__}`'
            )

        return self.tsh.eval_formula(
            cn,
            formula,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            tz=tz
        )


@extend(mainsource)
def formula(self,
            name: str,
            display: bool=True,
            expanded: bool=False,
            remote: bool=True,
            level: int=-1) -> Optional[str]:
    """Get the formula associated with a name.

    .. highlight:: python
    .. code-block:: python

      tsa.formula('sales.eu')
      ...
      '(add (series "sales.fr") (series "sales.be"))')

    Expanding means replacing all `series` expressions that are
    formulas with the formula contents.

    It can be all-or-nothing with the expanded parameter or asked for
    a defined level (stopping the expansion process).

    The maximum level can be obtained through the `formula_depth` api
    call.

    """
    expanded = expanded or level >= 0

    with self.engine.begin() as cn:
        form = self.tsh.formula(cn, name)
        if form:
            if not expanded:
                return form

            tree = self.tsh._expanded_formula(
                cn,
                form,
                level=level,
                display=display,
                remote=remote
            )
            if tree:
                return serialize(tree)

    # NOTE: pass levels and remote
    # except: if we are actually a primary ... do we want to look otherwise ?
    return self.othersources.formula(
        name,
        display=display,
        expanded=expanded
    )


@extend(altsources)
def formula(self,  # noqa: F811
            name: str,
            display: bool=True,
            expanded: bool=False,
            remote: bool=True,
            level: int=-1) -> Optional[str]:
    source = self._findsourcefor(name)
    if source is None:
        return

    return source.tsa.formula(
        name,
        display=display,
        expanded=expanded,
        # NOTE: we don't know how to actually test this
        # as testing for a local -> remote1 -> remote2
        # is not cracked yet.
        remote=remote,
        level=level
    )


@extend(mainsource)
def formula_depth(self, name: str):
    """Compute the depth of a formula.

    The depth is the maximum number of formula series sub expressions
    that have to be traversed to get to the bottom.
    """
    with self.engine.begin() as cn:
        depth = self.tsh.depth(cn, name)

    if depth is None:
        return self.othersources.formula_depth(name)

    return depth


@extend(mainsource)
def depends(self, name: str, direct=False, reverse=False) -> List[str]:
    with self.engine.begin() as cn:
        if not self.tsh.exists(cn, name):
            return []

        if reverse:
            return self.tsh.dependents(cn, name, direct=direct, static=False)

        return self.tsh.depends(cn, name, direct=direct)


@extend(altsources)
def formula_depth(self, name: str) -> int:  # noqa
    source = self._findsourcefor(name)
    if source is None:
        return 0
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

    with self.engine.begin() as cn:
        if form is None:
            if not self.tsh.exists(cn, name):
                return self.othersources.formula_components(
                    name,
                    expanded=expanded
                )
            return

        parsed = parse(form)
        names = list(
            self.tsh.find_series(cn, parsed, static=False)
        )

        # compute expansion of the remotely defined formula
        remotes = [
            name for name in names
            if not self.tsh.exists(cn, name)
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


@extend(mainsource)
def oldformulas(self, name: str) -> List[Tuple[str, pd.Timestamp]]:
    with self.engine.begin() as cn:
        if self.tsh.exists(cn, name):
            return self.tsh.oldformulas(cn, name)

    return self.othersources.oldformulas(name)


@extend(altsources)
def oldformulas(self, name):  # noqa: F811
    source = self._findsourcefor(name)
    if source is None:
        return []
    return source.tsa.oldformulas(name)


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


@extend(altsources)
def group_formula(self,  # noqa: F811
                name: str,
                expanded: bool=False):
    source = self._findsourceforgroup(name)
    if source is None:
        return {}
    return source.tsa.group_formula(name, expanded=expanded)


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

    Given a formula named "form1"::

        (add (series "foo") (series "bar") (series "quux"))

    ... where one wants to treat "foo" and "bar" as groups.
    The binding is expressed as a dataframe::

        binding = pd.DataFrame(
            [
                ['foo', 'foo-group', 'group'],
                ['bar', 'bar-group', 'group'],
            ],
            columns=('series', 'group', 'family')
        )

    The complete registration looks like::

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
        bindings = self.tsh.bindings_for(cn, name)

    if bindings is not None:
        return bindings

    return self.othersources.bindings_for(name)


@extend(altsources)
def bindings_for(self, name):  # noqa
    source = self._findsourceforgroup(name)
    if source is None:
        return None
    return source.tsa.bindings_for(name)


@extend(mainsource)
def group_eval_formula(self,
                 formula: str,
                 revision_date: pd.Timestamp=None,
                 from_value_date: pd.Timestamp=None,
                 to_value_date: pd.Timestamp=None,
                 tz=None) -> pd.Series:
    """Execute a group formula on the spot.

    .. highlight:: python
    .. code-block:: python

      tsa.group_eval_formula('(group-add (group "group1") (group "group2"))')
    """

    # basic syntax check
    tree = parse(formula)

    with self.engine.begin() as cn:
        # type checking
        i = interpreter.GroupInterpreter(
            cn,
            self,
            {
                'revision_date': revision_date,
                'from_value_date': from_value_date,
                'to_value_date': to_value_date
            }
        )
        rtype = types.typecheck(tree, env=i.env)
        if not types.sametype(rtype, pd.DataFrame):
            # this normalizes the formula
            formula = serialize(tree)
            raise TypeError(
                f'formula `{formula}` must return a `Group`, not `{rtype.__name__}`'
            )

        return self.tsh.group_eval_formula(
            cn,
            formula,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            tz=tz,
        )
