from datetime import datetime
from typing import Optional
import io

import pandas as pd

from flask import request
from flask_restx import (
    inputs,
    Resource,
    reqparse
)

from tshistory.util import (
    series_metadata,
    tzaware_series
)
from tshistory.codecs import unpack_group, unpack_series
from tshistory.http.client import httpclient, unwraperror
from tshistory.http.util import (
    group_response,
    onerror,
    required_roles,
    series_response,
    utcdt
)
from tshistory.http.server import httpapi
from tshistory.http.client import strft


base = reqparse.RequestParser()
base.add_argument(
    'name',
    type=str,
    required=True,
    help='timeseries name'
)

formula = base.copy()
formula.add_argument(
    'expanded',
    type=inputs.boolean,
    default=False,
    help='return the recursively expanded formula'
)
formula.add_argument(
    'level',
    type=int,
    default=-1,
    help='levels of formula expansion'
)
formula.add_argument(
    'display',
    type=inputs.boolean,
    default=True,
    help='return undecorated formula (for display purposes)'
)
formula.add_argument(
    'remote',
    type=inputs.boolean,
    default=True,
    help='if expanded, perform expansion also for the remote formulas'
)

formula_components = base.copy()
formula_components.add_argument(
    'expanded',
    type=inputs.boolean,
    default=False,
    help='return the recursively expanded formula components'
)

register_formula = base.copy()
register_formula.add_argument(
    'text',
    type=str,
    required=True,
    help='source of the formula'
)
register_formula.add_argument(
    'reject_unknown',
    type=inputs.boolean,
    default=True,
    help='fail if the referenced series do not exist'
)
register_formula.add_argument(
    'user',
    type=str,
    default='no-user'
)

oldformulas = base.copy()

formula_depth = base.copy()

eval_formula = reqparse.RequestParser()
eval_formula.add_argument(
    'text',
    type=str,
    required=True,
    help='formula to evaluate'
)
eval_formula.add_argument(
    'revision_date', type=utcdt, default=None,
    help='revision date can be forced'
)
eval_formula.add_argument(
    'from_value_date', type=utcdt, default=None
)
eval_formula.add_argument(
    'to_value_date', type=utcdt, default=None
)
eval_formula.add_argument(
    'tz', type=str, default=None
)
eval_formula.add_argument(
    'format', type=str, choices=('json', 'tshpack'), default='json'
)

depends = base.copy()
depends.add_argument(
    'direct', type=inputs.boolean, default=False
)
depends.add_argument(
    'reverse', type=inputs.boolean, default=False
)

# groups

groupbase = reqparse.RequestParser()
groupbase.add_argument(
    'name',
    type=str,
    required=True,
    help='group name'
)

register_group_formula = groupbase.copy()
register_group_formula.add_argument(
    'text',
    type=str,
    required=True,
    help='source of the formula'
)

groupformula = groupbase.copy()
groupformula.add_argument(
    'expanded',
    type=inputs.boolean,
    default=False,
    help='return the recursively expanded formula'
)

group_eval_formula = reqparse.RequestParser()
group_eval_formula.add_argument(
    'text',
    type=str,
    required=True,
    help='formula to evaluate'
)
group_eval_formula.add_argument(
    'revision_date', type=utcdt, default=None,
    help='revision date can be forced'
)
group_eval_formula.add_argument(
    'from_value_date', type=utcdt, default=None
)
group_eval_formula.add_argument(
    'to_value_date', type=utcdt, default=None
)
group_eval_formula.add_argument(
    'tz', type=str, default=None
)
group_eval_formula.add_argument(
    'format', type=str, choices=('json', 'tshpack'), default='json'
)

boundformula = groupbase.copy()
boundformula.add_argument(
    'formulaname',
    type=str,
    help='name of the formula to exploit (create/update)'
)
boundformula.add_argument(
    'bindings',
    type=str,
    help='json representation of the bindings (create/update)'
)


class formula_httpapi(httpapi):

    def routes(self):
        super().routes()

        tsa = self.tsa
        api = self.api
        nss = self.nss
        nsg = self.nsg

        @nss.route('/formula')
        class timeseries_formula(Resource):

            @api.doc(
                responses={
                    200: 'Got content',
                    404: 'Does not exist',
                    409: 'Not a formula'
                },
                description="""Get the formula text for a computed series

**Parameters:**
- name: series name
- expanded: if true, recursively substitute all formula references (default: false)
- level: if >= 0, expand formulas to specified depth (default: -1, no limit)
- display: if false, include variable bindings in expansion (default: true)
- remote: if expanded, also expand formulas from secondary sources (default: true)

**Returns:** object with two fields
```json
{
  "level": -1,
  "formula": "(add (series \"base-series\") (series \"other-series\"))"
}
```

**Example:**
```
GET /series/formula?name=total-eu-sales
→ {"level": -1, "formula": "(add (series \"fr-sales\") (series \"de-sales\"))"}

GET /series/formula?name=total-eu-sales&expanded=true
→ {"level": -1, "formula": "(add (series \"raw-fr\") (series \"raw-de\"))"}
```
"""
            )
            @api.expect(formula)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = formula.parse_args()
                if not tsa.exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.formula(
                    args.name,
                    args.display,
                    args.expanded,
                    args.remote,
                    args.level
                )
                return {'level': args.level, 'formula': form}, 200

            @api.doc(
                responses={
                    200: 'Updated',
                    201: 'Created',
                    400: 'Malformed Formula',
                    409: 'Invalid Formula'
                },
                description="""Register or update a formula (computed series)

**Parameters:**
- name: series name for the formula
- text: Lisp formula expression
- reject_unknown: if true, fail if referenced series don't exist (default: true)
- user: username for audit trail (default: current user from request environment)

**Returns:** empty body
- 201: formula created
- 200: formula updated

**Errors:**
- 400: syntax error in formula (malformed Lisp expression)
- 409: type error, value error, or assertion error (invalid operators, unknown series, type mismatch)

**Example:**
```
PATCH /series/formula
  name=eu-sales&text=(add (series "fr-sales") (series "de-sales"))
→ 201 Created
```
"""
            )
            @api.expect(register_formula)
            @onerror
            @required_roles('admin', 'rw')
            def patch(self):
                args = register_formula.parse_args()

                exists = tsa.formula(args.name)
                if args.user == 'no-user':
                    user = request.environ.get('USER')
                else:
                    user = args.user
                try:
                    tsa.register_formula(
                        args.name,
                        args.text,
                        reject_unknown=args.reject_unknown,
                        user=user
                    )
                except TypeError as err:
                    api.abort(409, repr(err))
                except ValueError as err:
                    api.abort(409, repr(err))
                except AssertionError as err:
                    api.abort(409, repr(err))
                except SyntaxError as err:
                    api.abort(400, repr(err))
                except Exception:
                    raise

                return '', 200 if exists else 201

        @nss.route('/old_formulas')
        class old_formulas(Resource):

            @api.doc(
                responses={200: 'Got content', 404: 'Does not exist'},
                description="""Get the modification history of a formula

**Parameters:**
- name: series name

**Returns:** list of historical formula versions
```json
[
  ["(add (series \"a\") (series \"b\"))", "2025-01-15T10:30:00+00:00", "UTC", "alice"],
  ["(add (series \"a\") (constant 5))", "2024-12-10T08:00:00+00:00", "UTC", "bob"]
]
```

Each entry is a tuple: [formula_text, timestamp, timezone, username]

**Example:**
```
GET /series/old_formulas?name=total-sales
→ [["(add (series \"fr\") (series \"de\"))", "2025-01-15T10:00:00+00:00", "UTC", "alice"]]
```
"""
            )
            @api.expect(oldformulas)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = oldformulas.parse_args()
                return [
                    (form, stamp.isoformat(), str(stamp.tzinfo), user)
                    for form, stamp, user in tsa.oldformulas(args.name)
                ]

        @nss.route('/formula_depth')
        class formula_depth_(Resource):

            @api.doc(
                responses={200: 'Got content',
                           404: 'Does not exist'},
                description="""Get the nesting depth of a formula

Returns the number of times `(series <name>)` expressions must be substituted by their underlying formulas before all series expressions refer to primary (non-formula) series.

**Parameters:**
- name: series name

**Returns:** integer depth value

**Example:**
```
# primary series: base-temp
# formula level 1: adjusted-temp = (+ 2 (series "base-temp"))
# formula level 2: final-temp = (+ 1 (series "adjusted-temp"))

GET /series/formula_depth?name=base-temp
→ 0

GET /series/formula_depth?name=adjusted-temp
→ 1

GET /series/formula_depth?name=final-temp
→ 2
```
"""
            )
            @api.expect(formula_depth)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = formula_depth.parse_args()
                if not tsa.exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                return tsa.formula_depth(args.name)

        @nss.route('/formula_depends')
        class formula_depends(Resource):

            @api.doc(
                responses={200: 'Got content',
                           404: 'Does not exist'},
                description="""Get dependencies or dependents of a formula

Returns which series this formula uses (dependencies) or which formulas use this series (dependents).

**Parameters:**
- name: series name
- direct: if true, return only direct dependencies/dependents (default: false, returns all transitively)
- reverse: if true, return dependents instead of dependencies (default: false)

**Returns:** list of series names

**Example:**
```
# base-temp (primary)
# adjusted-temp = (+ 2 (series "base-temp"))
# final-temp = (+ 1 (series "adjusted-temp"))

GET /series/formula_depends?name=final-temp
→ ["adjusted-temp", "base-temp"]

GET /series/formula_depends?name=final-temp&direct=true
→ ["adjusted-temp"]

GET /series/formula_depends?name=base-temp&reverse=true
→ ["adjusted-temp", "final-temp"]

GET /series/formula_depends?name=base-temp&reverse=true&direct=true
→ ["adjusted-temp"]
```
"""
            )
            @api.expect(depends)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = depends.parse_args()
                return tsa.depends(
                    args.name,
                    direct=args.direct,
                    reverse=args.reverse
                )

        @nss.route('/eval_formula')
        class eval_formula_(Resource):

            @api.doc(
                responses={200: 'Got content',
                           400: 'Invalid formula'},
                description="""Evaluate a formula expression on the fly

Execute a formula without persisting it. Useful for developing and debugging formulas before registration, or for one-off computations.

**Parameters:**
- text: formula expression to evaluate
- revision_date: evaluate formula at this revision date (ISO8601, optional)
- from_value_date: restrict time range start (ISO8601, optional)
- to_value_date: restrict time range end (ISO8601, optional)
- tz: timezone for index conversion (optional)
- format: "json" or "tshpack" (default: json)

**Returns:** time series data in requested format

**Errors:**
- 400: syntax error (prefixed with "syn:") or type error (prefixed with "typ:")

**Example:**
```
POST /series/eval_formula
  text=(add (series "fr-sales") (series "de-sales"))&from_value_date=2025-01-01
→ {"2025-01-01T00:00:00+00:00": 100.5, "2025-01-02T00:00:00+00:00": 105.2, ...}
```
"""
            )
            @api.expect(eval_formula)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def post(self):
                args = eval_formula.parse_args()
                try:
                    ts = tsa.eval_formula(
                        args.text,
                        revision_date=args.revision_date,
                        from_value_date=args.from_value_date,
                        to_value_date=args.to_value_date,
                        tz=args.tz
                    )
                except SyntaxError as err:
                    return f'syn:{err}', 400
                except TypeError as err:
                    return f'typ:{err}', 400

                return series_response(
                    args.format,
                    ts,
                    series_metadata(ts),
                    200
                )

        @nss.route('/formula_components')
        class timeseries_formula_components(Resource):

            @api.doc(
                responses={200: 'Got content',
                           404: 'Does not exist',
                           409: 'Not a formula'},
                description="""Get the component series of a formula

Returns the series used by a formula, optionally expanded to show the full dependency tree.

**Parameters:**
- name: series name
- expanded: if true, recursively show all nested formula components (default: false)

**Returns:** object mapping formula name to list of components

**Example:**
```
# base-temp (primary)
# adjusted-temp = (+ 2 (series "base-temp"))
# final-temp = (+ 1 (series "adjusted-temp"))

GET /series/formula_components?name=final-temp
→ {"final-temp": ["adjusted-temp"]}

GET /series/formula_components?name=final-temp&expanded=true
→ {"final-temp": [{"adjusted-temp": ["base-temp"]}]}
```
"""
            )
            @api.expect(formula_components)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = formula_components.parse_args()

                if not tsa.exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.formula_components(args.name, args.expanded)
                return form, 200

        @nsg.route('/formula')
        class group_formula(Resource):

            @api.doc(
                responses={200: 'Got content',
                           404: 'Does not exist',
                           409: 'Not a formula'},
                description="""Get the formula text for a computed group

**Parameters:**
- name: group name
- expanded: if true, recursively substitute all formula references (default: false)

**Returns:** formula text string

**Example:**
```
GET /group/formula?name=ensemble-forecast
→ "(group-add (group \"scenario-a\") (group \"scenario-b\"))"
```
"""
            )
            @api.expect(groupformula)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = groupformula.parse_args()
                if not tsa.group_exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.group_type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.group_formula(args.name, args.expanded)
                return form, 200

            @api.doc(
                responses={200: 'Updated',
                           201: 'Created',
                           400: 'Syntax error',
                           409: 'Invalid formula'},
                description="""Register or update a group formula

**Parameters:**
- name: group name for the formula
- text: Lisp formula expression

**Returns:** empty body
- 201: formula created
- 200: formula updated

**Errors:**
- 400: syntax error in formula
- 409: type error, value error, or assertion error

**Example:**
```
PUT /group/formula
  name=total-ensemble&text=(group-add (group "ensemble-a") (group "ensemble-b"))
→ 201 Created
```
"""
            )
            @api.expect(register_group_formula)
            @onerror
            @required_roles('admin', 'rw')
            def put(self):
                args = register_group_formula.parse_args()

                exists = tsa.group_formula(args.name)
                try:
                    tsa.register_group_formula(
                        args.name,
                        args.text
                    )
                except TypeError as err:
                    api.abort(409, err.args[0])
                except ValueError as err:
                    api.abort(409, err.args[0])
                except AssertionError as err:
                    api.abort(409, err.args[0])
                except SyntaxError:
                    api.abort(400, f'`{args.name}` has a syntax error in it')
                except Exception:
                    raise

                return '', 200 if exists else 201

        @nsg.route('/eval_formula')
        class group_eval_formula_(Resource):

            @api.doc(
                responses={200: 'Got content',
                           400: 'Invalid formula'},
                description="""Evaluate a group formula expression on the fly

Execute a group formula without persisting it. Useful for developing and debugging group formulas before registration, or for one-off computations.

**Parameters:**
- text: formula expression to evaluate
- revision_date: evaluate formula at this revision date (ISO8601, optional)
- from_value_date: restrict time range start (ISO8601, optional)
- to_value_date: restrict time range end (ISO8601, optional)
- tz: timezone for index conversion (optional)
- format: "json" or "tshpack" (default: json)

**Returns:** group data (DataFrame) in requested format

**Errors:**
- 400: syntax error (prefixed with "syn:") or type error (prefixed with "typ:")

**Example:**
```
POST /group/eval_formula
  text=(group-add (group "ensemble-a") (group "ensemble-b"))&format=json
→ {"2025-01-01T00:00:00+00:00": {"scenario-1": 10.5, "scenario-2": 12.3}, ...}
```
"""
            )
            @api.expect(group_eval_formula)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def post(self):
                args = group_eval_formula.parse_args()
                try:
                    df = tsa.group_eval_formula(
                        args.text,
                        revision_date=args.revision_date,
                        from_value_date=args.from_value_date,
                        to_value_date=args.to_value_date,
                        tz=args.tz
                    )
                except SyntaxError as err:
                    return f'syn:{err}', 400
                except TypeError as err:
                    return f'typ:{err}', 400

                return group_response(
                    args.format,
                    df,
                    200
                )

            @nsg.route('/boundformula')
            class bound_formula(Resource):

                @api.doc(
                    responses={200: 'Got content',
                               404: 'Does not exist',
                               409: 'Not a bound formula'},
                    description="""Get the bindings for a bound formula

A bound formula "hijacks" a series formula to apply it across multiple series from groups, creating a computed group.

**Parameters:**
- name: bound formula group name

**Returns:** object with formula name and bindings
```json
{
  "name": "base-formula",
  "bindings": [
    {"series": "temp-series", "group": "temp-ensemble", "family": "meteo"},
    {"series": "wind-series", "group": "wind-ensemble", "family": "meteo"}
  ]
}
```

**Example:**
```
GET /group/boundformula?name=hijacked-ensemble
→ {"name": "base-formula", "bindings": [...]}
```
"""
                )
                @api.expect(boundformula)
                @onerror
                @required_roles('admin', 'rw', 'ro')
                def get(self):
                    args = boundformula.parse_args()
                    if not tsa.group_exists(args.name):
                        api.abort(404, f'`{args.name}` does not exists')

                    if tsa.group_type(args.name) != 'bound':
                        api.abort(409, f'`{args.name}` exists but is not a bound formula')

                    name, bindings = tsa.bindings_for(args.name)
                    return {
                        'name': name,
                        'bindings': bindings.to_dict(orient='records')
                    }, 200

                @api.doc(
                    responses={200: 'Success'},
                    description="""Register or update bindings for a bound formula

Creates a bound formula that applies a series formula across groups by binding its series inputs to group columns.

**Parameters:**
- name: bound formula group name
- formulaname: name of the series formula to bind
- bindings: JSON array of binding objects with "series", "group", and "family" fields

**Returns:** empty body, status 200

**Example:**
```
PUT /group/boundformula
  name=weather-ensemble
  &formulaname=weather-calc
  &bindings=[{"series": "temp", "group": "temp-scenarios", "family": "meteo"},
             {"series": "wind", "group": "wind-scenarios", "family": "meteo"}]
→ 200 Success
```

**Note:** The series formula is evaluated for each column of the bound groups, producing a computed group.
"""
                )
                @api.expect(boundformula)
                @onerror
                @required_roles('admin', 'rw')
                def put(self):
                    args = boundformula.parse_args()
                    bindings = pd.read_json(io.StringIO(args.bindings))
                    tsa.register_formula_bindings(
                        args.name,
                        args.formulaname,
                        bindings
                    )

                    return '', 200


class formula_httpclient(httpclient):
    index = 1

    @unwraperror
    def formula(self,
                name: str,
                display: Optional[bool]=True,
                expanded: Optional[bool]=False,
                remote: Optional[bool]=True,
                level: Optional[int]=-1):
        res = self.session.get(
            f'{self.uri}/series/formula', params={
                'name': name,
                'display': display,
                'expanded': expanded,
                'remote': remote,
                'level': level
            }
        )
        if res.status_code == 200:
            return res.json()['formula']
        if res.status_code == 418:
            return res
        return  # None is the reasonable api answer

    @unwraperror
    def formula_components(self, name, expanded=False):
        res = self.session.get(
            f'{self.uri}/series/formula_components', params={
                'name': name,
                'expanded': expanded
            }
        )
        if res.status_code == 200:
            return res.json()
        if res.status_code == 418:
            return res
        return  # None is the reasonable api answer

    @unwraperror
    def register_formula(self,
                         name: str,
                         formula: str,
                         reject_unknown: bool=True,
                         user: str='no-user'):
        res = self.session.patch(
            f'{self.uri}/series/formula', data={
                'name': name,
                'text': formula,
                'reject_unknown': reject_unknown,
                'user': user
            }
        )
        if res.status_code == 400:
            raise SyntaxError(res.json()['message'])
        elif res.status_code == 409:
            msg = res.json()['message']
            if msg.startswith('ValueError'):
                raise ValueError(msg[10:])
            elif msg.startswith('AssertionError'):
                raise AssertionError(msg[15:])
            elif msg.startswith('TypeError'):
                raise TypeError(msg[10:])
            else:
                raise Exception(msg)

        if res.status_code in (200, 204):
            return

        return res

    @unwraperror
    def oldformulas(self, name: str):
        res = self.session.get(
            f'{self.uri}/series/old_formulas',
            params={'name': name}
        )
        if res.status_code == 200:
            return [
                (form, pd.Timestamp(dt, tz=tz), user)
                for form, dt, tz, user in res.json()
            ]

        return res

    @unwraperror
    def formula_depth(self, name: str):
        res = self.session.get(
            f'{self.uri}/series/formula_depth',
            params={'name': name}
        )
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def depends(self,
                name: str,
                direct: bool=False,
                reverse: bool=False):
        res = self.session.get(
            f'{self.uri}/series/formula_depends',
            params={'name': name, 'direct': direct, 'reverse': reverse}
        )
        if res.status_code == 200:
            return res.json()

        return None

    @unwraperror
    def eval_formula(self,
                     formula: str,
                     revision_date: Optional[datetime]=None,
                     from_value_date: Optional[datetime]=None,
                     to_value_date: Optional[datetime]=None,
                     tz: Optional[str]=None):
        query = {
            'text': formula,
            'revision_date': strft(revision_date) if revision_date else None,
            'from_value_date': strft(from_value_date) if from_value_date else None,
            'to_value_date': strft(to_value_date) if to_value_date else None,
            'tz': tz,
            'format': 'tshpack'
        }
        res = self.session.post(
            f'{self.uri}/series/eval_formula',
            data=query
        )
        if res.status_code == 200:
            ts = unpack_series('on-the-fly', res.content)
            if tz and tzaware_series(ts):
                ts = ts.tz_convert(tz)
            return ts

        if res.status_code == 400:
            msg = res.json()
            if msg.startswith('syn:'):
                raise SyntaxError(msg[4:])
            elif msg.startswith('typ:'):
                raise TypeError(msg[4:])

        return res

    @unwraperror
    def group_formula(self, name: str, expanded: bool=False):
        res = self.session.get(
            f'{self.uri}/group/formula', params={
                'name': name,
                'expanded': expanded
            }
        )
        if res.status_code == 200:
            return res.json()
        if res.status_code == 418:
            return res
        if res.status_code == 404:
            return
        return res

    @unwraperror
    def register_group_formula(self, name: str, formula: str):
        res = self.session.put(
            f'{self.uri}/group/formula', data={
                'name': name,
                'text': formula
            }
        )
        if res.status_code == 400:
            raise SyntaxError(res.json()['message'])
        elif res.status_code == 409:
            msg = res.json()['message']
            if 'unknown' in msg:
                raise ValueError(msg)
            elif 'exists' in msg:
                raise AssertionError(msg)
            else:
                raise TypeError(msg)

        if res.status_code in (200, 204):
            return

        return res

    @unwraperror
    def register_formula_bindings(self,
                                  name: str,
                                  formulaname: str,
                                  bindings: pd.DataFrame):
        res = self.session.put(
            f'{self.uri}/group/boundformula', data={
                'name': name,
                'formulaname': formulaname,
                'bindings': bindings.to_json(orient='records')
            }
        )
        return res

    @unwraperror
    def bindings_for(self, name: str):
        res = self.session.get(
            f'{self.uri}/group/boundformula', params={
                'name': name
            }
        )
        if res.status_code == 200:
            out = res.json()
            return out['name'], pd.DataFrame(out['bindings'])

        if res.status_code == 404:
            return None

        return res

    @unwraperror
    def group_eval_formula(self,
                           formula: str,
                           revision_date: Optional[datetime]=None,
                           from_value_date: Optional[datetime]=None,
                           to_value_date: Optional[datetime]=None,
                           tz: Optional[str]=None):
        query = {
            'text': formula,
            'revision_date': strft(revision_date) if revision_date else None,
            'from_value_date': strft(from_value_date) if from_value_date else None,
            'to_value_date': strft(to_value_date) if to_value_date else None,
            'tz': tz,
            'format': 'tshpack'
        }
        res = self.session.post(
            f'{self.uri}/group/eval_formula',
            data=query
        )
        if res.status_code == 200:
            df = unpack_group(res.content)
            if tz and tzaware_series(df):
                df = df.tz_convert(tz)
            return df

        if res.status_code == 400:
            msg = res.json()
            if msg.startswith('syn:'):
                raise SyntaxError(msg[4:])
            elif msg.startswith('typ:'):
                raise TypeError(msg[4:])

        return res
