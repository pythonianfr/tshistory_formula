import pandas as pd

from flask_restx import (
    inputs,
    Resource,
    reqparse
)

from tshistory.util import (
    series_metadata,
    unpack_series
)
from tshistory.http.client import httpclient, unwraperror
from tshistory.http.util import (
    enum,
    onerror,
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
    default=False,
    help='return undecorated formula (for display purposes)'
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
    'format', type=enum('json', 'tshpack'), default='json'
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

            @api.expect(formula)
            @onerror
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
                    args.level
                )
                return {'level': args.level, 'formula': form}, 200

            @api.expect(register_formula)
            @onerror
            def patch(self):
                args = register_formula.parse_args()

                exists = tsa.formula(args.name)
                try:
                    tsa.register_formula(
                        args.name,
                        args.text,
                        reject_unknown=args.reject_unknown
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

        @nss.route('/formula_depth')
        class formula_depth_(Resource):

            @api.expect(formula_depth)
            @onerror
            def get(self):
                args = formula_depth.parse_args()
                return tsa.formula_depth(args.name)

        @nss.route('/eval_formula')
        class eval_formula_(Resource):

            @api.expect(eval_formula)
            @onerror
            def post(self):
                args = eval_formula.parse_args()
                try:
                    ts = tsa.eval_formula(
                        args.text,
                        revision_date=args.revision_date,
                        from_value_date=args.from_value_date,
                        to_value_date=args.to_value_date
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

            @api.expect(formula_components)
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

            @api.expect(groupformula)
            @onerror
            def get(self):
                args = groupformula.parse_args()
                if not tsa.group_exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.group_type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.group_formula(args.name, args.expanded)
                return form, 200

            @api.expect(register_group_formula)
            @onerror
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

            @nsg.route('/boundformula')
            class bound_formula(Resource):

                @api.expect(boundformula)
                @onerror
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

                @api.expect(boundformula)
                @onerror
                def put(self):
                    args = boundformula.parse_args()
                    bindings = pd.read_json(args.bindings)
                    tsa.register_formula_bindings(
                        args.name,
                        args.formulaname,
                        bindings
                    )

                    return '', 200


class formula_httpclient(httpclient):
    index = 1

    @unwraperror
    def formula(self, name, display=False, expanded=False, level=-1):
        res = self.session.get(
            f'{self.uri}/series/formula', params={
                'name': name,
                'display': display,
                'expanded': expanded,
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
    def register_formula(self, name,
                         formula,
                         reject_unknown=True):
        res = self.session.patch(
            f'{self.uri}/series/formula', data={
                'name': name,
                'text': formula,
                'reject_unknown': reject_unknown
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
    def formula_depth(self, name):
        res = self.session.get(
            f'{self.uri}/series/formula_depth',
            params={'name': name}
        )
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def eval_formula(self, formula,
                     revision_date=None,
                     from_value_date=None,
                     to_value_date=None):
        query = {
            'text': formula,
            'revision_date': strft(revision_date) if revision_date else None,
            'from_value_date': strft(from_value_date) if from_value_date else None,
            'to_value_date': strft(to_value_date) if to_value_date else None,
            'format': 'tshpack'
        }
        res = self.session.post(
            f'{self.uri}/series/eval_formula',
            data=query
        )
        if res.status_code == 200:
            return unpack_series('on-the-fly', res.content)

        if res.status_code == 400:
            msg = res.json()
            if msg.startswith('syn:'):
                raise SyntaxError(msg[4:])
            elif msg.startswith('typ:'):
                raise TypeError(msg[4:])

        return res

    @unwraperror
    def group_formula(self, name, expanded=False):
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
    def register_group_formula(self, name, formula):
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
    def register_formula_bindings(self, name, formulaname, bindings):
        res = self.session.put(
            f'{self.uri}/group/boundformula', data={
                'name': name,
                'formulaname': formulaname,
                'bindings': bindings.to_json(orient='records')
            }
        )
        return res

    @unwraperror
    def bindings_for(self, name):
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
