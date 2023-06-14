from functools import partial

import responses

from tshistory.testutil import (
    read_request_bridge,
    with_http_bridge as basebridge,
    write_request_bridge
)


class with_http_bridge(basebridge):

    def __init__(self, uri, resp, wsgitester):
        super().__init__(uri, resp, wsgitester)
        resp.add_callback(
            responses.GET, uri + '/series/formula',
            callback=partial(read_request_bridge, wsgitester)
        )

        resp.add_callback(
            responses.PATCH, uri + '/series/formula',
            callback=write_request_bridge(wsgitester.patch)
        )

        resp.add_callback(
            responses.GET, uri + '/series/formula_components',
            callback=partial(read_request_bridge, wsgitester)
        )

        resp.add_callback(
            responses.GET, uri + '/series/formula_depth',
            callback=partial(read_request_bridge, wsgitester)
        )

        resp.add_callback(
            responses.POST, uri + '/series/eval_formula',
            callback=write_request_bridge(wsgitester.post)
        )

        resp.add_callback(
            responses.GET, uri + '/group/formula',
            callback=partial(read_request_bridge, wsgitester)
        )

        resp.add_callback(
            responses.PUT, uri + '/group/formula',
            callback=write_request_bridge(wsgitester.put)
        )

        resp.add_callback(
            responses.GET, uri + '/group/boundformula',
            callback=partial(read_request_bridge, wsgitester)
        )

        resp.add_callback(
            responses.PUT, uri + '/group/boundformula',
            callback=write_request_bridge(wsgitester.put)
        )

