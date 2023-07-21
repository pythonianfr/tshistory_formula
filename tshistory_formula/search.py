from tshistory.search import (
    _OPMAP,
    query,
    usym
)


# rendez-vous object
IMPORTCALLBACK = None


class byformulacontents(query):
    __slots__ = ('query',)

    def __init__(self, query: str):
        self.query = query

    def __expr__(self):
        return f'(by.formulacontents "{self.query}")'

    @classmethod
    def _fromtree(cls, tree):
        return cls(tree[1])

    def sql(self, namespace='tsh'):
        vid = usym('name')
        return (
            f'internal_metadata ->> \'formula\' like %({vid})s',
            {vid: f'%%{self.query}%%'}
        )


class isformula(query):

    def __expr__(self):
        return '(by.formula)'

    @classmethod
    def _fromtree(cls, _):
        return cls()

    def sql(self, namespace='tsh'):
        return 'internal_metadata -> \'formula\' is not null', {}



_OPMAP['by.formulacontents'] = 'byformulacontents'
_OPMAP['by.formula'] = 'isformula'
