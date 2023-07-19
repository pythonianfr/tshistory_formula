from tshistory.search import (
    _OPMAP,
    query,
    usym
)


class byformulacontents(query):
    __slots__ = ('query',)

    def __init__(self, query: str):
        self.query = query

    def __expr__(self):
        return f'(by.formulacontents "{self.query}")'

    @classmethod
    def _fromtree(cls, tree):
        return cls(tree[1])

    def sql(self):
        vid = usym('name')
        return (
            f'internal_metadata ->> \'formula\' like %({vid})s',
            {vid: f'%%{self.query}%%'}
        )


_OPMAP['by.formulacontents'] = 'byformulacontents'
