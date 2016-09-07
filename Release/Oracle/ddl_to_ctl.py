''' ddl_to_ctl - create sqlldr .ctl's from .sql with "create table" statements

>>> sql = """CREATE TABLE meta.diag ( "Level" INTEGER,
...                                   "Name" VARCHAR);"""
>>> st, table, ctl = sql_to_ctl(sql).next()
>>> (st, table)
('meta.diag', 'diag')
>>> print ctl
options (errors=0, skip=1)
load data
truncate into table meta.diag
fields terminated by '|' optionally enclosed by '"'
trailing nullcols(
  Level,
  Name
  )

'''
from re import findall, search, sub, DOTALL

ctl_template = '''options (errors=0, skip=1)
load data
truncate into table %(schema_table)s
fields terminated by '|' optionally enclosed by '"'
trailing nullcols(
  %(columns)s
  )'''

date_format = "DATE 'YYYY/MM/DD HH:MI:SS AM'"
data_type_xlate = dict(
    # skip blob column so that we can use direct path load
    C_METADATAXML='FILLER',
    C_COMMENT='FILLER',
    C_TOOLTIP='CHAR(900)',
    C_NAME='CHAR(2000)',
    C_DIMCODE='CHAR(700)',
    UPDATE_DATE=date_format,
    DOWNLOAD_DATE=date_format,
    IMPORT_DATE=date_format)


def main(argv, stdout, cwd):
    override_schema = ''
    if len(argv) > 2:
        override_schema = argv[2]

    sql_fn = argv[1]
    with (cwd / sql_fn).open('rb') as inf:
        sql = inf.read()

    for st, table, ctl in sql_to_ctl(sql, override_schema):
        with (cwd / (table + '.ctl')).open('wb') as fout:
            fout.write(ctl)
        print >>stdout, st


def sql_to_ctl(sql, override_schema=''):
    for st, cols in get_stcols(sql, override_schema).items():
        schema, table = st.split('.')
        ctl = ctl_template % dict(schema_table=st,
                                  columns=',\n  '.join(cols))
        yield st, table, ctl


def get_stcols(sql, override_schema=''):
    r'''Get sqlldr column specifications from a CREATE TABLE statement.

    We expect column names to be quoted and on separate lines:
    >>> get_stcols("""CREATE TABLE S.T ( "X" INTEGER,
    ...                                  "Y" NUMERIC);""")
    {'S.T': ['X', 'Y']}

    Override the schema:
    >>> get_stcols('CREATE TABLE S.T ( "X" INTEGER);', override_schema='S2')
    {'S2.T': ['X']}

    Predefined datatypes:
    >>> get_stcols('CREATE TABLE S.T ( "C_DIMCODE" VARCHAR(100));')
    {'S.T': ['C_DIMCODE CHAR(700)']}
    '''
    def xlate_col(col):
        if col in data_type_xlate:
            return ' '.join([col, data_type_xlate[col]])
        return col

    st_cols = dict()
    for _, schema, table, col_blk in findall(
            'CREATE TABLE ((?P<schema>.*?)\.)?'
            '(?P<table>.*?)\((?P<columns>.*?);',
            sql, flags=DOTALL):

        key = '.'.join([v for v in
                        [override_schema or schema.strip(),
                         table.strip()] if len(v.strip())])
        st_cols[key] = list()
        for col_line in [c for c in sub('[\r)]', '', col_blk).split('\n')
                         if len(c.strip()) > 0]:
            st_cols[key].append(xlate_col(search('\s+"(?P<column>.*?)".*',
                                                 col_line).group(1)))
    return st_cols


class Path(object):
    def __init__(self, path, ops):
        io_open, joinpath = ops
        self.open = lambda mode='rb': io_open(path, mode=mode)
        self.joinpath = lambda other: Path(joinpath(path, other), ops)

    def __div__(self, other):
        return self.joinpath(other)


if __name__ == '__main__':
    def _script():
        from sys import argv, stdout
        from io import open as io_open
        from os.path import join as joinpath

        main(argv[:], stdout, cwd=Path('.', (io_open, joinpath)))

    _script()
