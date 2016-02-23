''' ddl_to_ctl - create sqlldr .ctl's from .sql with "create table" statements
'''
from contextlib import contextmanager
from re import findall, search, sub, DOTALL

ctl_template = '''options (direct=true, errors=0, skip=1)
load data
truncate into table %(schema_table)s
fields terminated by '|' optionally enclosed by '"'
trailing nullcols(
  %(columns)s
  )'''

date_format = "DATE 'YYYY/MM/DD HH:MI:SS AM'"
data_type_xlate = dict(
    C_METADATAXML='CHAR(100000)',
    C_TOOLTIP='CHAR(900)',
    C_NAME='CHAR(2000)',
    C_DIMCODE='CHAR(700)',
    UPDATE_DATE=date_format,
    DOWNLOAD_DATE=date_format,
    IMPORT_DATE=date_format)


def main(open_argv, open_subpath, override_schema):
    with open_argv(1, 'rb') as inf:
        sql = inf.read()

    for st, cols in get_stcols(sql, override_schema).items():
        with open_subpath(st.split('.')[1] + '.ctl', 'wb') as fout:
            fout.write(ctl_template % dict(schema_table=st,
                                           columns=',\n  '.join(cols)))


def get_stcols(sql, override_schema=''):
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


if __name__ == '__main__':
    def _tcb():
        from sys import argv
        from os.path import dirname, abspath, realpath

        @contextmanager
        def open_argv(idx, mode):
            with open(argv[idx], mode) as f:
                yield f

        @contextmanager
        def open_subpath(path, mode):
            script_path = dirname(realpath(__file__))
            ap = abspath(path)
            if script_path not in abspath(path):
                raise ValueError('Can\'t open outside script directory: %s' %
                                 path)
            with open(path, mode) as f:
                yield f

        override_schema = ''
        if len(argv) > 2:
            override_schema = argv[2]

        return dict(open_argv=open_argv, open_subpath=open_subpath,
                    override_schema=override_schema)
    main(**_tcb())
