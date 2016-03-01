''' rm_field_newlines - remove newlines in .csv fields (i.e. \n in tooltips)

Specifically looks at files that have Windows style newlines (\r\n) but have
Unix style line endings within fields (\n).

sqlldr won't load the data if these newlines are in there (at least, not
with the options I used).
'''
import csv
import sys
from tempfile import TemporaryFile

if __name__ == '__main__':
    infile = sys.argv[1]
    with open(infile, 'rb') as fin:
        dr = csv.DictReader(fin, delimiter='|', lineterminator='\r\n')
        tmp = TemporaryFile(mode='w+b')
        dw = csv.DictWriter(tmp, dr.fieldnames, delimiter='|',
                            lineterminator='\r\n')
        # Write header, even with < Python 2.7
        dw.writerow(dict(zip(dr.fieldnames, dr.fieldnames)))
        for row in dr:
            dw.writerow(dict([(f, d.replace('\n', ''))
                              for f, d in row.items()]))
    with open(infile, 'wb') as fout:
        tmp.seek(0)
        while True:
            data = tmp.read(10*1024*1024)
            if data:
                fout.write(data)
            else:
                break
    tmp.close()
                