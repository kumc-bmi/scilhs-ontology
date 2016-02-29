#!/bin/bash

# Variables expected to be in the environment (for Jenkins, etc.)
#export i2b2_meta_schema=
#export ontology_tables=
#export drop_ontology_tables=
#export create_oracle_metadata_tables=
#export pcornet_cdm_user=
#export pcornet_cdm=
#export sid=

# Some of the ontology files are zipped, some are not.  Capitalization isn't consistent.
for i in *.zip; do unzip -o $i; done
for i in *.txt; do mv -f $i `echo $i | tr [:lower:] [:upper:]`; done

# Generate .ctl files for sqlldr - save off the schema/tables found
python ddl_to_ctl.py ${create_oracle_metadata_tables} ${i2b2_meta_schema} > ${ontology_tables}

# Drop existing ontology tables /create the ontology tables
while read f; do
  echo "drop table $f;" >> ${drop_ontology_tables}
done < ${ontology_tables}

sqlplus /nolog <<EOF
connect ${pcornet_cdm_user}/${pcornet_cdm};

set echo on;

define i2b2_meta_schema=${i2b2_meta_schema}

WHENEVER SQLERROR CONTINUE;
start ${drop_ontology_tables}

WHENEVER SQLERROR EXIT SQL.SQLCODE;
start ${create_oracle_metadata_tables}


quit;
EOF

# Load the ontology tables
for ctl in *.ctl; do
    base=${ctl%.*}
    ORACLE_SID=${sid} sqlldr ${pcornet_cdm_user}/${pcornet_cdm} control="$ctl" data="$base".TXT bad="$base".bad log="$base".log errors=10000 || true # ignoring errors!!
done

touch nothing.log
touch nothing.bad

# TODO: Properly handle errors - note we have a high limit above
wc -l *.bad
