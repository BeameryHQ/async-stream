#!/bin/bash

set -e

# Generates a coverprofile for each package and concatenates all those
# coverprofiles into one. It outputs a html file based off the
# coverprofile.

EXCLUDE_FILES_REGEXP=${EXCLUDE_FILES_REGEXP:-"vendor"}

covfile=coverage.out
tmpcovfile="${covfile}.tmp"
emptytestfile=emptytestusedforcoverage_test.go
mode=count
test_prefix_cmd="go test -v -covermode=$mode -coverprofile=${covfile}.tmp"
concat_and_rm_cmd="tail -n +2 $tmpcovfile >> $covfile && rm $tmpcovfile"

# Build the coverage profile
echo "mode: $mode" > "$covfile"
go list ./... | egrep -v ${EXCLUDE_FILES_REGEXP} | \
    xargs go list -f "\
        '{{if not (or (len .TestGoFiles) (len .XTestGoFiles))}}\
             echo \"package {{.Name}}\" > {{.Dir}}/${emptytestfile} &&\
             $test_prefix_cmd {{.ImportPath}} && $concat_and_rm_cmd &&\
             rm {{.Dir}}/${emptytestfile}\
        {{else}}\
             $test_prefix_cmd {{.ImportPath}} && $concat_and_rm_cmd\
        {{end}}'" | \
    xargs -L 1 bash -c

# Output the percent
go tool cover -func=coverage.out | \
    tail -n 1 | \
    awk '{ print $3 }' | \
    sed -e 's/^\(.*\)$/TotalCoverage: \1/g'

