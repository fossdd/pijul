#!/bin/bash

REPO="$(pijul remote | grep -F nest.pijul.com | sed -e 's/  //g')"
if echo "$REPO" | grep -Fq '@'; then
    # ssh url
    REPO="https://$(echo "$REPO" | cut -f2 -d@ | sed -e 's#:#/#g')"
fi
if [ -z "$REPO" ]; then
    echo "WARN: unable to get repo url" 1>&2
fi

TMPFB="$(mktemp)"
trap 'rm -f $TMPFB' EXIT

(
echo 'digraph PCG {'
#echo 'nodesep = 5;'
echo 'overlap = scale;'
echo 'node [shape=none,target="_blank"];'
# mark important nodes
for i; do
    echo "\"$i\" [fontcolor=\"#0000FF\"];"
    echo "$i" >> "$TMPFB"
done
pijul log | grep ^Change | while read DUMMY ID; do
    pijul change "$ID" | awk -v oid="$ID" -v collf="$TMPFB" '
$1 == "#" {
    section = $2;
}
$1 != "#" && $2 != "" && section == "Dependencies" && $1 != "[*]" && substr($1,length($1),1) != "+" {
    print "\"" oid "\" -> \"" $2 "\"" extra ";";
    print $2 >> collf;
}
'
    echo "$ID" >> "$TMPFB"
done
sort -u < "$TMPFB" | while read ID; do
    echo "\"$ID\" [URL=\"$REPO/changes/$ID\"]";
done
echo '}'
) | sfdp -Tsvg /dev/stdin -o /dev/stdout
