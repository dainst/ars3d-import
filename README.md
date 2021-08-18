
# ARS3D-Import Scripts

These are scripts used to import data from ARS3D project.

Inputs are an objects.csv file provided by the i3Mainz (Florian Thierry) and the portal at

* http://143.93.113.149/_portal/featureSearch.htm

The objects.csv was enriched to include literature references with:

```sh
mkdir mainz-literatur-by-uuid
tail -n+2 objects.csv | cut -f1 -d';' | tr -d '"' | while read ars_uuid; do curl 'https://java-dev.rgzm.de/arspi/sparql' --data-raw "$(sed "s/____ARSUUID____/$ars_uuid/g" mainz-curl-literature.txt)" -o "mainz-literatur-by-uuid/${ars_uuid}.json"; sleep 0.5; done
tail -n+2 objects.csv | cut -f1 -d';' | tr -d '"' | while read ars_uuid; do data=$(jq -r '.results.bindings[] | [.classificationNumber.value, .bookLabel.value, .comment.value] |@csv' "mainz-literatur-by-uuid/${ars_uuid}.json"); echo "${ars_uuid},${data}"; done >> literature.csv
cat <(echo 'ars_id,litClassificationNumber,litBookLabel,litComment') literature.csv | sponge literature.csv
```
