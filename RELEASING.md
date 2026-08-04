# Releasing the eWAVE dataset

```shell
cldfbench makecldf cldfbench_ewave.py --glottolog-version v5.3 --with-cldfreadme --with-zenodo
```

```shell
pytest
```

```shell
cldfbench cldfviz.map --language-properties Type_ID  cldf --width 20 --markersize 12 --padding-bottom 8 --padding-top 8 --padding-left 5 --padding-right 5 --with-ocean   --format svg --out map.svg --language-labels --fontsize xx-small
```

Labels may have to be re-arranged e.g. using inkscape.

```shell
cldfbench readme cldfbench_ewave.py
```
