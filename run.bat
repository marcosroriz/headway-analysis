@echo off

set line=263
set stops=data/263-pontos.csv
set

for %%i in (avl/diasuteis/*) do (
    echo python analyze.py --avl avl/diasuteis/%%i --line %line% --stops %stops% --start 8 --end 10 --headway 420 --output diasuteis6h8h
    python analyze.py --avl avl/diasuteis/%%i --line %line% --stops %stops% --start 6 --end 8 --headway 420 --output diasuteis.6h8h.csv
)
