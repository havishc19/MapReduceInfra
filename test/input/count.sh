grep -iInr "^$1 " * | wc -l
grep -iInr " $1 " * | wc -l
grep -iInr " $1\." * | wc -l

