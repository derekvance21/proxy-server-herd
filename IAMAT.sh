time=$(date +%s)
echo -ne "IAMAT $1 $2 $time" | nc localhost $3