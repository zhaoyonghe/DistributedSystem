for((integer = 1; integer <= 100; integer++))
do
    go test | grep ok >> a.txt
done
