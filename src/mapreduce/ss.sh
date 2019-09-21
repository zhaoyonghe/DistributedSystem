for((integer = 1; integer <= 100; integer++))
do
    go test | grep Passed | wc -l >> b.txt
done
