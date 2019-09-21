for((integer = 1; integer <= 30; integer++))
do
    go test | grep Passed | wc -l >> b.txt
done
