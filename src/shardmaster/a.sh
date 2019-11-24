for (( i = 1; i <= 200; i++))
	do
		echo $i >> b.txt
		go test | grep -E "Test|Pass|Fail|ok |FAIL|test_test.go" >> b.txt
	done