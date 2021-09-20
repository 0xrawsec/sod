COVERAGE=.github/coverage


coverage:
	mkdir -p $(COVERAGE)
	go test -coverprofile=/tmp/cover.out -covermode=atomic ./... > /dev/null && go tool cover -func=/tmp/cover.out > $(COVERAGE)/cover.txt
	cat $(COVERAGE)/cover.txt | ./$(COVERAGE)/badgen.sh > $(COVERAGE)/badge.svg