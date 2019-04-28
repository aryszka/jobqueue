default: build

build:
	go build

.coverprofile:
	go test -coverprofile .coverprofile

cover: .coverprofile
	go tool cover -func .coverprofile

showcover: .coverprofile
	go tool cover -html .coverprofile

check:
	go test

race:
	go test -race

fmt:
	@gofmt -w -s .

doc.go: README.md
	@echo '/*' > doc.go
	@sed 's/^#* //' < README.md >> doc.go
	@echo '*/' >> doc.go
	@echo package jobqueue >> doc.go
