BINARY_NAME=mapReducer

build :

	go vet
	go fmt cmd/*.go
	go fmt helper/*.go
	go build -race -o ${BINARY_NAME}
	go build -race -buildmode=plugin $(plugin)

clean:
	go clean
	rm -f ${BINARY_NAME}
	rm -f mr-*
	rm -f mr-out*
