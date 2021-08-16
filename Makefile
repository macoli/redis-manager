.PHONY: all build run gotool clean help

BINARY="redis-manager"

all: gotool build

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${BINARY}

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ${BINARY}

build-mac:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ${BINARY}
run:
	@go run ./

gotool:
	go fmt ./
	go vet ./

clean:
	@if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi

help:
	@echo "make                - 格式化 Go 代码, 并编译生成二进制文件"
	@echo "make build-linux    - 编译 Go 代码, 生成 linux 下的二进制文件"
	@echo "make build-windows  - 编译 Go 代码, 生成 windows 下的二进制文件"
	@echo "make build-mac      - 编译 Go 代码, 生成 mac 下的二进制文件"
	@echo "make run            - 直接运行 Go 代码"
	@echo "make clean          - 移除二进制文件和 vim swap files"
	@echo "make gotool         - 运行 Go 工具 'fmt' and 'vet'"