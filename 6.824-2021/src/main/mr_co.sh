rm -r mr-*
go build -race -buildmode=plugin ../mrapps/indexer.go
go run -race ./mrcoordinator.go pg*.txt