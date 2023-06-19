## SurfStore
This is the final course project repo for CSE224(Graduate Network System) at UCSD, the repo implements a cloud file storage system called SurfStore.

### Run the servers
#### Run a block service at localhost:8080
`go run cmd/SurfstoreServerExec/main.go -s block -p 8080 -l`
#### Run a raft meta service
`go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i 0`
### Sync files
`go run cmd/SurfstoreClientExec/main.go -f configfile.txt baseDir blockSize`
