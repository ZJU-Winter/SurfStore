package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")
var ERR_MAJORITY_DOWN = fmt.Errorf("Majority of the nodes is down")
var ERR_LEADER_NOTFOUND = fmt.Errorf("Leader not found")
