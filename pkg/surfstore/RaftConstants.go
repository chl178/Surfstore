package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")
var STR_SERVER_CRASHED = "Server is crashed."
var STR_NOT_LEADER = "Server is not the leader"
