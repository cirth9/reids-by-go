package cluster

import (
	"reids-by-go/utils/trans"
)

func MultipleToSingleCmd(cmd [][]byte, discovery *Discovery) []string {
	cmdStrings := trans.BytesToStrings(cmd)
	var SingleCmds []string
	if cmdStrings[0] == "mset" {
		if (len(cmdStrings)-1)%2 != 0 {
			return nil
		}
		cmdHeader := "set"
		for i := 1; i < len(cmdStrings); i += 2 {
			SingleCmds = append(SingleCmds, cmdHeader)
			SingleCmds = append(SingleCmds, cmdStrings[i])
			SingleCmds = append(SingleCmds, cmdStrings[i+1])
		}
		return SingleCmds
	} else if cmdStrings[0] == "mget" {
		cmdHeader := "get"
		for i := 1; i < len(cmdStrings); i++ {
			SingleCmds = append(SingleCmds, cmdHeader)
			SingleCmds = append(SingleCmds, cmdStrings[i])
		}
		return SingleCmds
	}
	return nil
}
