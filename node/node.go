package node

import (
	"errors"
)

type Node int64

const (
	//前端使用
	BIZ Node = iota
	//应用层之间使用
	FUND
	//运营管理系统
	OPT
)

func (n Node) String() string {
	if n < BIZ || n > OPT {
		return "Unknown"
	}
	return [...]string{"biz", "fund", "opt"}[n]
}
func (n Node) IsValid() error {
	switch n {
	case BIZ, FUND, OPT:
		return nil
	}
	return errors.New("invalid leave type")
}

func GetNode(node string) Node {
	switch node {
	case BIZ.String():
		return BIZ
	case FUND.String():
		return FUND
	case OPT.String():
		return OPT
	}
	return -1
}
func Values() []Node {
	return []Node{BIZ, FUND, OPT}
}
func HasNode(node string) bool {
	if GetNode(node).IsValid() == nil {
		return true
	}
	return false
}
