package main

import "fmt"

const (
	VERSMAJ = 0
	VERSMID = 9
	VERSMIN = 0
)

var (
	REVISION     = "HEAD"
	REVISIONDATE = "now"
)

func Build() string {
	var ver = fmt.Sprintf("%d.%d.%d", VERSMAJ, VERSMID, VERSMIN)
	return fmt.Sprintf("%s (%s %s)", ver, REVISIONDATE, REVISION)
}
