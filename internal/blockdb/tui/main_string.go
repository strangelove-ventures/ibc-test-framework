// Code generated by "stringer -type=main"; DO NOT EDIT.

package tui

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[testCasesMain-0]
	_ = x[cosmosSummaryMain-1]
}

const _main_name = "testCasesMaincosmosSummaryMain"

var _main_index = [...]uint8{0, 13, 30}

func (i main) String() string {
	if i < 0 || i >= main(len(_main_index)-1) {
		return "main(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _main_name[_main_index[i]:_main_index[i+1]]
}
