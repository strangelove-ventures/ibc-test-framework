package tui

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type keyBinding struct {
	Key  string // Single key or combination of keys.
	Help string // Very short help text describing the key's action.
}

type keyBindings []keyBinding

func (b keyBindings) Prepend(bindings ...keyBindings) keyBindings {
	var altered keyBindings
	for i := range bindings {
		altered = append(altered, bindings[i]...)
	}
	return append(altered, b...)
}

var (
	baseHelpKeys = keyBindings{
		{"esc", "go back"},
		{"ctl+c", "exit"},
	}
	tableHelpKeys = keyBindings{
		{fmt.Sprintf("%c/k", tcell.RuneUArrow), "move up"},
		{fmt.Sprintf("%c/j", tcell.RuneDArrow), "move down"},
	}
	textViewHelpKeys = keyBindings{
		{fmt.Sprintf("%c/k", tcell.RuneUArrow), "scroll up"},
		{fmt.Sprintf("%c/j", tcell.RuneDArrow), "scroll down"},
		{"g", "go to top"},
		{"shift+g", "go to bottom"},
		{"ctrl+b", "page up"},
		{"ctrl+f", "page down"},
	}

	keyMap = map[mainContent]keyBindings{
		testCasesMain:      baseHelpKeys.Prepend(tableHelpKeys, keyBindings{{"m", "cosmos messages"}, {"enter", "view txs"}}),
		cosmosMessagesMain: baseHelpKeys.Prepend(tableHelpKeys),
		txDetailMain:       baseHelpKeys.Prepend(textViewHelpKeys, keyBindings{{"[", "previous tx"}, {"]", "next tx"}}),
	}
)

type helpView struct {
	*tview.Table
}

func newHelpView() *helpView {
	tbl := tview.NewTable().SetBorders(false)
	tbl.SetBorder(false)
	return &helpView{tbl}
}

// Replace serves as a hook to clear all keys and update the help table view with new keys.
func (view *helpView) Replace(keys []keyBinding) *helpView {
	view.Table.Clear()
	keyCell := func(s string) *tview.TableCell {
		return tview.NewTableCell("<" + s + ">").
			SetTextColor(tcell.ColorBlue)
	}
	textCell := func(s string) *tview.TableCell {
		return tview.NewTableCell(s).
			SetStyle(textStyle.Attributes(tcell.AttrDim))
	}
	var colOffset int
	for row, binding := range keys {
		// Only allow 6 help items per row or else help items will not be visible.
		if row > 0 && row%6 == 0 {
			colOffset += 2
		}
		view.Table.SetCell(row, colOffset, keyCell(binding.Key))
		view.Table.SetCell(row, colOffset+1, textCell(binding.Help))
	}
	return view
}
