package main

import (
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
	"github.com/stormgo/terminal/sttable"
	"github.com/stormgo/terminal/stwin"
	"github.com/stormgo/terminal/stwinmgr"
)

func main() {
	log.Debugf("Starting terminal...")

	stlib.InitTerminal()
	defer stlib.EndTerminal()

	stwinmgr.Start()

	stlib.ClearScreen()

	table1 := sttable.NewTable("table1")
	table1.AddHeader([]sttable.STCell{
		{Content: "COLUMN1", Width: 10},
		{Content: "COLUMN2", Width: 15},
	})
	table1.AddRow([]string{"col1row1", "col2row1"})
	table1.AddRow([]string{"col1row2", "col2row2"})
	table1.AddRow([]string{"col1row3", "col2row3"})
	table1.AddRow([]string{"col1row4", "col2row4"})
	table1.AddRow([]string{"col1row5", "col2row5"})
	table1.AddRow([]string{"longer than width", "again longer than width"})

	table2 := sttable.NewTable("table2")
	table2.AddHeader([]sttable.STCell{
		{Content: "COLUMN1", Width: 10},
		{Content: "COLUMN2", Width: 15},
	})
	table2.AddRow([]string{"col1row1", "col2row1"})
	table2.AddRow([]string{"col1row2", "col2row2"})
	table2.AddRow([]string{"col1row3", "col2row3"})
	table2.AddRow([]string{"col1row4", "col2row4"})
	table2.AddRow([]string{"col1row5", "col2row5"})
	table1.AddRow([]string{"longer than width", "again longer than width"})

	// 1 for the heading and 1 each for the boundary lines on both sides.
	win1 := stwin.NewWin(table1, table1.GetRowCount()+3, 1, 0)
	stwinmgr.AddWindow(win1)

	win2 := stwin.NewWin(table2, table2.GetRowCount()+3, 5, 5)
	stwinmgr.AddWindow(win2)

	win3 := stwin.NewWin(table2, table2.GetRowCount()+3, 10, 10)
	stwinmgr.AddWindow(win3)

	//
	// Run the window manager to handle events and periodically refresh the
	// windows.
	//
	stwinmgr.Run()

	log.Assert(false)
}
