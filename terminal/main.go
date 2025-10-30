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

	// Initialize ncurses.
	stlib.InitTerminal()
	defer stlib.EndTerminal()

	// Initialize the window manager.
	stwinmgr.Start()

	table1 := sttable.NewTable("table1", false /* dynamic */)
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

	table2 := sttable.NewTable("table2", false /* dynamic */)
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

	table3 := sttable.NewTable("table3", false /* dynamic */)
	table3.AddHeader([]sttable.STCell{
		{Content: "COLUMN1", Width: 15},
		{Content: "COLUMN2", Width: 20},
		{Content: "COLUMN3", Width: 20},
		{Content: "LONG COLUMN NAME4", Width: 25},
	})
	table3.AddRow([]string{"c1r1", "c2r1", "c3r1", "c4r1"})
	table3.AddRow([]string{"c1r2", "c2r2", "c3r2", "c4r2"})
	table3.AddRow([]string{"c1r3", "c2r3", "c3r3", "c4r3"})
	table3.AddRow([]string{"c1r4", "c2r4", "c3r4", "c4r4"})
	table3.AddRow([]string{"c1r5", "c2r5", "c3r5", "c4r5"})
	table3.AddRow([]string{"c1r6", "c2r6", "c3r6", "c4r6"})
	table3.AddRow([]string{"c1r7", "c2r7", "c3r7", "c4r7"})
	table3.AddRow([]string{"c1r8", "c2r8", "c3r8", "c4r8"})
	table3.AddRow([]string{"c1r9", "c2r9", "c3r9", "c4r9"})
	table3.AddRow([]string{"c1r10", "c2r10", "c3r10", "c4r10"})

	// 1 for the heading and 1 each for the boundary lines on both sides.
	win1 := stwin.NewWin(table1, 1, 0)
	stwinmgr.AddWindow(win1)

	win2 := stwin.NewWin(table2, 5, 5)
	stwinmgr.AddWindow(win2)

	win3 := stwin.NewWin(table2, 10, 10)
	stwinmgr.AddWindow(win3)

	win4 := stwin.NewWin(table1, 1, 30)
	stwinmgr.AddWindow(win4)

	win5 := stwin.NewWin(table3, 10, 10)
	//
	// Reduce width and height of the window so as not to fit the table
	// completely. This is to demonstrate that the table will be truncated
	// according to the window size and if we resize the window, the table
	// will then fit.
	//
	win5.W = 30
	win5.H = 4
	stwinmgr.AddWindow(win5)

	//
	// Add a dynamically populated table and a window to hold it.
	//
	table4 := sttable.NewTable("table4", true /* dynamic */)
	err := table4.AddCol("stock")
	if err != nil {
		log.Fatalf("Failed to add column 'stock' to table4: %v", err)
	}

	err = table4.AddCol("ltp")
	if err != nil {
		log.Panicf("Failed to add column 'ltp' to table4: %v", err)
	}

	table4.GenRows()
	win6 := stwin.NewWin(table4, 2, 2)

	// Add the window to the window manager.
	stwinmgr.AddWindow(win6)

	//
	// Now run the window manager to display all the windows with their contents,
	// also handle key and mouse events and periodically refresh the windows.
	//
	stwinmgr.Run()

	log.Assert(false)
}
