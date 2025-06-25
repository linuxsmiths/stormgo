package stwin

import (
	gc "github.com/linuxsmiths/goncurses"
	"github.com/stormgo/common"
	"github.com/stormgo/common/log"
	"github.com/stormgo/terminal/stlib"
	"github.com/stormgo/terminal/sttable"
)

// This is the ST window.
// It displays an STTable.
type STWin struct {
	//
	// Table to be displayed in this window.
	//
	Table *sttable.STTable

	//
	// goncurses window, and it's height, width and starting position (top left corner).
	// The Window content cannot be assumed to be populated and Populate() must
	// be called to populate the Window with content.
	//
	Window *gc.Window
	H      int
	W      int
	X      int
	Y      int
}

func NewWin(table *sttable.STTable, h, y, x int) *STWin {
	// Table must have been properly initialized.
	log.Assert(table.Width > 0)
	log.Assert(table.Name != "")

	//
	// Catch unlikely windows that extend outside the terminal.
	// These are not useful and it's not something user wants.
	//
	log.Assert(y+h <= stlib.GetMaxRows(),
		"Window extends outside terminal (more rows)",
		table.Name, y, h, stlib.GetMaxRows())
	log.Assert(x+table.Width+2 <= stlib.GetMaxCols(),
		"Window extends outside terminal (more cols)",
		table.Name, x, table.Width+2, stlib.GetMaxCols())

	return &STWin{
		Table: table,
		H:     h,
		W:     table.Width + 2, // 1 for left border, 1 for right border.
		Y:     y,
		X:     x,
	}
}

// Does the given (y, x) coordinate fall within this window?
func (sw *STWin) FallsInWindow(y, x int) bool {
	log.Assert(y >= 0 && x >= 0, y, x)
	// Impossibly high values.
	log.Assert(y <= stlib.GetMaxRows() && x <= stlib.GetMaxCols(),
		y, x, stlib.GetMaxRows(), stlib.GetMaxCols())

	return y >= sw.Y && y < sw.Y+sw.H && x >= sw.X && x < sw.X+sw.W
}

// Does the given (y, x) coordinate fall within any column header?
// Returns the column index if it falls in a column header, or -1 if it does
// not.
func (sw *STWin) FallsInColumnHeader(y, x int) int {
	log.Assert(y >= 0 && x >= 0, y, x)
	log.Assert(y <= stlib.GetMaxRows() && x <= stlib.GetMaxCols(), y, x)

	//
	// Click doesn't fall in the header row.
	// sw.Y is the border followed immediately by the header row.
	//
	if y != sw.Y+1 {
		return -1
	}

	// First column header starts at sw.X + 1 (1 for the left border).
	startCol := sw.X + 1
	endCol := 0

	hdr := sw.Table.Header.Cells
	for i, cell := range hdr {
		endCol = startCol + cell.Width

		if x >= startCol && x < endCol {
			return i
		}

		// Next column starts after current column, leaving one space.
		startCol = endCol + 1
	}

	return -1
}

// Populate the window with the table contents.
func (sw *STWin) Populate(inFocus bool) {
	//stlib.PrintStatus("Populating window %s (Y=%d, X=%d, H=%d, W=%d)",
	//	sw.Table.Name, sw.Y, sw.X, sw.H, sw.W)

	//
	// Everytime we create a new gocurses window and populate it afresh.
	//
	win, err := gc.NewWindow(sw.H, sw.W, sw.Y, sw.X)
	if err != nil {
		log.Fatalf("gc.NewWindow(H=%d, W=%d, Y=%d, X=%d), name: %s, failed: %v",
			sw.H, sw.W, sw.Y, sw.X, sw.Table.Name, err)
	}

	//
	// Draw a box around the window.
	// If the window is in focus, we draw it with a different color.
	//
	if inFocus {
		win.ColorOn(stlib.GreenOnBlack)
		err = win.Box(0, 0)
		win.ColorOff(stlib.GreenOnBlack)
	} else {
		err = win.Box(0, 0)
	}
	log.Assert(err == nil, err)

	//
	// Print the title bar.
	//
	win.MovePrint(0, 1, sw.Table.Name)

	//
	// Print the header.
	//
	y := 1
	x := 1
	win.ColorOn(stlib.BlackOnCyan)
	for _, cell := range sw.Table.Header.Cells {
		paddedStr := common.TruncateAndPadUTF8String(cell.Content, cell.Width)
		win.MovePrint(y, x, paddedStr)

		if cell.Sort == sttable.SortOrderAsc {
			win.MoveAddWChar(y, x+cell.Width, '△')
		} else if cell.Sort == sttable.SortOrderDesc {
			win.MoveAddWChar(y, x+cell.Width, '▽')
		}

		x += (cell.Width + 1)
	}
	win.ColorOff(stlib.BlackOnCyan)

	//
	// Print all rows.
	//
	y = 2
	for _, row := range sw.Table.Rows {
		x = 1
		for _, cell := range row.Cells {
			// Data rows must not have sort order set.
			log.Assert(cell.Sort == sttable.SortOrderNone, cell.Sort)

			paddedStr := common.TruncateAndPadUTF8String(cell.Content, cell.Width)
			win.MovePrint(y, x, paddedStr)
			x += (cell.Width + 1)
		}
		y++
	}

	if sw.Window != nil {
		// Free up the memory allocated for the previous window by cgo.
		err := sw.Window.Delete()
		log.Assert(err == nil, err)
	}

	sw.Window = win
}

// Called when 'key' is pressed while this window is in focus.
func (sw *STWin) HandleKey(key gc.Key) {
	// For now, we just log the key pressed.
	stlib.PrintStatus("Key %v pressed in window %s", gc.KeyString(key), sw.Table.Name)
}

// Called when user presses the quit character (usually 'q') when this window
// is in focus.
// Quit can either close this window (if it's a temporary window created for
// performing some operation) or quit the entire application.
func (sw *STWin) HandleQuit() {
	// For now, we always quit the application.
	stlib.PrintStatus("Quit pressed in window %s", sw.Table.Name)
	stlib.EndTerminal()
}

// Called when mouse left key is pressed while this window is in focus.
func (sw *STWin) HandleMouse(y, x int) {
	colIdx := sw.FallsInColumnHeader(y, x)

	stlib.PrintStatus("Mouse clicked at (%d, %d) in window %s (col: %d)",
		y, x, sw.Table.Name, colIdx)

	// If the click does not fall in a column header, do nothing.
	if colIdx < 0 {
		return
	}

	// else, sort the table by the clicked column.
	cell := &sw.Table.Header.Cells[colIdx]

	// Toggle the sort order for the clicked column.
	if cell.Sort == sttable.SortOrderNone || cell.Sort == sttable.SortOrderDesc {
		cell.Sort = sttable.SortOrderAsc
	} else {
		cell.Sort = sttable.SortOrderDesc
	}

	// Only one column can be sorted at a time.
	for i := range sw.Table.Header.Cells {
		if i != colIdx {
			sw.Table.Header.Cells[i].Sort = sttable.SortOrderNone
		}
	}

	sw.Table.Sort(colIdx, cell.Sort)
}
